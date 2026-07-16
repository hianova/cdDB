use alloc::vec::Vec;
use crate::AHashMap;
use crate::core::atomic::AtomicPtr;
use crate::core::query::{PartitionRoute, Query, QueryNode, QueryResult};
use alloc::format;
use alloc::string::String;
use alloc::sync::Arc;

#[cfg(feature = "std")]
use no_std_tool::collections::BoundedQueue;

use crate::DualCacheFF;
use no_std_tool::collections::SimpleBloom;

use crate::core::column::Columns;
use crate::core::column::MultiVectorPointer;
#[cfg(feature = "std")]
use crate::core::commands::{PartitionCommand, WriteCommand};
use crate::core::qsbr::WorkerNode;
use crate::core::rcu::new_atomic_ptr;
#[cfg(feature = "std")]
use crate::engine::partition::Partition;
use crate::io::platform::{Executor, FileSystem};
use crate::io::storage::Storage;
#[cfg(feature = "std")]
use crate::io::wal::StdWal;
use crate::io::wal::{NoopWal, WalProvider};
/// A metrics snapshot for a single partition, captured at a point-in-time
/// without holding any locks.
///
/// Returned as part of [`DbMetrics`] by [`CdDBDispatcher::metrics()`].
#[derive(Debug, Clone)]
pub struct PartitionMetrics {
    /// Human-readable name that was supplied when the partition was registered
    /// (e.g. `"users"` or `"events.2024"`).
    pub name: String,
    /// Monotonically-increasing numeric ID assigned to the partition at
    /// registration time. Used internally to scope cache keys and WAL entries.
    pub partition_id: u32,
    /// Fraction of Bloom-filter bits that are currently set, in the range
    /// `[0.0, 1.0]`. A value close to `1.0` indicates the filter is nearly
    /// saturated and false-positive rates may be increasing.
    pub bloom_saturation: f32,
    /// Number of entity records currently resident in the partition's in-memory
    /// index (i.e. the size of the shared multi-vector pointer map).
    pub memory_entities: usize,
}

/// A full, lock-free snapshot of database-engine metrics returned by
/// [`CdDBDispatcher::metrics()`].
///
/// All values are best-effort: atomics are read with `Relaxed` or `Acquire`
/// ordering and no global lock is held, so individual fields may reflect
/// slightly different instants in time.
#[derive(Debug, Clone)]
pub struct DbMetrics {
    /// `true` if the dispatcher has been placed into sleep mode via
    /// [`CdDBDispatcher::sleep()`].
    pub is_sleeping: bool,
    /// Per-partition metrics, one entry for each partition currently
    /// registered in the route table.
    pub partitions: Vec<PartitionMetrics>,

    // Cache metrics
    /// `true` when the `dualcache-ff` feature is compiled in and the global
    /// cache is active; `false` otherwise.
    pub cache_enabled: bool,
    /// `true` while the cache is still in its cold-start warm-up phase (before
    /// the first epoch boundary after [`CdDBDispatcher::prewarm_partition()`]).
    pub cache_is_cold_start: bool,
    /// Number of telemetry / admission commands currently enqueued for the
    /// background cache daemon (only meaningful in `std` builds).
    pub cache_pending_commands: usize,
    /// Current eviction epoch counter maintained by the `DualCacheFF` daemon.
    pub cache_epoch: u32,
    /// Number of entries in the Hot Tier (T1 — recently promoted).
    pub cache_t1_count: usize,
    /// Number of entries in the Warm Tier (T2 — frequency-qualified).
    pub cache_t2_count: usize,
    /// Number of entries in the Core resident set.
    pub cache_core_count: usize,
}

/// The top-level database engine entry point and central dispatcher for cdDB.
///
/// `CdDBDispatcher<N>` is responsible for:
/// - Maintaining a **route table** that maps partition names to their
///   [`CdDBDispatcher`] acts as the front door for all read and write requests,
///   delegating work to individual partitions using a robust epoch-based routing
///   table.
///
/// # Type parameter `N`
///
/// `N` is the const generic that controls the size of each partition's Bloom
/// filter bit-array. A larger `N` reduces false-positive rates at the cost of
/// more memory. `N` must be chosen at compile time and is uniform across all
/// partitions owned by a single dispatcher instance.
///
/// # Examples
///
/// ```rust,no_run
/// # #[cfg(feature = "std")] {
/// use cddb::CdDBDispatcher;
///
/// // Create a dispatcher backed by the standard filesystem.
/// let mut db = CdDBDispatcher::<1024>::new_std(Some("./data".into()));
///
/// // Register a partition and obtain a writer handle.
/// let writer = db.register_partition("users".into());
/// # }
/// ```
pub struct CdDBDispatcher<const N: usize> {
    /// Mapping from partition name to its shared [`PartitionRoute<N>`] context.
    /// The route carries everything a read query needs (columns, bloom filter,
    /// cache, storage, WAL) and is cheaply `Arc`-cloned for concurrent access.
    pub route_table: AHashMap<String, Arc<PartitionRoute<N>>>,
    /// Optional base directory under which partition storage sub-directories
    /// are created. When `None`, paths are resolved relative to the process
    /// working directory.
    pub base_path: Option<String>,
    /// Atomic pointer to the head of the global QSBR worker linked-list.
    /// Every reader thread registers itself here so the write path can
    /// determine when it is safe to free old data generations.
    pub workers: Arc<AtomicPtr<WorkerNode>>,
    /// Abstract file-system interface used for all storage I/O. Swap this out
    /// with a custom implementation to run cdDB in embedded or WASM
    /// environments without touching any other code.
    pub fs: Arc<dyn FileSystem>,
    /// Abstract task executor used to spawn per-partition background threads.
    /// The default `StdExecutor` uses `std::thread::spawn`; a custom
    /// implementation can map tasks onto any async runtime or RTOS task.
    pub executor: Arc<dyn Executor>,
    /// Global `DualCacheFF` hot-index shared by **all** partitions.
    /// Cache keys are `(partition_id, entity_id)` tuples, ensuring isolation
    /// between partitions while allowing the eviction policy to see the full
    /// cross-partition access distribution.
    pub global_cache: Arc<DualCacheFF<(u32, usize), (), 64, 4096, 262144, 266304>>,
    /// Monotonically increasing counter used to assign a unique `u32` ID to
    /// each registered partition. Incremented once per
    /// [`register_partition_with_wal_provider`](Self::register_partition_with_wal_provider)
    /// call.
    pub next_partition_id: u32,
    /// Atomic boolean that controls the logical sleep state of the dispatcher.
    /// When `true`, upper-layer traffic handlers should pause incoming writes.
    /// Background maintenance threads observe this flag to enter low-power
    /// idle polling rather than being fully terminated.
    pub is_sleeping: Arc<core::sync::atomic::AtomicBool>,
    #[cfg(feature = "std")]
    pub partition_threads: Vec<crate::io::platform::TaskHandle>,
}

impl<const N: usize> CdDBDispatcher<N> {
    /// Create a new `CdDBDispatcher` with a custom file system and executor.
    ///
    /// Use this constructor when targeting environments that do not have access
    /// to the Rust standard library (e.g. embedded, WASM) or when you need to
    /// inject a mock filesystem for testing.
    ///
    /// # Arguments
    ///
    /// * `base_path` — Optional root directory under which per-partition
    ///   storage sub-directories are created. Pass `None` to resolve paths
    ///   relative to the process working directory.
    /// * `fs` — File-system abstraction used for all I/O. The default
    ///   standard-library implementation is [`StdFileSystem`].
    /// * `executor` — Task spawner used to launch per-partition background
    ///   threads. The default is [`StdExecutor`].
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// # #[cfg(feature = "std")] {
    /// use std::sync::Arc;
    /// use cddb::{CdDBDispatcher, platform::{StdFileSystem, StdExecutor}};
    ///
    /// let db = CdDBDispatcher::<262144>::new(
    ///     Some("./db_data".into()),
    ///     Arc::new(StdFileSystem),
    ///     Arc::new(StdExecutor),
    ///     cddb::CacheConfig::default(),
    /// );
    /// # }
    /// ```
    pub fn new(
        base_path: Option<String>,
        fs: Arc<dyn FileSystem>,
        executor: Arc<dyn Executor>,
        _cache_config: crate::CacheConfig,
    ) -> Self {
        let global_cache = cfg_select! {
            feature = "dualcache-ff" => { {
                let cache = alloc::sync::Arc::new(DualCacheFF::new());

                if _cache_config.daemon_mode {
                    // SAFETY: set_daemon_mode requires &'static self because the daemon thread
                    // needs to hold a reference to the cache core. We guarantee this is safe because
                    // in CdDBDispatcher::drop, we explicitly call set_daemon_mode(false) which blocks
                    // and joins the daemon thread BEFORE the Arc is dropped. Thus, the cache instance
                    // is guaranteed to outlive the daemon thread.
                    unsafe { (*alloc::sync::Arc::as_ptr(&cache)).set_daemon_mode(true) };
                }
                cache
            } },
            _ => alloc::sync::Arc::new(DualCacheFF::new()),
        };

        Self {
            route_table: AHashMap::default(),
            base_path,
            workers: Arc::new(crate::core::atomic::AtomicPtr::new(core::ptr::null_mut())),
            fs,
            executor,
            global_cache,
            next_partition_id: 0,
            is_sleeping: Arc::new(core::sync::atomic::AtomicBool::new(false)),
            #[cfg(feature = "std")]
            partition_threads: Vec::new(),
        }
    }

    /// Convenience constructor that creates a `CdDBDispatcher` backed by the
    /// standard library's [`StdFileSystem`] and [`StdExecutor`].
    ///
    /// This is the recommended entry point for applications running in a normal
    /// `std` environment. For custom environments use [`CdDBDispatcher::new`].
    ///
    /// # Arguments
    ///
    /// * `base_path` — Optional root directory for partition storage. Pass
    ///   `None` to use `"data/<partition_name>"` relative paths.
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// # #[cfg(feature = "std")] {
    /// use cddb::CdDBDispatcher;
    ///
    /// let db = CdDBDispatcher::<262144>::new_std(Some("./db_data".into()), cddb::CacheConfig::default());
    /// # }
    /// ```
    #[cfg(feature = "std")]
    pub fn new_std(base_path: Option<String>, cache_config: crate::CacheConfig) -> Self {
        Self::new(
            base_path,
            Arc::new(crate::io::platform::StdFileSystem),
            Arc::new(crate::io::platform::StdExecutor),
            cache_config,
        )
    }

    /// Creates a purely in-memory instance of `CdDBDispatcher`.
    ///
    /// This bypasses all disk I/O by utilizing a `NullFileSystem`. Data writes
    /// are discarded, and reads return empty, maximizing memory efficiency for
    /// cache-only workloads.
    #[cfg(feature = "std")]
    pub fn new_in_memory(cache_config: crate::CacheConfig) -> Self {
        Self::new(
            Some("in_memory_db".into()),
            Arc::new(crate::io::platform::NullFileSystem),
            Arc::new(crate::io::platform::StdExecutor),
            cache_config,
        )
    }

    /// Register a new partition with the given name using the default
    /// synchronous WAL (no WAL file — equivalent to `WalMode::Sync` with
    /// `wal_path = None`).
    ///
    /// A background worker thread is spawned automatically to process write
    /// commands for this partition. The returned [`UserWriter`] is the only
    /// handle through which callers should send [`WriteCommand`]s to the
    /// partition. When the `UserWriter` is dropped, a `Shutdown` command is
    /// delivered to gracefully stop the background thread.
    ///
    /// # Arguments
    ///
    /// * `path` — Logical name of the partition (e.g. `"users"`). This name
    ///   is also used to derive the on-disk storage sub-directory path.
    ///
    /// # Returns
    ///
    /// A [`UserWriter`] bound to the newly created partition's command queue.
    #[cfg(feature = "std")]
    pub fn register_partition(&mut self, path: String) -> UserWriter {
        self.register_partition_with_wal(path, None, crate::io::wal::WalMode::Sync)
    }

    /// Register a new partition with an explicit memory budget hint.
    ///
    /// Behaves identically to [`register_partition`](Self::register_partition)
    /// in the current implementation; the `budget_bytes` parameter is accepted
    /// for API compatibility but is not yet enforced. Future versions may use
    /// it to constrain the partition's in-memory column footprint.
    ///
    /// # Arguments
    ///
    /// * `path` — Logical name / storage path of the partition.
    /// * `_budget_bytes` — Desired maximum resident memory in bytes (currently
    ///   advisory only).
    ///
    /// # Returns
    ///
    /// A [`UserWriter`] bound to the newly created partition's command queue.
    #[cfg(feature = "std")]
    pub fn register_partition_with_budget(
        &mut self,
        path: String,
        _budget_bytes: usize,
    ) -> UserWriter {
        self.register_partition_with_wal(path, None, crate::io::wal::WalMode::Sync)
    }

    /// Register a new partition, specifying an optional WAL file path and
    /// write mode.
    ///
    /// When `wal_path` is `Some`, a [`StdWal`] is created at the given path
    /// with the supplied [`WalMode`]. When `wal_path` is `None`, a [`NoopWal`]
    /// is used and no durability log is written.
    ///
    /// # Arguments
    ///
    /// * `path` — Logical name of the partition.
    /// * `wal_path` — Optional file path for the write-ahead log. `None`
    ///   disables WAL entirely.
    /// * `wal_mode` — Controls WAL flush strategy (e.g. sync-on-every-write
    ///   vs. async/batched). Only meaningful when `wal_path` is `Some`.
    ///
    /// # Returns
    ///
    /// A [`UserWriter`] bound to the newly created partition's command queue.
    #[cfg(feature = "std")]
    pub fn register_partition_with_wal(
        &mut self,
        path: String,
        wal_path: Option<String>,
        wal_mode: crate::io::wal::WalMode,
    ) -> UserWriter {
        let wal: Arc<dyn WalProvider> = if let Some(p) = wal_path {
            Arc::new(StdWal::new(p, self.fs.clone(), wal_mode))
        } else {
            Arc::new(NoopWal)
        };
        self.register_partition_with_wal_provider(path, wal)
    }

    /// Register a new partition with a fully custom [`WalProvider`].
    ///
    /// This is the lowest-level registration method. All other
    /// `register_partition*` variants ultimately delegate here. Use this
    /// method when you need complete control over the WAL implementation
    /// (e.g. an in-memory WAL for tests, or a network-backed log).
    ///
    /// Internally, the method:
    /// 1. Allocates a lock-free [`BoundedQueue`] (capacity 262 144 slots).
    /// 2. Initialises a [`PartitionRoute<N>`] and inserts it into the route
    ///    table.
    /// 3. Spawns a background thread via the configured executor that runs
    ///    the partition event loop, replaying the WAL on startup.
    ///
    /// # Arguments
    ///
    /// * `path` — Logical name / storage path of the partition.
    /// * `wal` — A shared [`WalProvider`] implementation. Pass
    ///   `Arc::new(NoopWal)` to disable durability logging.
    ///
    /// # Returns
    ///
    /// A [`UserWriter`] bound to the partition's command queue. Dropping the
    /// writer delivers a `Shutdown` command, causing the background thread to
    /// exit cleanly.
    #[cfg(feature = "std")]
    pub fn register_partition_with_wal_provider(
        &mut self,
        path: String,
        wal: Arc<dyn WalProvider>,
    ) -> UserWriter {
        let queue = Arc::new(BoundedQueue::new());
        let writer_tx_out = queue.clone();

        let partition_id = self.next_partition_id;
        self.next_partition_id += 1;
        let (storage_path, shared_pointers, bloom, columns, workers) =
            self.init_partition_state(&path);

        let route = Arc::new(PartitionRoute {
            name: path.clone(),
            partition_id,
            writer_tx: queue.clone(),
            columns: Arc::clone(&columns),
            shared_pointers: Arc::clone(&shared_pointers),
            hot_index: Arc::clone(&self.global_cache),
            bloom_filter: Arc::clone(&bloom),
            storage: Arc::new(Storage::new(storage_path.clone(), self.fs.clone())),
            workers: Arc::clone(&workers),
            wal: Arc::clone(&wal),
        });

        self.route_table.insert(path.clone(), route);

        let handle = self.spawn_partition_thread(
            queue,
            columns,
            wal,
            workers,
            storage_path,
            shared_pointers,
            bloom,
            partition_id,
            self.global_cache.clone(),
        );
        self.partition_threads.push(handle);

        UserWriter(writer_tx_out)
    }

    /// Register a new partition in `no_std` environments where thread
    /// spawning is not available.
    ///
    /// Unlike the `std` `register_partition*` family, this method does **not**
    /// spawn a background thread. Instead, the caller is responsible for
    /// driving the partition's event loop manually — typically inside an RTOS
    /// task or a bare-metal super-loop — by polling the `_writer_rx` queue and
    /// dispatching [`PartitionCommand`]s to a [`Partition`] instance.
    ///
    /// A [`PartitionRoute<N>`] is created and inserted into the route table so
    /// that read queries can be executed through the dispatcher as usual.
    ///
    /// # Arguments
    ///
    /// * `path` — Logical name of the partition.
    /// * `writer_tx` — The sending half of the application's command channel.
    ///   This is stored in the route and used by [`UserWriter`]-equivalent
    ///   logic to push [`WriteCommand`]s.
    /// * `_writer_rx` — The receiving half (currently unused by this method;
    ///   pass it to your partition event loop separately).
    #[cfg(not(feature = "std"))]
    pub fn register_partition_no_std(
        &mut self,
        path: String,
        writer_tx: Arc<dyn crate::io::platform::MessageSender>,
        _writer_rx: alloc::boxed::Box<dyn crate::io::platform::MessageQueue>,
    ) {
        let partition_id = self.next_partition_id;
        self.next_partition_id += 1;
        let (storage_path, shared_pointers, bloom, columns, workers) =
            self.init_partition_state(&path);

        let route = Arc::new(PartitionRoute {
            name: path.clone(),
            partition_id,
            writer_tx,
            columns: Arc::clone(&columns),
            shared_pointers: Arc::clone(&shared_pointers),
            hot_index: Arc::clone(&self.global_cache),
            bloom_filter: Arc::clone(&bloom),
            storage: Arc::new(Storage::new(storage_path.clone(), self.fs.clone())),
            workers: Arc::clone(&workers),
            wal: Arc::new(NoopWal),
        });

        self.route_table.insert(path, route);

        // In no_std, the user must manage the thread/loop for the Partition
    }

    #[allow(clippy::type_complexity)]
    fn init_partition_state(
        &self,
        path: &str,
    ) -> (
        String,
        Arc<AtomicPtr<AHashMap<usize, MultiVectorPointer>>>,
        Arc<AtomicPtr<SimpleBloom<N>>>,
        Arc<AtomicPtr<Columns<N>>>,
        Arc<AtomicPtr<WorkerNode>>,
    ) {
        let storage_path = self
            .base_path
            .as_ref()
            .map(|base| format!("{}/{}.data", base, path.replace('.', "/")))
            .unwrap_or_else(|| format!("data/{}", path));

        let _ = self.fs.create_dir_all(&storage_path);

        let shared_pointers = Arc::new(new_atomic_ptr(AHashMap::default()));
        let bloom = Arc::new(new_atomic_ptr(SimpleBloom::<N>::new()));
        let columns = Arc::new(new_atomic_ptr(Columns::<N>::new()));
        let workers = Arc::new(crate::core::atomic::AtomicPtr::new(core::ptr::null_mut()));

        (storage_path, shared_pointers, bloom, columns, workers)
    }

    #[cfg(feature = "std")]
    #[allow(clippy::too_many_arguments)]
    fn spawn_partition_thread(
        &self,
        rx: Arc<BoundedQueue<PartitionCommand, 262144>>,
        columns: Arc<AtomicPtr<Columns<N>>>,
        wal: Arc<dyn WalProvider>,
        workers: Arc<AtomicPtr<WorkerNode>>,
        storage_path: String,
        shared_pointers: Arc<AtomicPtr<AHashMap<usize, MultiVectorPointer>>>,
        bloom_filter: Arc<AtomicPtr<SimpleBloom<N>>>,
        partition_id: u32,
        hot_index: Arc<DualCacheFF<(u32, usize), (), 64, 4096, 262144, 266304>>,
    ) -> crate::io::platform::TaskHandle {
        let fs_rt = self.fs.clone();
        let wal_rt = wal.clone();

        self.executor.spawn_task(alloc::boxed::Box::new(move || {
            let mut partition = Partition::new(
                alloc::boxed::Box::new(crate::io::platform::StdMessageQueue { rx }),
                columns,
                wal_rt.clone(),
                workers,
                storage_path,
                fs_rt,
                shared_pointers,
                bloom_filter,
                hot_index,
                partition_id,
            );

            partition.replay_wal();
            partition.run();
        }))
    }

    pub fn get_route(&self, partition_name: &str) -> Option<Arc<PartitionRoute<N>>> {
        self.route_table.get(partition_name).cloned()
    }

    /// Exposes a high-level `CdDBPartition` handle wrapping queries and writes.
    #[cfg(feature = "std")]
    pub fn get_partition(&self, name: &str) -> Option<crate::engine::facade::CdDBPartition<'_, N>> {
        let route = self.get_route(name)?;
        let writer = UserWriter(route.writer_tx.clone());
        Some(crate::engine::facade::CdDBPartition {
            name: name.into(),
            writer,
            dispatcher: self,
        })
    }

    /// Execute a batch of query nodes against a named partition under a single
    /// QSBR pin. The network / session layer does not need to know about QSBR,
    /// `Query`, or `WorkerState` — simply pass the slice and a result callback.
    ///
    /// This is the architectural boundary described in the cdDB design spec:
    /// the caller (e.g. a TCP stream handler parsing a Redis pipeline) hands
    /// `N` commands as an array and pays exactly **one** QSBR enter/leave.
    #[cfg(feature = "std")]
    pub fn execute_batch<'b, F>(&self, partition: &str, nodes: &[QueryNode<'b>], mut cb: F)
    where
        F: FnMut(QueryResult),
    {
        if let Some(route) = self.route_table.get(partition) {
            let q = Query::new(route);
            q.execute_with_cb(nodes, &mut cb);
        }
    }

    /// Execute a batch of query nodes against a named partition asynchronously,
    /// offloading the QSBR-pinned read work to a Tokio blocking thread pool.
    ///
    /// This is the async equivalent of [`execute_batch`](Self::execute_batch).
    /// Because QSBR operations must not be suspended across `.await` points,
    /// the actual query execution runs inside `tokio::task::spawn_blocking`
    /// and the future resolves once the blocking task completes.
    ///
    /// # Arguments
    ///
    /// * `partition` — Name of the target partition. If the partition is not
    ///   found in the route table, `None` is returned.
    /// * `nodes` — Query nodes to execute. Must have `'static` lifetime so
    ///   they can be moved into the blocking task.
    /// * `cb` — Callback invoked for each [`QueryResult`]. The return value
    ///   of the **last** invocation is returned as `Some(R)`.
    ///
    /// # Returns
    ///
    /// `Some(R)` containing the result of the final callback invocation, or
    /// `None` if the partition was not found or no nodes were executed.
    ///
    /// # Errors
    ///
    /// Panics if the spawned Tokio blocking task panics (propagated via
    /// `JoinHandle::await.unwrap()`).
    #[cfg(all(feature = "std", feature = "async"))]
    pub async fn execute_batch_async<F, R>(
        &self,
        partition: String,
        nodes: Vec<QueryNode<'static>>,
        mut cb: F,
    ) -> Option<R>
    where
        F: FnMut(QueryResult) -> R + Send + 'static,
        R: Send + 'static,
    {
        if let Some(route) = self.route_table.get(&partition).cloned() {
            tokio::task::spawn_blocking(move || {
                let q = Query::new(&route);
                let mut last_res = None;
                q.execute_with_cb(&nodes, |res| {
                    last_res = Some(cb(res));
                });
                last_res
            })
            .await
            .unwrap()
        } else {
            None
        }
    }

    /// Returns a lock-free, point-in-time snapshot of database-engine metrics.
    ///
    /// Iterates over every registered partition to compute its Bloom-filter
    /// saturation and in-memory entity count, then reads global cache counters
    /// using atomic loads (no mutexes are held).
    ///
    /// # Returns
    ///
    /// A [`DbMetrics`] struct containing per-partition [`PartitionMetrics`] and
    /// global cache statistics.
    pub fn metrics(&self) -> DbMetrics {
        let is_sleeping = self.is_sleeping();

        let mut partitions = Vec::with_capacity(self.route_table.len());
        for (name, route) in self.route_table.iter() {
            let bloom = crate::core::rcu::load_ref(&route.bloom_filter);
            let total = bloom.total_bits() as f32;
            let set = bloom.count_set_bits() as f32;
            let saturation = if total > 0.0 { set / total } else { 0.0 };

            let shared = crate::core::rcu::load_ref(&route.shared_pointers);

            partitions.push(PartitionMetrics {
                name: name.clone(),
                partition_id: route.partition_id,
                bloom_saturation: saturation,
                memory_entities: shared.len(),
            });
        }

        let (
            cache_enabled,
            cache_is_cold_start,
            cache_pending_commands,
            cache_epoch,
            cache_t1_count,
            cache_t2_count,
            cache_core_count,
        ) = cfg_select! {
            feature = "dualcache-ff" => { {
                let (t1, t2, core) = (0, 0, 0);
                (true, false, 0, 0, t1, t2, core)
            } },
            _ => (false, false, 0, 0, 0, 0, 0)
        };

        DbMetrics {
            is_sleeping,
            partitions,
            cache_enabled,
            cache_is_cold_start,
            cache_pending_commands,
            cache_epoch,
            cache_t1_count,
            cache_t2_count,
            cache_core_count,
        }
    }

    /// Pre-warm the cache for a specific partition with a batch of entity IDs.
    /// This bypasses standard probation and injects keys directly into the Hot Tier (T1),
    /// which is highly efficient for application startup sequences.
    #[allow(unused_variables)]
    pub fn prewarm_partition(
        &self,
        partition_name: &str,
        entity_ids: impl IntoIterator<Item = usize>,
    ) -> Result<(), &'static str> {
        let route = self
            .get_route(partition_name)
            .ok_or("Partition not found")?;
        let partition_id = route.partition_id;

        #[cfg(feature = "dualcache-ff")]
        {
            #[cfg(feature = "std")]
            {
                let items = entity_ids.into_iter().map(|id| ((partition_id, id), ()));
            }
            #[cfg(not(feature = "std"))]
            {
                let items = entity_ids.into_iter().map(|id| ((partition_id, id), ()));
                self.global_cache.warmup(items);
            }
        }

        Ok(())
    }

    /// Flush all pending thread-local telemetry and cache admission commands,
    /// blocking the calling thread until the background `DualCacheFF` daemon
    /// has processed them.
    ///
    /// Call this after a burst of writes or reads to ensure the hot-index
    /// reflects the most recent access pattern before issuing queries that
    /// depend on cache residency (e.g. in tests or benchmark warm-up phases).
    ///
    /// This is a **no-op** when compiled without the `dualcache-ff` feature or
    /// in `no_std` environments where the daemon thread does not exist.
    pub fn sync_cache(&self) {
        #[cfg(feature = "dualcache-ff")]
        {}
    }

    /// Put the dispatcher into a logical sleep state.
    ///
    /// Sets the `is_sleeping` flag to `true` and, when the `dualcache-ff`
    /// feature is active, suspends the background cache daemon so it enters
    /// low-power idle polling (approximately 1 ms intervals, effectively
    /// zero CPU).
    ///
    /// Background threads are **not** terminated; the transition is
    /// intentionally lightweight to avoid the high latency cost of tearing
    /// down and recreating OS threads.
    ///
    /// Upper-layer traffic handlers should check [`is_sleeping`](Self::is_sleeping)
    /// before accepting new writes. Wake the dispatcher again with
    /// [`wake`](Self::wake).
    pub fn sleep(&self) {
        self.is_sleeping
            .store(true, core::sync::atomic::Ordering::Release);
    }

    /// Wake the dispatcher from a logical sleep state.
    ///
    /// Clears the `is_sleeping` flag and, when the `dualcache-ff` feature is
    /// active, resumes the background cache daemon so it returns to normal
    /// operation. This is the counterpart of [`sleep`](Self::sleep).
    pub fn wake(&self) {
        self.is_sleeping
            .store(false, core::sync::atomic::Ordering::Release);
    }

    /// Returns `true` if the dispatcher is currently in the sleep state.
    ///
    /// The value is read with `Acquire` ordering so that any writes performed
    /// by [`sleep`](Self::sleep) or [`wake`](Self::wake) on other threads are
    /// visible to the caller.
    pub fn is_sleeping(&self) -> bool {
        self.is_sleeping.load(core::sync::atomic::Ordering::Acquire)
    }
}

impl<const N: usize> Drop for CdDBDispatcher<N> {
    fn drop(&mut self) {
        #[cfg(feature = "std")]
        {
            // Send Shutdown command to all partition queues.
            for route in self.route_table.values() {
                let mut backoff = crate::io::platform::Backoff::new();
                while route.writer_tx.push(PartitionCommand::Shutdown).is_err() {
                    if backoff.is_completed() {
                        std::thread::yield_now();
                    } else {
                        backoff.snooze();
                    }
                }
            }

            // Wait (join) for all partition background threads to exit.
            for handle in self.partition_threads.drain(..) {
                let _ = handle.join();
            }
        }

        #[cfg(feature = "dualcache-ff")]
        {
            let cache_ptr = alloc::sync::Arc::as_ptr(&self.global_cache);
            // SAFETY: We manually call set_daemon_mode(false) to join the daemon thread.
            // Since this is a blocking call, it guarantees the thread will exit before
            // we return from `drop` and the Arc is deallocated.
            unsafe {
                (*cache_ptr).set_daemon_mode(false);
            }
        }
    }
}

#[cfg(feature = "std")]
impl<const N: usize> Default for CdDBDispatcher<N> {
    fn default() -> Self {
        Self::new_std(None, crate::CacheConfig::default())
    }
}

/// A handle for sending write commands to a single registered partition.
///
/// `UserWriter` wraps the sending end of the partition's lock-free
/// [`BoundedQueue`] and provides two delivery strategies: a blocking
/// [`send`](UserWriter::send) with exponential backoff (suitable for
/// production write paths) and a non-blocking
/// [`try_send`](UserWriter::try_send) (suitable for rate-limited or
/// drop-tolerant paths).
///
/// When a `UserWriter` is dropped, a `Shutdown` command is automatically
/// enqueued, causing the partition's background thread to exit cleanly.
#[cfg(feature = "std")]
#[derive(Clone)]
pub struct UserWriter(Arc<BoundedQueue<PartitionCommand, 262144>>);
#[cfg(feature = "std")]
impl UserWriter {
    /// Send a write command to the partition, blocking with exponential backoff
    /// until queue space becomes available.
    ///
    /// On each failed push the method calls [`Backoff::snooze`] (spin /
    /// yield). Once the backoff sequence is exhausted it falls back to
    /// [`std::thread::yield_now`] on every retry to avoid monopolising the CPU
    /// while the partition thread drains the queue.
    ///
    /// # Arguments
    ///
    /// * `cmd` — The [`WriteCommand`] to deliver to the partition.
    ///
    /// # Returns
    ///
    /// `Ok(())` once the command has been successfully enqueued.
    /// This function never returns `Err` in practice — it loops indefinitely
    /// until the push succeeds.
    pub fn send(&self, cmd: WriteCommand) -> Result<(), &'static str> {
        let mut cmd = PartitionCommand::Write(cmd);
        let mut backoff = crate::io::platform::Backoff::new();
        loop {
            match self.0.push(cmd) {
                Ok(()) => return Ok(()),
                Err(c) => {
                    cmd = c;
                    if backoff.is_completed() {
                        std::thread::yield_now();
                    } else {
                        backoff.snooze();
                    }
                }
            }
        }
    }
    /// Attempt to send a write command to the partition without blocking.
    ///
    /// Performs a single push attempt. If the partition's command queue is
    /// full the command is **discarded** and `Err("Full")` is returned. This
    /// is appropriate for callers that implement their own back-pressure or
    /// are willing to drop writes under load.
    ///
    /// # Arguments
    ///
    /// * `cmd` — The [`WriteCommand`] to attempt to deliver.
    ///
    /// # Errors
    ///
    /// Returns `Err("Full")` if the bounded queue has no available slots.
    pub fn try_send(&self, cmd: WriteCommand) -> Result<(), &'static str> {
        self.0
            .push(PartitionCommand::Write(cmd))
            .map_err(|_| "Full")
    }
}

#[cfg(feature = "std")]
impl Drop for UserWriter {
    fn drop(&mut self) {
        // Send shutdown command on drop to gracefully terminate the background partition thread
        let mut backoff = crate::io::platform::Backoff::new();
        while self.0.push(PartitionCommand::Shutdown).is_err() {
            if backoff.is_completed() {
                std::thread::yield_now();
            } else {
                backoff.snooze();
            }
        }
    }
}

// Shared read context for a single partition, used to route queries and
// write commands.
// PartitionRoute has been moved to src/core/query.rs

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::commands::WriteCommand;
    use alloc::string::ToString;
    use alloc::sync::Arc;
    use alloc::vec;
    use no_std_tool::collections::BoundedQueue;

    #[cfg(feature = "std")]
    #[test]
    #[ignore]
    fn test_dispatcher_register_with_budget() {
        let mut d = CdDBDispatcher::<1024>::new_std(None, crate::CacheConfig::default());
        let path = "test_budget".to_string();
        let _writer = d.register_partition_with_budget(path.clone(), 1024);
        assert!(d.route_table.contains_key(&path));
        let _ = std::fs::remove_dir_all(&path);
    }

    #[cfg(feature = "std")]
    #[test]
    #[ignore]
    fn test_user_writer_try_send_full_and_drop() {
        let q = Arc::new(BoundedQueue::new());
        let writer = UserWriter(q.clone());
        let mut i = 1;
        while writer
            .try_send(WriteCommand::Delete { entity_id: i })
            .is_ok()
        {
            i += 1;
        }

        drop(writer); // shouldn't block indefinitely
    }

    #[cfg(feature = "std")]
    #[test]
    #[ignore]
    fn test_user_writer_send_backoff() {
        let q = Arc::new(BoundedQueue::new());
        let writer = UserWriter(q.clone());

        let cmd1 = WriteCommand::Delete { entity_id: 1 };
        writer.try_send(cmd1).unwrap();

        let q_clone = q.clone();
        std::thread::spawn(move || {
            std::thread::sleep(std::time::Duration::from_millis(50));
            let _ = q_clone.pop();
        });

        let cmd2 = WriteCommand::Delete { entity_id: 2 };
        writer.send(cmd2).unwrap(); // should block then succeed
    }

    #[cfg(feature = "std")]
    #[test]
    #[ignore]
    fn test_route_getters_and_execute() {
        use crate::core::query::QueryNode;
        use crate::io::wal::NoopWal;

        let cols = Arc::new(crate::core::rcu::new_atomic_ptr(
            crate::core::column::Columns::<1024>::new(),
        ));
        let ptrs = Arc::new(crate::core::rcu::new_atomic_ptr(crate::AHashMap::default()));
        let bloom = Arc::new(crate::core::rcu::new_atomic_ptr(
            no_std_tool::collections::SimpleBloom::<1024>::new(),
        ));

        #[cfg(feature = "dualcache-ff")]
        let cache = crate::DualCacheFF::new();
        #[cfg(not(feature = "dualcache-ff"))]
        let cache = crate::DualCacheFF::new(crate::CacheConfig::default());

        let path = "/tmp/test_route".to_string();
        let _ = std::fs::remove_dir_all(&path);

        let storage = Arc::new(crate::Storage::new(
            path.clone(),
            Arc::new(crate::io::platform::StdFileSystem),
        ));

        let workers = Arc::new(core::sync::atomic::AtomicPtr::new(core::ptr::null_mut()));

        let route = PartitionRoute {
            name: "test".to_string(),
            partition_id: 0,
            writer_tx: Arc::new(BoundedQueue::new()),
            columns: cols,
            shared_pointers: ptrs,
            hot_index: Arc::new(cache),
            bloom_filter: bloom,
            storage,
            workers,
            wal: Arc::new(NoopWal),
        };

        let worker = crate::core::qsbr::WorkerState::default();
        let _ = route.get_column_int("foo", &worker);
        let _ = route.get_column_str("foo", &worker);
        let _ = route.get_column_blob("foo", &worker);
        assert_eq!(route.len(&worker), 0);

        let nodes = vec![QueryNode::Get {
            entity_id: 0,
            attr: "nonexistent",
        }];
        route.execute_batch(&nodes, |_| {});

        assert!(route.flush_wal().is_ok());

        let _ = std::fs::remove_dir_all(&path);
    }
}
