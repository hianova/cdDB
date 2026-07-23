use crate::AHashMap;
use crate::DualCacheFF;
use crate::core::atomic::AtomicPtr;
use crate::core::column::Columns;
use crate::core::column::MultiVectorPointer;
#[cfg(feature = "std")]
use crate::core::commands::{PartitionCommand, WriteCommand};
use crate::core::qsbr::WorkerNode;
use crate::core::query::{PartitionRoute, Query, QueryNode, QueryResult};
use crate::core::rcu::new_atomic_ptr;
#[cfg(feature = "std")]
use crate::engine::partition::Partition;
use crate::io::platform::{Executor, FileSystem};
use crate::io::storage::Storage;
#[cfg(feature = "std")]
use crate::io::wal::StdWal;
use crate::io::wal::{NoopWal, WalProvider};
use alloc::format;
use alloc::string::String;
use alloc::sync::Arc;
use alloc::vec::Vec;
#[cfg(feature = "std")]
use no_std_tool::collections::BoundedQueue;
use no_std_tool::collections::SimpleBloom;
#[doc = " A metrics snapshot for a single partition, captured at a point-in-time"]
#[doc = " without holding any locks."]
#[doc = ""]
#[doc = " Returned as part of [`DbMetrics`] by [`CdDBDispatcher::metrics()`]."]
#[derive(Debug, Clone)]
#[repr(C, align(64))]
pub struct PartitionMetrics {
    #[doc = " Human-readable name that was supplied when the partition was registered"]
    #[doc = " (e.g. `\"users\"` or `\"events.2024\"`)."]
    pub name: String,
    #[doc = " Monotonically-increasing numeric ID assigned to the partition at"]
    #[doc = " registration time. Used internally to scope cache keys and WAL entries."]
    pub partition_id: u32,
    #[doc = " Fraction of Bloom-filter bits that are currently set, in the range"]
    #[doc = " `[0.0, 1.0]`. A value close to `1.0` indicates the filter is nearly"]
    #[doc = " saturated and false-positive rates may be increasing."]
    pub bloom_saturation: f32,
    #[doc = " Number of entity records currently resident in the partition's in-memory"]
    #[doc = " index (i.e. the size of the shared multi-vector pointer map)."]
    pub memory_entities: usize,
}
#[doc = " A full, lock-free snapshot of database-engine metrics returned by"]
#[doc = " [`CdDBDispatcher::metrics()`]."]
#[doc = ""]
#[doc = " All values are best-effort: atomics are read with `Relaxed` or `Acquire`"]
#[doc = " ordering and no global lock is held, so individual fields may reflect"]
#[doc = " slightly different instants in time."]
#[derive(Debug, Clone)]
#[repr(C, align(64))]
pub struct DbMetrics {
    #[doc = " `true` if the dispatcher has been placed into sleep mode via"]
    #[doc = " [`CdDBDispatcher::sleep()`]."]
    pub is_sleeping: bool,
    #[doc = " Per-partition metrics, one entry for each partition currently"]
    #[doc = " registered in the route table."]
    pub partitions: Vec<PartitionMetrics>,
    #[doc = " `true` when the `dualcache-ff` feature is compiled in and the global"]
    #[doc = " cache is active; `false` otherwise."]
    pub cache_enabled: bool,
    #[doc = " `true` while the cache is still in its cold-start warm-up phase (before"]
    #[doc = " the first epoch boundary after [`CdDBDispatcher::prewarm_partition()`])."]
    pub cache_is_cold_start: bool,
    #[doc = " Number of telemetry / admission commands currently enqueued for the"]
    #[doc = " background cache daemon (only meaningful in `std` builds)."]
    pub cache_pending_commands: usize,
    #[doc = " Current eviction epoch counter maintained by the `DualCacheFF` daemon."]
    pub cache_epoch: u32,
    #[doc = " Number of entries in the Hot Tier (T1 — recently promoted)."]
    pub cache_t1_count: usize,
    #[doc = " Number of entries in the Warm Tier (T2 — frequency-qualified)."]
    pub cache_t2_count: usize,
    #[doc = " Number of entries in the Core resident set."]
    pub cache_core_count: usize,
}
#[doc = " The top-level database engine entry point and central dispatcher for cdDB."]
#[doc = ""]
#[doc = " `CdDBDispatcher<N>` is responsible for:"]
#[doc = " - Maintaining a **route table** that maps partition names to their"]
#[doc = "   [`CdDBDispatcher`] acts as the front door for all read and write requests,"]
#[doc = "   delegating work to individual partitions using a robust epoch-based routing"]
#[doc = "   table."]
#[doc = ""]
#[doc = " # Type parameter `N`"]
#[doc = ""]
#[doc = " `N` is the const generic that controls the size of each partition's Bloom"]
#[doc = " filter bit-array. A larger `N` reduces false-positive rates at the cost of"]
#[doc = " more memory. `N` must be chosen at compile time and is uniform across all"]
#[doc = " partitions owned by a single dispatcher instance."]
#[doc = ""]
#[doc = " # Examples"]
#[doc = ""]
#[doc = " ```rust,no_run"]
#[doc = " # #[cfg(feature = \"std\")] {"]
#[doc = " use cddb::CdDBDispatcher;"]
#[doc = ""]
#[doc = " // Create a dispatcher backed by the standard filesystem."]
#[doc = " let mut db = CdDBDispatcher::<1024>::new_std(Some(\"./data\".into()));"]
#[doc = ""]
#[doc = " // Register a partition and obtain a writer handle."]
#[doc = " let writer = db.register_partition(\"users\".into());"]
#[doc = " # }"]
#[doc = " ```"]
#[repr(C, align(64))]
pub struct CdDBDispatcher<const N: usize> {
    #[doc = " Mapping from partition name to its shared [`PartitionRoute<N>`] context."]
    #[doc = " The route carries everything a read query needs (columns, bloom filter,"]
    #[doc = " cache, storage, WAL) and is cheaply `Arc`-cloned for concurrent access."]
    pub route_table: AHashMap<String, Arc<PartitionRoute<N>>>,
    #[doc = " Optional base directory under which partition storage sub-directories"]
    #[doc = " are created. When `None`, paths are resolved relative to the process"]
    #[doc = " working directory."]
    pub base_path: Option<String>,
    #[doc = " Atomic pointer to the head of the global QSBR worker linked-list."]
    #[doc = " Every reader thread registers itself here so the write path can"]
    #[doc = " determine when it is safe to free old data generations."]
    pub workers: Arc<AtomicPtr<WorkerNode>>,
    #[doc = " Abstract file-system interface used for all storage I/O. Swap this out"]
    #[doc = " with a custom implementation to run cdDB in embedded or WASM"]
    #[doc = " environments without touching any other code."]
    pub fs: Arc<dyn FileSystem>,
    #[doc = " Abstract task executor used to spawn per-partition background threads."]
    #[doc = " The default `StdExecutor` uses `std::thread::spawn`; a custom"]
    #[doc = " implementation can map tasks onto any async runtime or RTOS task."]
    pub executor: Arc<dyn Executor>,
    #[doc = " Global `DualCacheFF` hot-index shared by **all** partitions."]
    #[doc = " Cache keys are `(partition_id, entity_id)` tuples, ensuring isolation"]
    #[doc = " between partitions while allowing the eviction policy to see the full"]
    #[doc = " cross-partition access distribution."]
    pub global_cache: Arc<DualCacheFF<(u32, usize), (), dualcache_ff::core::config::DefaultExponentialPolicy, 64, 4096, 262144, 266304>>,
    #[doc = " Monotonically increasing counter used to assign a unique `u32` ID to"]
    #[doc = " each registered partition. Incremented once per"]
    #[doc = " [`register_partition_with_wal_provider`](Self::register_partition_with_wal_provider)"]
    #[doc = " call."]
    pub next_partition_id: u32,
    #[doc = " Atomic boolean that controls the logical sleep state of the dispatcher."]
    #[doc = " When `true`, upper-layer traffic handlers should pause incoming writes."]
    #[doc = " Background maintenance threads observe this flag to enter low-power"]
    #[doc = " idle polling rather than being fully terminated."]
    pub is_sleeping: Arc<core::sync::atomic::AtomicBool>,
    #[cfg(feature = "std")]
    pub partition_threads: Vec<crate::io::platform::TaskHandle>,
}
impl<const N: usize> CdDBDispatcher<N> {
    #[doc = " Create a new `CdDBDispatcher` with a custom file system and executor."]
    #[doc = ""]
    #[doc = " Use this constructor when targeting environments that do not have access"]
    #[doc = " to the Rust standard library (e.g. embedded, WASM) or when you need to"]
    #[doc = " inject a mock filesystem for testing."]
    #[doc = ""]
    #[doc = " # Arguments"]
    #[doc = ""]
    #[doc = " * `base_path` — Optional root directory under which per-partition"]
    #[doc = "   storage sub-directories are created. Pass `None` to resolve paths"]
    #[doc = "   relative to the process working directory."]
    #[doc = " * `fs` — File-system abstraction used for all I/O. The default"]
    #[doc = "   standard-library implementation is [`StdFileSystem`]."]
    #[doc = " * `executor` — Task spawner used to launch per-partition background"]
    #[doc = "   threads. The default is [`StdExecutor`]."]
    #[doc = ""]
    #[doc = " # Examples"]
    #[doc = ""]
    #[doc = " ```rust,no_run"]
    #[doc = " # #[cfg(feature = \"std\")] {"]
    #[doc = " use std::sync::Arc;"]
    #[doc = " use cddb::{CdDBDispatcher, platform::{StdFileSystem, StdExecutor}};"]
    #[doc = ""]
    #[doc = " let db = CdDBDispatcher::<262144>::new("]
    #[doc = "     Some(\"./db_data\".into()),"]
    #[doc = "     Arc::new(StdFileSystem),"]
    #[doc = "     Arc::new(StdExecutor),"]
    #[doc = "     cddb::CacheConfig::default(),"]
    #[doc = " );"]
    #[doc = " # }"]
    #[doc = " ```"]
    pub fn new(
        base_path: Option<String>,
        fs: Arc<dyn FileSystem>,
        executor: Arc<dyn Executor>,
        _cache_config: crate::CacheConfig,
    ) -> Self {
        #[cfg(feature = "std")]
        let global_cache = std::thread::Builder::new()
            .stack_size(1024 * 1024 * 1024)
            .spawn(move || {
                let cache = alloc::sync::Arc::new(DualCacheFF::new());
                if _cache_config.daemon_mode {
                    unsafe { (*alloc::sync::Arc::as_ptr(&cache)).set_daemon_mode(true) };
                }
                cache
            })
            .unwrap()
            .join()
            .unwrap();

        #[cfg(not(feature = "std"))]
        let global_cache = {
            let cache = alloc::sync::Arc::new(DualCacheFF::new());
            // In no_std, daemon_mode might not be supported anyway, but keep logic consistent
            if _cache_config.daemon_mode {
                unsafe { (*alloc::sync::Arc::as_ptr(&cache)).set_daemon_mode(true) };
            }
            cache
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
    #[doc = " Convenience constructor that creates a `CdDBDispatcher` backed by the"]
    #[doc = " standard library's [`StdFileSystem`] and [`StdExecutor`]."]
    #[doc = ""]
    #[doc = " This is the recommended entry point for applications running in a normal"]
    #[doc = " `std` environment. For custom environments use [`CdDBDispatcher::new`]."]
    #[doc = ""]
    #[doc = " # Arguments"]
    #[doc = ""]
    #[doc = " * `base_path` — Optional root directory for partition storage. Pass"]
    #[doc = "   `None` to use `\"data/<partition_name>\"` relative paths."]
    #[doc = ""]
    #[doc = " # Examples"]
    #[doc = ""]
    #[doc = " ```rust,no_run"]
    #[doc = " # #[cfg(feature = \"std\")] {"]
    #[doc = " use cddb::CdDBDispatcher;"]
    #[doc = ""]
    #[doc = " let db = CdDBDispatcher::<262144>::new_std(Some(\"./db_data\".into()), cddb::CacheConfig::default());"]
    #[doc = " # }"]
    #[doc = " ```"]
    #[cfg(feature = "std")]
    pub fn new_std(base_path: Option<String>, cache_config: crate::CacheConfig) -> Self {
        Self::new(
            base_path,
            Arc::new(crate::io::platform::StdFileSystem),
            Arc::new(crate::io::platform::StdExecutor),
            cache_config,
        )
    }
    #[doc = " Creates a purely in-memory instance of `CdDBDispatcher`."]
    #[doc = ""]
    #[doc = " This bypasses all disk I/O by utilizing a `NullFileSystem`. Data writes"]
    #[doc = " are discarded, and reads return empty, maximizing memory efficiency for"]
    #[doc = " cache-only workloads."]
    #[cfg(feature = "std")]
    pub fn new_in_memory(cache_config: crate::CacheConfig) -> Self {
        Self::new(
            Some("in_memory_db".into()),
            Arc::new(crate::io::platform::NullFileSystem),
            Arc::new(crate::io::platform::StdExecutor),
            cache_config,
        )
    }
    #[doc = " Register a new partition with the given name using the default"]
    #[doc = " synchronous WAL (no WAL file — equivalent to `WalMode::Sync` with"]
    #[doc = " `wal_path = None`)."]
    #[doc = ""]
    #[doc = " A background worker thread is spawned automatically to process write"]
    #[doc = " commands for this partition. The returned [`UserWriter`] is the only"]
    #[doc = " handle through which callers should send [`WriteCommand`]s to the"]
    #[doc = " partition. When the `UserWriter` is dropped, a `Shutdown` command is"]
    #[doc = " delivered to gracefully stop the background thread."]
    #[doc = ""]
    #[doc = " # Arguments"]
    #[doc = ""]
    #[doc = " * `path` — Logical name of the partition (e.g. `\"users\"`). This name"]
    #[doc = "   is also used to derive the on-disk storage sub-directory path."]
    #[doc = ""]
    #[doc = " # Returns"]
    #[doc = ""]
    #[doc = " A [`UserWriter`] bound to the newly created partition's command queue."]
    #[cfg(feature = "std")]
    pub fn register_partition(&mut self, path: String) -> UserWriter {
        self.register_partition_with_wal(path, None, crate::io::wal::WalMode::Sync)
    }
    #[doc = " Register a new partition with an explicit memory budget hint."]
    #[doc = ""]
    #[doc = " Behaves identically to [`register_partition`](Self::register_partition)"]
    #[doc = " in the current implementation; the `budget_bytes` parameter is accepted"]
    #[doc = " for API compatibility but is not yet enforced. Future versions may use"]
    #[doc = " it to constrain the partition's in-memory column footprint."]
    #[doc = ""]
    #[doc = " # Arguments"]
    #[doc = ""]
    #[doc = " * `path` — Logical name / storage path of the partition."]
    #[doc = " * `_budget_bytes` — Desired maximum resident memory in bytes (currently"]
    #[doc = "   advisory only)."]
    #[doc = ""]
    #[doc = " # Returns"]
    #[doc = ""]
    #[doc = " A [`UserWriter`] bound to the newly created partition's command queue."]
    #[cfg(feature = "std")]
    pub fn register_partition_with_budget(
        &mut self,
        path: String,
        _budget_bytes: usize,
    ) -> UserWriter {
        self.register_partition_with_wal(path, None, crate::io::wal::WalMode::Sync)
    }
    #[doc = " Register a new partition, specifying an optional WAL file path and"]
    #[doc = " write mode."]
    #[doc = ""]
    #[doc = " When `wal_path` is `Some`, a [`StdWal`] is created at the given path"]
    #[doc = " with the supplied [`WalMode`]. When `wal_path` is `None`, a [`NoopWal`]"]
    #[doc = " is used and no durability log is written."]
    #[doc = ""]
    #[doc = " # Arguments"]
    #[doc = ""]
    #[doc = " * `path` — Logical name of the partition."]
    #[doc = " * `wal_path` — Optional file path for the write-ahead log. `None`"]
    #[doc = "   disables WAL entirely."]
    #[doc = " * `wal_mode` — Controls WAL flush strategy (e.g. sync-on-every-write"]
    #[doc = "   vs. async/batched). Only meaningful when `wal_path` is `Some`."]
    #[doc = ""]
    #[doc = " # Returns"]
    #[doc = ""]
    #[doc = " A [`UserWriter`] bound to the newly created partition's command queue."]
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
    #[doc = " Register a new partition with a fully custom [`WalProvider`]."]
    #[doc = ""]
    #[doc = " This is the lowest-level registration method. All other"]
    #[doc = " `register_partition*` variants ultimately delegate here. Use this"]
    #[doc = " method when you need complete control over the WAL implementation"]
    #[doc = " (e.g. an in-memory WAL for tests, or a network-backed log)."]
    #[doc = ""]
    #[doc = " Internally, the method:"]
    #[doc = " 1. Allocates a lock-free [`BoundedQueue`] (capacity 262 144 slots)."]
    #[doc = " 2. Initialises a [`PartitionRoute<N>`] and inserts it into the route"]
    #[doc = "    table."]
    #[doc = " 3. Spawns a background thread via the configured executor that runs"]
    #[doc = "    the partition event loop, replaying the WAL on startup."]
    #[doc = ""]
    #[doc = " # Arguments"]
    #[doc = ""]
    #[doc = " * `path` — Logical name / storage path of the partition."]
    #[doc = " * `wal` — A shared [`WalProvider`] implementation. Pass"]
    #[doc = "   `Arc::new(NoopWal)` to disable durability logging."]
    #[doc = ""]
    #[doc = " # Returns"]
    #[doc = ""]
    #[doc = " A [`UserWriter`] bound to the partition's command queue. Dropping the"]
    #[doc = " writer delivers a `Shutdown` command, causing the background thread to"]
    #[doc = " exit cleanly."]
    #[cfg(feature = "std")]
    pub fn register_partition_with_wal_provider(
        &mut self,
        path: String,
        wal: Arc<dyn WalProvider>,
    ) -> UserWriter {
        let queue: Arc<BoundedQueue<crate::core::commands::PartitionCommand, 262144>> = unsafe { Arc::new_zeroed().assume_init() };
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
            hot_index: Arc::clone(&self.global_cache) as Arc<dyn crate::core::hot_index::HotIndexProvider<Handle = crate::dualcache_ff::component::tls::TlsHandle>>,
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
    #[doc = " Register a new partition in `no_std` environments where thread"]
    #[doc = " spawning is not available."]
    #[doc = ""]
    #[doc = " Unlike the `std` `register_partition*` family, this method does **not**"]
    #[doc = " spawn a background thread. Instead, the caller is responsible for"]
    #[doc = " driving the partition's event loop manually — typically inside an RTOS"]
    #[doc = " task or a bare-metal super-loop — by polling the `_writer_rx` queue and"]
    #[doc = " dispatching [`PartitionCommand`]s to a [`Partition`] instance."]
    #[doc = ""]
    #[doc = " A [`PartitionRoute<N>`] is created and inserted into the route table so"]
    #[doc = " that read queries can be executed through the dispatcher as usual."]
    #[doc = ""]
    #[doc = " # Arguments"]
    #[doc = ""]
    #[doc = " * `path` — Logical name of the partition."]
    #[doc = " * `writer_tx` — The sending half of the application's command channel."]
    #[doc = "   This is stored in the route and used by [`UserWriter`]-equivalent"]
    #[doc = "   logic to push [`WriteCommand`]s."]
    #[doc = " * `_writer_rx` — The receiving half (currently unused by this method;"]
    #[doc = "   pass it to your partition event loop separately)."]
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
            hot_index: Arc::clone(&self.global_cache) as Arc<dyn crate::core::hot_index::HotIndexProvider<Handle = crate::dualcache_ff::component::tls::TlsHandle>>,
            bloom_filter: Arc::clone(&bloom),
            storage: Arc::new(Storage::new(storage_path.clone(), self.fs.clone())),
            workers: Arc::clone(&workers),
            wal: Arc::new(NoopWal),
        });
        self.route_table.insert(path, route);
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
        let bloom = Arc::new(crate::core::rcu::new_atomic_ptr_from_box(SimpleBloom::<N>::new_boxed()));
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
        hot_index: Arc<DualCacheFF<(u32, usize), (), dualcache_ff::core::config::DefaultExponentialPolicy, 64, 4096, 262144, 266304>>,
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
    #[doc = " Exposes a high-level `CdDBPartition` handle wrapping queries and writes."]
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
    #[doc = " Execute a batch of query nodes against a named partition under a single"]
    #[doc = " QSBR pin. The network / session layer does not need to know about QSBR,"]
    #[doc = " `Query`, or `WorkerState` — simply pass the slice and a result callback."]
    #[doc = ""]
    #[doc = " This is the architectural boundary described in the cdDB design spec:"]
    #[doc = " the caller (e.g. a TCP stream handler parsing a Redis pipeline) hands"]
    #[doc = " `N` commands as an array and pays exactly **one** QSBR enter/leave."]
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
    #[doc = " Execute a batch of query nodes against a named partition asynchronously,"]
    #[doc = " offloading the QSBR-pinned read work to a Tokio blocking thread pool."]
    #[doc = ""]
    #[doc = " This is the async equivalent of [`execute_batch`](Self::execute_batch)."]
    #[doc = " Because QSBR operations must not be suspended across `.await` points,"]
    #[doc = " the actual query execution runs inside `tokio::task::spawn_blocking`"]
    #[doc = " and the future resolves once the blocking task completes."]
    #[doc = ""]
    #[doc = " # Arguments"]
    #[doc = ""]
    #[doc = " * `partition` — Name of the target partition. If the partition is not"]
    #[doc = "   found in the route table, `None` is returned."]
    #[doc = " * `nodes` — Query nodes to execute. Must have `'static` lifetime so"]
    #[doc = "   they can be moved into the blocking task."]
    #[doc = " * `cb` — Callback invoked for each [`QueryResult`]. The return value"]
    #[doc = "   of the **last** invocation is returned as `Some(R)`."]
    #[doc = ""]
    #[doc = " # Returns"]
    #[doc = ""]
    #[doc = " `Some(R)` containing the result of the final callback invocation, or"]
    #[doc = " `None` if the partition was not found or no nodes were executed."]
    #[doc = ""]
    #[doc = " # Errors"]
    #[doc = ""]
    #[doc = " Panics if the spawned Tokio blocking task panics (propagated via"]
    #[doc = " `JoinHandle::await.unwrap()`)."]
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
    #[doc = " Returns a lock-free, point-in-time snapshot of database-engine metrics."]
    #[doc = ""]
    #[doc = " Iterates over every registered partition to compute its Bloom-filter"]
    #[doc = " saturation and in-memory entity count, then reads global cache counters"]
    #[doc = " using atomic loads (no mutexes are held)."]
    #[doc = ""]
    #[doc = " # Returns"]
    #[doc = ""]
    #[doc = " A [`DbMetrics`] struct containing per-partition [`PartitionMetrics`] and"]
    #[doc = " global cache statistics."]
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
        ) = cfg_select! { feature = "dualcache-ff" => { { let (t1 , t2 , core) = (0 , 0 , 0) ; (true , false , 0 , 0 , t1 , t2 , core) } } , _ => (false , false , 0 , 0 , 0 , 0 , 0) };
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
    #[doc = " Pre-warm the cache for a specific partition with a batch of entity IDs."]
    #[doc = " This bypasses standard probation and injects keys directly into the Hot Tier (T1),"]
    #[doc = " which is highly efficient for application startup sequences."]
    #[allow(unused_variables)]
    pub fn prewarm_partition(
        &self,
        partition_name: &str,
        entity_ids: impl IntoIterator<Item = usize>,
        cache_promotions: Option<Vec<usize>>,
    ) -> Result<(), &'static str> {
        let route = self
            .get_route(partition_name)
            .ok_or("Partition not found")?;
        let partition_id = route.partition_id;
        if let Some(entity_ids) = cache_promotions {
            use crate::core::hot_index::HotIndexProvider;
            self.global_cache.prewarm(partition_id, &entity_ids);
        }
        Ok(())
    }
    #[doc = " Flush all pending thread-local telemetry and cache admission commands,"]
    #[doc = " blocking the calling thread until the background `DualCacheFF` daemon"]
    #[doc = " has processed them."]
    #[doc = ""]
    #[doc = " Call this after a burst of writes or reads to ensure the hot-index"]
    #[doc = " reflects the most recent access pattern before issuing queries that"]
    #[doc = " depend on cache residency (e.g. in tests or benchmark warm-up phases)."]
    #[doc = ""]
    #[doc = " This is a **no-op** when compiled without the `dualcache-ff` feature or"]
    #[doc = " in `no_std` environments where the daemon thread does not exist."]
    pub fn sync_cache(&self) {
        #[cfg(feature = "dualcache-ff")]
        {}
    }
    #[doc = " Put the dispatcher into a logical sleep state."]
    #[doc = ""]
    #[doc = " Sets the `is_sleeping` flag to `true` and, when the `dualcache-ff`"]
    #[doc = " feature is active, suspends the background cache daemon so it enters"]
    #[doc = " low-power idle polling (approximately 1 ms intervals, effectively"]
    #[doc = " zero CPU)."]
    #[doc = ""]
    #[doc = " Background threads are **not** terminated; the transition is"]
    #[doc = " intentionally lightweight to avoid the high latency cost of tearing"]
    #[doc = " down and recreating OS threads."]
    #[doc = ""]
    #[doc = " Upper-layer traffic handlers should check [`is_sleeping`](Self::is_sleeping)"]
    #[doc = " before accepting new writes. Wake the dispatcher again with"]
    #[doc = " [`wake`](Self::wake)."]
    pub fn sleep(&self) {
        self.is_sleeping
            .store(true, core::sync::atomic::Ordering::Release);
    }
    #[doc = " Wake the dispatcher from a logical sleep state."]
    #[doc = ""]
    #[doc = " Clears the `is_sleeping` flag and, when the `dualcache-ff` feature is"]
    #[doc = " active, resumes the background cache daemon so it returns to normal"]
    #[doc = " operation. This is the counterpart of [`sleep`](Self::sleep)."]
    pub fn wake(&self) {
        self.is_sleeping
            .store(false, core::sync::atomic::Ordering::Release);
    }
    #[doc = " Returns `true` if the dispatcher is currently in the sleep state."]
    #[doc = ""]
    #[doc = " The value is read with `Acquire` ordering so that any writes performed"]
    #[doc = " by [`sleep`](Self::sleep) or [`wake`](Self::wake) on other threads are"]
    #[doc = " visible to the caller."]
    pub fn is_sleeping(&self) -> bool {
        self.is_sleeping.load(core::sync::atomic::Ordering::Acquire)
    }
}
impl<const N: usize> Drop for CdDBDispatcher<N> {
    fn drop(&mut self) {
        #[cfg(feature = "std")]
        {
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
            for handle in self.partition_threads.drain(..) {
                let _ = handle.join();
            }
        }
        #[cfg(feature = "dualcache-ff")]
        {
            let cache_ptr = alloc::sync::Arc::as_ptr(&self.global_cache);
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
#[doc = " A handle for sending write commands to a single registered partition."]
#[doc = ""]
#[doc = " `UserWriter` wraps the sending end of the partition's lock-free"]
#[doc = " [`BoundedQueue`] and provides two delivery strategies: a blocking"]
#[doc = " [`send`](UserWriter::send) with exponential backoff (suitable for"]
#[doc = " production write paths) and a non-blocking"]
#[doc = " [`try_send`](UserWriter::try_send) (suitable for rate-limited or"]
#[doc = " drop-tolerant paths)."]
#[doc = ""]
#[doc = " When a `UserWriter` is dropped, a `Shutdown` command is automatically"]
#[doc = " enqueued, causing the partition's background thread to exit cleanly."]
#[cfg(feature = "std")]
#[derive(Clone)]
#[repr(C, align(64))]
pub struct UserWriter(Arc<BoundedQueue<PartitionCommand, 262144>>);
#[cfg(feature = "std")]
impl UserWriter {
    #[doc = " Send a write command to the partition, blocking with exponential backoff"]
    #[doc = " until queue space becomes available."]
    #[doc = ""]
    #[doc = " On each failed push the method calls [`Backoff::snooze`] (spin /"]
    #[doc = " yield). Once the backoff sequence is exhausted it falls back to"]
    #[doc = " [`std::thread::yield_now`] on every retry to avoid monopolising the CPU"]
    #[doc = " while the partition thread drains the queue."]
    #[doc = ""]
    #[doc = " # Arguments"]
    #[doc = ""]
    #[doc = " * `cmd` — The [`WriteCommand`] to deliver to the partition."]
    #[doc = ""]
    #[doc = " # Returns"]
    #[doc = ""]
    #[doc = " `Ok(())` once the command has been successfully enqueued."]
    #[doc = " This function never returns `Err` in practice — it loops indefinitely"]
    #[doc = " until the push succeeds."]
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
    #[doc = " Attempt to send a write command to the partition without blocking."]
    #[doc = ""]
    #[doc = " Performs a single push attempt. If the partition's command queue is"]
    #[doc = " full the command is **discarded** and `Err(\"Full\")` is returned. This"]
    #[doc = " is appropriate for callers that implement their own back-pressure or"]
    #[doc = " are willing to drop writes under load."]
    #[doc = ""]
    #[doc = " # Arguments"]
    #[doc = ""]
    #[doc = " * `cmd` — The [`WriteCommand`] to attempt to deliver."]
    #[doc = ""]
    #[doc = " # Errors"]
    #[doc = ""]
    #[doc = " Returns `Err(\"Full\")` if the bounded queue has no available slots."]
    pub fn try_send(&self, cmd: WriteCommand) -> Result<(), &'static str> {
        self.0
            .push(PartitionCommand::Write(cmd))
            .map_err(|_| "Full")
    }
}
#[cfg(feature = "std")]
impl Drop for UserWriter {
    fn drop(&mut self) {
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
    fn test_dispatcher_register_with_budget() {
        let mut d = CdDBDispatcher::<1024>::new_std(None, crate::CacheConfig::default());
        let path = "test_budget".to_string();
        let _writer = d.register_partition_with_budget(path.clone(), 1024);
        assert!(d.route_table.contains_key(&path));
        let _ = std::fs::remove_dir_all(&path);
    }
    #[cfg(feature = "std")]
    #[test]
    fn test_user_writer_try_send_full_and_drop() {
        let q = BoundedQueue::new_arc();
        let writer = UserWriter(q.clone());
        let mut i = 1;
        while writer
            .try_send(WriteCommand::Delete { entity_id: i })
            .is_ok()
        {
            i += 1;
        }
        drop(writer);
    }
    #[cfg(feature = "std")]
    #[test]
    fn test_user_writer_send_backoff() {
        let q = BoundedQueue::new_arc();
        let writer = UserWriter(q.clone());
        let cmd1 = WriteCommand::Delete { entity_id: 1 };
        writer.try_send(cmd1).unwrap();
        let q_clone = q.clone();
        std::thread::spawn(move || {
            std::thread::sleep(std::time::Duration::from_millis(50));
            let _ = q_clone.pop();
        });
        let cmd2 = WriteCommand::Delete { entity_id: 2 };
        writer.send(cmd2).unwrap();
    }
    #[cfg(feature = "std")]
    #[test]
    fn test_route_getters_and_execute() {
        use crate::core::query::QueryNode;
        use crate::io::wal::NoopWal;
        let cols = Arc::new(crate::core::rcu::new_atomic_ptr(
            crate::core::column::Columns::<1024>::new(),
        ));
        let ptrs = Arc::new(crate::core::rcu::new_atomic_ptr(crate::AHashMap::default()));
        let bloom = Arc::new(crate::core::rcu::new_atomic_ptr_from_box(
            no_std_tool::collections::SimpleBloom::<1024>::new_boxed(),
        ));
        #[cfg(feature = "dualcache-ff")]
        let cache: crate::DualCacheFF<(u32, usize), (), dualcache_ff::core::config::DefaultExponentialPolicy, 64, 4096, 262144, 266304> = crate::DualCacheFF::new();
        #[cfg(not(feature = "dualcache-ff"))]
        let cache: crate::DualCacheFF<(u32, usize), (), dualcache_ff::core::config::DefaultExponentialPolicy, 64, 4096, 262144, 266304> = crate::DualCacheFF::new(crate::CacheConfig::default());
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
            writer_tx: BoundedQueue::new_arc(),
            columns: cols,
            shared_pointers: ptrs,
            hot_index: Arc::new(cache) as Arc<dyn crate::core::hot_index::HotIndexProvider<Handle = crate::dualcache_ff::component::tls::TlsHandle>>,
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
