use crate::core::AHashMap;
use no_std_tool::collections::SimpleBloom;
use crate::core::column::{ColumnArray, Columns, MultiVectorPointer};
#[cfg(feature = "std")]
use crate::core::commands::PartitionCommand;
use crate::core::qsbr::{WorkerNode, WorkerState};
#[cfg(feature = "std")]
use no_std_tool::collections::BoundedQueue;
use crate::core::rcu::load_ref;
use crate::io::storage::Storage;
use crate::io::wal::WalProvider;
use alloc::string::String;
use alloc::sync::Arc;
use alloc::vec::Vec;
use core::sync::atomic::AtomicPtr;
#[cfg(feature = "dualcache-ff")]
use dualcache_ff::DualCacheFF;

/// A single logical operation within a query plan.
///
/// Each variant describes one step of work the query engine should perform.
/// Multiple `QueryNode`s are combined into a [`CdDbQuery`] and executed
/// sequentially by [`QuerySession::execute_with_cb`].
#[derive(Debug, Clone)]
pub enum QueryNode<'a> {
    /// Look up a single attribute value for a specific entity.
    ///
    /// The engine tries integer, string, and blob columns in order and returns
    /// the first match. Yields [`QueryResult::None`] when the entity or
    /// attribute does not exist.
    Get {
        /// The entity ID to look up.
        entity_id: usize,
        /// The name of the attribute column to read.
        attr: &'a str,
    },
    /// Follow an integer foreign-key attribute on one entity to read an
    /// attribute on the referenced entity (within the same partition).
    ///
    /// The value stored at `link_attr` on `from_entity_id` is used as the
    /// target entity ID. Yields [`QueryResult::None`] when either the source
    /// or the resolved target entity/attribute is missing.
    Link {
        /// The entity ID whose `link_attr` holds the foreign key.
        from_entity_id: usize,
        /// The attribute on the source entity that stores the target entity ID.
        link_attr: &'a str,
        /// The attribute to read on the resolved target entity.
        target_attr: &'a str,
    },
    /// Read a contiguous slice of integer values starting at a specific entity.
    ///
    /// Yields [`QueryResult::IntRange`] containing up to `len` values, or
    /// [`QueryResult::None`] when the entity or attribute is not found.
    Range {
        /// The entity ID that anchors the start of the range.
        entity_id: usize,
        /// The name of the integer attribute column to read.
        attr: &'a str,
        /// The maximum number of successive elements to return.
        len: usize,
    },
    /// Perform a full vectorized scan over an entire attribute column.
    ///
    /// Returns all non-null values as [`QueryResult::IntList`],
    /// [`QueryResult::StrList`], or [`QueryResult::BlobList`] depending on
    /// the column type. Yields [`QueryResult::None`] when the column does not
    /// exist.
    Scan {
        /// The name of the attribute column to scan.
        attr: &'a str,
    },
    /// Apply a vectorized aggregation operation to an integer attribute column.
    ///
    /// Yields an appropriate [`QueryResult`] variant (e.g. [`QueryResult::IntSum`]).
    /// Returns [`QueryResult::None`] when the column is not found.
    Aggregate {
        /// The name of the integer attribute column to aggregate.
        attr: &'a str,
        /// The aggregation function to apply.
        op: AggregateOp,
    },
}

/// Supported aggregation operations.
#[derive(Debug, Clone)]
pub enum AggregateOp {
    /// Calculate the sum.
    Sum,
    /// Calculate the average.
    Avg,
    /// Find the minimum value.
    Min,
    /// Find the maximum value.
    Max,
    /// Count the total number of elements.
    Count,
}

/// Represents a complete query composed of multiple query nodes.
#[derive(Debug, Clone)]
pub struct CdDbQuery<'a> {
    /// The sequence of operations to perform.
    pub nodes: Vec<QueryNode<'a>>,
}

/// The output of a query node operation.
#[derive(Debug, Clone)]
pub enum QueryResult<'a> {
    /// String result.
    Str(String),
    /// Integer result.
    Int(u32),
    /// Blob result.
    Blob(Vec<u8>),
    /// Range of integers.
    IntRange(&'a [u32]),
    /// Sum of integers.
    IntSum(u64),
    /// Average of integers.
    IntAvg(f64),
    /// Minimum integer.
    IntMin(u32),
    /// Maximum integer.
    IntMax(u32),
    /// Count result.
    Count(usize),
    /// List of integers.
    IntList(&'a [u32]),
    /// List of strings.
    StrList(&'a [&'a str]),
    /// List of blobs.
    BlobList(&'a [&'a [u8]]),
    /// No result.
    None,
}

/// A simple bump (arena) allocator that keeps allocated slices alive for
/// the lifetime of the owning [`QuerySession`].
///
/// Each call to [`Bump::alloc`] moves a `Vec<T>` into internal storage and
/// returns a raw slice reference into that storage. Because the backing
/// storage never reallocates individual chunks, the returned pointers remain
/// stable until the `Bump` itself is dropped. This allows zero-copy results
/// to be returned from scan operations without per-query heap allocation.
pub struct Bump<T> {
    chunks: core::cell::RefCell<Vec<Vec<T>>>,
}

impl<T: Clone> Default for Bump<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T: Clone> Bump<T> {
    /// Create a new, empty `Bump` allocator.
    pub fn new() -> Self {
        Self {
            chunks: core::cell::RefCell::new(Vec::new()),
        }
    }

    /// Push `data` into the internal arena and return a stable slice reference.
    ///
    /// The returned slice is valid for as long as the `Bump` itself is alive.
    /// Ownership of `data` is transferred to the arena; no copy is made of the
    /// element values beyond what already exists in the `Vec`.
    ///
    /// # Safety
    ///
    /// This function uses `core::slice::from_raw_parts` to extend the lifetime
    /// of the slice reference. Safety is upheld because the backing `Vec` is
    /// stored inside the `Bump` and never removed or reallocated while any
    /// returned reference may still be in use.
    pub fn alloc(&self, data: Vec<T>) -> &[T] {
        let mut chunks = self.chunks.borrow_mut();
        chunks.push(data);
        let last = chunks.last().unwrap();
        unsafe { core::slice::from_raw_parts(last.as_ptr(), last.len()) }
    }
}

/// A query executor bound to a specific partition and QSBR worker thread.
///
/// `Query` owns the [`WorkerState`] registration for its lifetime. All query
/// execution is driven through either a short-lived [`QuerySession`] (obtained
/// via [`Query::session`]) or the convenience helper methods.
pub struct Query<'a, const N: usize> {
    route: &'a PartitionRoute<N>,
    worker: Arc<WorkerState>,
}

/// An active query session that holds arena allocators and a QSBR pin.
///
/// A single QSBR critical-section pin covers the entire session: the pin is
/// entered when the session is created and released when it is dropped.
/// All scan and range results backed by the internal [`Bump`] arenas remain
/// valid until the session is dropped.
///
/// Create a session via [`Query::session`] rather than calling
/// [`QuerySession::new`] directly.
pub struct QuerySession<'a, const N: usize> {
    route: &'a PartitionRoute<N>,
    worker: &'a WorkerState,
    int_arena: Bump<u32>,
    str_arena: Bump<&'a str>,
    blob_arena: Bump<&'a [u8]>,
}

impl<'a, const N: usize> Query<'a, N> {
    /// Create a new `Query` bound to the given partition route.
    ///
    /// Registers a QSBR worker for this query executor. The worker remains
    /// registered for the lifetime of the `Query` instance.
    ///
    /// # Examples
    ///
    /// ```rust,ignore
    /// let route: &PartitionRoute<1024> = /* … */;
    /// let query = Query::new(route);
    /// let score = query.get_int(0, "score");
    /// ```
    pub fn new(route: &'a PartitionRoute<N>) -> Self {
        let worker = route.register_worker();
        Self { route, worker }
    }

    /// Create a [`QuerySession`], entering the QSBR critical section.
    ///
    /// The session borrows `self` for its duration, ensuring the underlying
    /// worker registration stays valid. The QSBR pin is held until the
    /// returned `QuerySession` is dropped.
    pub fn session(&self) -> QuerySession<'_, N> {
        QuerySession::new(self.route, &self.worker)
    }

    /// Execute a batch of query nodes, invoking the callback for each result.
    ///
    /// This is a convenience wrapper that creates a temporary [`QuerySession`],
    /// runs all nodes through [`QuerySession::execute_with_cb`], and then
    /// drops the session (releasing the QSBR pin).
    pub fn execute_with_cb<'b, F>(&self, nodes: &[QueryNode<'b>], cb: F)
    where
        F: FnMut(QueryResult<'_>),
    {
        self.session().execute_with_cb(nodes, cb);
    }

    /// Helper: Execute a single [`QueryNode::Get`] for an integer attribute.
    ///
    /// Returns `Some(value)` if the entity exists and the attribute is an
    /// integer column, otherwise `None`.
    pub fn get_int(&self, entity_id: usize, attr: &str) -> Option<u32> {
        self.session().get_int(entity_id, attr)
    }

    /// Helper: Execute a single [`QueryNode::Get`] for a string attribute.
    ///
    /// Returns `Some(value)` if the entity exists and the attribute is a
    /// string column, otherwise `None`.
    pub fn get_str(&self, entity_id: usize, attr: &str) -> Option<String> {
        self.session().get_str(entity_id, attr)
    }

    /// Helper: Execute a single [`QueryNode::Get`] for a blob attribute.
    ///
    /// Returns `Some(value)` if the entity exists and the attribute is a
    /// blob column, otherwise `None`.
    pub fn get_blob(&self, entity_id: usize, attr: &str) -> Option<Vec<u8>> {
        self.session().get_blob(entity_id, attr)
    }
}

impl<'a, const N: usize> QuerySession<'a, N> {
    /// Private constructor — enters the QSBR critical section.
    ///
    /// Prefer [`Query::session`] over calling this directly. The QSBR pin
    /// acquired here is released by the [`Drop`] implementation.
    pub fn new(route: &'a PartitionRoute<N>, worker: &'a WorkerState) -> Self {
        worker.enter();
        Self {
            route,
            worker,
            int_arena: Bump::new(),
            str_arena: Bump::new(),
            blob_arena: Bump::new(),
        }
    }

    /// Dispatch a slice of [`QueryNode`]s and call `cb` once for each result.
    ///
    /// Nodes are processed sequentially in order. For each node the callback
    /// receives exactly one [`QueryResult`]. The callback may not outlive the
    /// session because scan results are backed by the session's internal arenas.
    pub fn execute_with_cb<'b, F>(&self, nodes: &[QueryNode<'b>], mut cb: F)
    where
        F: FnMut(QueryResult),
    {
        for node in nodes {
            match node {
                QueryNode::Get { entity_id, attr } => {
                    if let Some(v) = self.get_int(*entity_id, attr) {
                        cb(QueryResult::Int(v));
                    } else if let Some(v) = self.get_str(*entity_id, attr) {
                        cb(QueryResult::Str(v));
                    } else if let Some(v) = self.get_blob(*entity_id, attr) {
                        cb(QueryResult::Blob(v));
                    } else {
                        cb(QueryResult::None);
                    }
                }
                QueryNode::Link {
                    from_entity_id,
                    link_attr,
                    target_attr,
                } => {
                    if let Some(target_id) = self.get_int(*from_entity_id, link_attr) {
                        let target_id = target_id as usize;
                        if let Some(v) = self.get_int(target_id, target_attr) {
                            cb(QueryResult::Int(v));
                        } else if let Some(v) = self.get_str(target_id, target_attr) {
                            cb(QueryResult::Str(v));
                        } else if let Some(v) = self.get_blob(target_id, target_attr) {
                            cb(QueryResult::Blob(v));
                        } else {
                            cb(QueryResult::None);
                        }
                    } else {
                        cb(QueryResult::None);
                    }
                }
                QueryNode::Range {
                    entity_id,
                    attr,
                    len,
                } => {
                    if let Some(ptr) = self.get_pointer(*entity_id)
                        && let Some(&start_idx) = ptr.attribute_indices.get(*attr)
                        && let Some(col) = self.route.get_column_int(attr, self.worker)
                    {
                        let range_vals = col.with_data_pinned(|data| {
                            data.iter()
                                .skip(start_idx)
                                .take(*len)
                                .flatten()
                                .cloned()
                                .collect::<Vec<u32>>()
                        });
                        let slice = self.int_arena.alloc(range_vals);
                        cb(QueryResult::IntRange(slice));
                        continue;
                    }
                    cb(QueryResult::None);
                }
                QueryNode::Scan { attr } => {
                    if let Some(col) = self.route.get_column_int(attr, self.worker) {
                        let vals = col.with_data_pinned(|data| {
                            data.iter().flatten().cloned().collect::<Vec<u32>>()
                        });
                        let slice = self.int_arena.alloc(vals);
                        cb(QueryResult::IntList(slice));
                    } else if let Some(col) = self.route.get_column_str(attr, self.worker) {
                        let vals = col.with_data_pinned(|data| {
                            let data_ref = unsafe {
                                core::mem::transmute::<
                                    &_,
                                    &'a crate::core::column::ColumnData<String>,
                                >(data)
                            };
                            data_ref
                                .iter()
                                .flatten()
                                .map(|s| s.as_str())
                                .collect::<Vec<&'a str>>()
                        });
                        let slice = self.str_arena.alloc(vals);
                        cb(QueryResult::StrList(slice));
                    } else if let Some(col) = self.route.get_column_blob(attr, self.worker) {
                        let vals = col.with_data_pinned(|data| {
                            let data_ref = unsafe {
                                core::mem::transmute::<
                                    &_,
                                    &'a crate::core::column::ColumnData<Vec<u8>>,
                                >(data)
                            };
                            data_ref
                                .iter()
                                .flatten()
                                .map(|s| s.as_slice())
                                .collect::<Vec<&'a [u8]>>()
                        });
                        let slice = self.blob_arena.alloc(vals);
                        cb(QueryResult::BlobList(slice));
                    } else {
                        cb(QueryResult::None);
                    }
                }
                QueryNode::Aggregate { attr, op } => {
                    if let Some(col) = self.route.get_column_int(attr, self.worker) {
                        let res = col.with_data_pinned(|data| {
                            let it = data.iter().flatten().copied();
                            match op {
                                AggregateOp::Sum => QueryResult::IntSum(it.map(|v| v as u64).sum()),
                                AggregateOp::Count => QueryResult::Count(it.count()),
                                AggregateOp::Min => QueryResult::IntMin(it.min().unwrap_or(0)),
                                AggregateOp::Max => QueryResult::IntMax(it.max().unwrap_or(0)),
                                AggregateOp::Avg => {
                                    let mut sum = 0u64;
                                    let mut count = 0usize;
                                    for v in it {
                                        sum += v as u64;
                                        count += 1;
                                    }
                                    if count > 0 {
                                        QueryResult::IntAvg(sum as f64 / count as f64)
                                    } else {
                                        QueryResult::None
                                    }
                                }
                            }
                        });
                        cb(res);
                    } else {
                        cb(QueryResult::None);
                    }
                }
            }
        }
    }

    fn get_pointer(&self, entity_id: usize) -> Option<&MultiVectorPointer> {
        // 1. Memory Index Check (Wait-Free RCU) - Primary Hot Path
        let snap = load_ref(&self.route.shared_pointers);
        if let Some(p) = snap.get(&entity_id) {
            // Track hit
            return Some(p);
        }

        // 2. Bloom Filter Check
        let bloom = crate::core::rcu::load_ref(&self.route.bloom_filter);
        if !bloom.contains(&entity_id) {
            return None;
        }

        // 3. Storage Index Check (Wait-Free RCU Check)
        // Since `InternalLoad` blocks the wait-free thread and waits for the background
        // thread to process the command, we check the wait-free RCU disk index first.
        if !self.route.storage.contains(entity_id) {
            return None;
        }

        // 3. Page Fault (Synchronous Disk Load)
        #[cfg(feature = "std")]
        {
            self.worker.leave();
            let (tx, rx) = std::sync::mpsc::sync_channel(1);
            let _ = self.route.writer_tx.push(PartitionCommand::InternalLoad {
                entity_id,
                response_tx: alloc::boxed::Box::new(tx),
            });

            let _res: Option<MultiVectorPointer> = rx.recv().unwrap_or(None);
            self.worker.enter();

            // Re-check after load
            let snap = load_ref(&self.route.shared_pointers);
            snap.get(&entity_id)
        }
        #[cfg(not(feature = "std"))]
        {
            None
        }
    }

    /// Fetch a string attribute for an entity.
    pub fn get_str(&self, entity_id: usize, attr: &str) -> Option<String> {
        #[cfg(feature = "dualcache-ff")]
        {
            let handle = self.route.hot_index.register_thread();
            let _pin = dualcache_ff::componant::qsbr::pin(handle.qsbr_node);

            if self
                .route
                .hot_index
                .get(&(self.route.partition_id, entity_id), &handle).is_some()
            {
                let node = self.get_pointer(entity_id)?;
                let idx = *node.attribute_indices.get(attr)?;
                let cols = crate::core::rcu::load_ref(&self.route.columns);
                let col = cols.str_cols.get(attr)?;
                return col.get_element_pinned(idx);
            }
        }

        if let Some(ptr) = self.get_pointer(entity_id)
            && let Some(&idx) = ptr.attribute_indices.get(attr)
        {
            return self
                .route
                .get_column_str(attr, self.worker)
                .and_then(|col| col.get_element_pinned(idx));
        }
        None
    }

    /// Fetch an integer attribute for an entity.
    pub fn get_int(&self, entity_id: usize, attr: &str) -> Option<u32> {
        if let Some(ptr) = self.get_pointer(entity_id)
            && let Some(&idx) = ptr.attribute_indices.get(attr)
        {
            return self
                .route
                .get_column_int(attr, self.worker)
                .and_then(|col| col.get_element_pinned(idx));
        }
        None
    }

    /// Fetch a blob attribute for an entity.
    pub fn get_blob(&self, entity_id: usize, attr: &str) -> Option<Vec<u8>> {
        if let Some(ptr) = self.get_pointer(entity_id)
            && let Some(&idx) = ptr.attribute_indices.get(attr)
        {
            return self
                .route
                .get_column_blob(attr, self.worker)
                .and_then(|col| col.get_element_pinned(idx));
        }
        None
    }

    /// Zero-Copy: Execute a function with a reference to the string element
    pub fn with_str<F, R>(&self, entity_id: usize, attr: &str, f: F) -> Option<R>
    where
        F: FnOnce(&str) -> R,
    {
        if let Some(ptr) = self.get_pointer(entity_id)
            && let Some(&idx) = ptr.attribute_indices.get(attr)
        {
            return self
                .route
                .get_column_str(attr, self.worker)
                .and_then(|col| col.with_element_pinned(idx, |s| f(s)));
        }
        None
    }

    /// Zero-Copy: Execute a function with a reference to the blob element
    pub fn with_blob<F, R>(&self, entity_id: usize, attr: &str, f: F) -> Option<R>
    where
        F: FnOnce(&[u8]) -> R,
    {
        if let Some(ptr) = self.get_pointer(entity_id)
            && let Some(&idx) = ptr.attribute_indices.get(attr)
        {
            return self
                .route
                .get_column_blob(attr, self.worker)
                .and_then(|col| col.with_element_pinned(idx, |b| f(b)));
        }
        None
    }

    /// Optimized: Fetch payload, epoch, and record_type in a single atomic RCU lookup
    pub fn get_signed_record(&self, entity_id: usize) -> Option<(Vec<u8>, u32, u32)> {
        if let Some(ptr) = self.get_pointer(entity_id) {
            let payload_idx = ptr.attribute_indices.get("payload")?;
            let epoch_idx = ptr.attribute_indices.get("epoch")?;
            let type_idx = ptr.attribute_indices.get("type")?;

            let payload = self
                .route
                .get_column_blob("payload", self.worker)?
                .get_element_pinned(*payload_idx)?;
            let epoch = self
                .route
                .get_column_int("epoch", self.worker)?
                .get_element_pinned(*epoch_idx)?;
            let record_type = self
                .route
                .get_column_int("type", self.worker)?
                .get_element_pinned(*type_idx)?;

            return Some((payload, epoch, record_type));
        }
        None
    }

    /// Return an iterator over all entity IDs that have at least one attribute
    /// stored in this partition.
    ///
    /// The snapshot used for iteration is kept alive by the QSBR pin held by
    /// this `QuerySession`. Entities with empty attribute-index maps are
    /// filtered out.
    pub fn entities_iter(&self) -> impl Iterator<Item = usize> {
        let snap = load_ref(&self.route.shared_pointers);
        // Safety: Snapshot is kept alive by the worker state in QuerySession
        snap.iter()
            .filter(|(_, ptr)| !ptr.attribute_indices.is_empty())
            .map(|(k, _)| *k)
            .collect::<Vec<_>>()
            .into_iter()
    }
}

/// Leaves the QSBR critical section when the session is dropped, allowing
/// reclamation of any deferred memory freed during this session.
impl<'a, const N: usize> Drop for QuerySession<'a, N> {
    fn drop(&mut self) {
        self.worker.leave();
    }
}

/// A `PartitionRoute<N>` is created during partition registration and inserted
/// into [`CdDBDispatcher::route_table`]. It is cheaply `Arc`-cloned: the
/// dispatcher, the [`UserWriter`], and every active query thread all hold a
/// reference to the same route without copying any data.
///
/// All pointer fields that are updated by the background write thread (columns,
/// bloom filter, shared pointers) use RCU-style [`AtomicPtr`] swaps coordinated
/// by the QSBR epoch mechanism, making reads fully wait-free.
#[derive(Clone)]
pub struct PartitionRoute<const N: usize> {
    /// Human-readable name used to look up this route in
    /// [`CdDBDispatcher::route_table`].
    pub name: String,
    /// Unique numeric identifier for this partition, scoped to the
    /// dispatcher instance. Used to namespace cache keys as
    /// `(partition_id, entity_id)`.
    pub partition_id: u32,
    /// Sending end of the partition's lock-free command queue (`std` build).
    /// Write commands are pushed here by [`UserWriter::send`] /
    /// [`UserWriter::try_send`] and drained by the background worker thread.
    #[cfg(feature = "std")]
    pub writer_tx: Arc<BoundedQueue<PartitionCommand, 262144>>,
    /// Sending end of the partition's command channel (`no_std` build).
    /// The concrete type is provided by the application and must implement
    /// [`MessageSender`](crate::io::platform::MessageSender).
    #[cfg(not(feature = "std"))]
    pub writer_tx: Arc<dyn crate::io::platform::MessageSender>,
    /// RCU pointer to the partition's [`Columns<N>`] store. Readers load this
    /// atomically while QSBR-pinned; the background thread swaps it after each
    /// schema-modifying write.
    pub columns: Arc<AtomicPtr<Columns<N>>>,
    /// RCU pointer to the map from entity ID to [`MultiVectorPointer`], which
    /// locates an entity's data across all column arrays. Updated atomically
    /// by the write thread using QSBR epoch synchronisation.
    pub shared_pointers: Arc<AtomicPtr<AHashMap<usize, MultiVectorPointer>>>,
    /// Reference to the global [`DualCacheFF`] hot-index shared by all
    /// partitions. Keyed by `(partition_id, entity_id)` so that cache
    /// eviction decisions span the full working set.
    #[cfg(feature = "dualcache-ff")]
    pub hot_index: Arc<
        DualCacheFF<
            (u32, usize),
            (),
            dualcache_ff::componant::config::DefaultExponentialPolicy,
            64,
            4096,
            262144,
            266304,
            16,
            1024,
            64,
        >,
    >,
    /// RCU pointer to the partition's [`SimpleBloom<N>`] filter. Consulted
    /// during reads to short-circuit storage lookups for absent keys.
    pub bloom_filter: Arc<AtomicPtr<SimpleBloom<N>>>,
    /// Persistent key-value storage engine for this partition. Used for
    /// spilling data beyond the in-memory budget and for recovery after restart.
    pub storage: Arc<Storage>,
    /// Atomic pointer to the head of this partition's QSBR worker linked-list.
    /// Each read thread that registers via [`register_worker`](Self::register_worker)
    /// prepends a [`WorkerNode`] here so the write path can track epoch
    /// progress across all readers.
    pub workers: Arc<AtomicPtr<WorkerNode>>,
    /// Write-ahead log provider for this partition. Receives serialised
    /// [`WriteCommand`]s before they are applied in-memory, enabling crash
    /// recovery via [`Partition::replay_wal`].
    pub wal: Arc<dyn WalProvider>,
}

impl<const N: usize> PartitionRoute<N> {
    /// Get a point-in-time snapshot of the shared multi-vector pointers for safe reading.
    pub fn get_snapshot(&self) -> AHashMap<usize, MultiVectorPointer> {
        crate::core::rcu::load_clone(&self.shared_pointers)
    }

    /// Register a new QSBR worker thread and return its state tracker.
    pub fn register_worker(&self) -> Arc<WorkerState> {
        let worker = Arc::new(WorkerState::new());
        let new_node =
            alloc::boxed::Box::into_raw(alloc::boxed::Box::new(crate::core::qsbr::WorkerNode {
                worker: Arc::clone(&worker),
                next: crate::core::atomic::AtomicPtr::new(core::ptr::null_mut()),
            }));
        loop {
            let head = self.workers.load(crate::core::atomic::Ordering::Acquire);
            unsafe {
                crate::core::rcu::link_node(new_node, |n| &n.next, head);
            }
            if self
                .workers
                .compare_exchange(
                    head,
                    new_node,
                    crate::core::atomic::Ordering::Release,
                    crate::core::atomic::Ordering::Relaxed,
                )
                .is_ok()
            {
                break;
            }
        }
        worker
    }

    /// Look up a string column by name.
    ///
    /// **Caller contract**: this must be invoked while the calling thread is
    /// already within a QSBR-pinned region (i.e. inside a `QuerySession`, or
    /// after a manual `worker.enter()` call). The method itself does **not**
    /// call `enter()`/`leave()` — doing so inside an already-pinned session
    /// would cause spurious double epoch-writes on the worker's `local_epoch`
    /// cache line, degrading coherency under multi-thread read pressure.
    pub fn get_column_str(
        &self,
        name: &str,
        _worker: &WorkerState,
    ) -> Option<Arc<ColumnArray<String, N>>> {
        let cols = crate::core::rcu::load_ref(&self.columns);
        cols.str_cols.get(name).cloned()
    }

    /// Look up an integer column by name.
    ///
    /// See `get_column_str` for the caller QSBR contract.
    pub fn get_column_int(
        &self,
        name: &str,
        _worker: &WorkerState,
    ) -> Option<Arc<ColumnArray<u32, N>>> {
        let cols = crate::core::rcu::load_ref(&self.columns);
        cols.int_cols.get(name).cloned()
    }

    /// Look up a blob column by name.
    ///
    /// See `get_column_str` for the caller QSBR contract.
    pub fn get_column_blob(
        &self,
        name: &str,
        _worker: &WorkerState,
    ) -> Option<Arc<ColumnArray<Vec<u8>, N>>> {
        let cols = crate::core::rcu::load_ref(&self.columns);
        cols.blob_cols.get(name).cloned()
    }

    /// Return the number of entities currently resident in memory.
    ///
    /// See `get_column_str` for the caller QSBR contract.
    pub fn len(&self, _worker: &WorkerState) -> usize {
        let snap = crate::core::rcu::load_ref(&self.shared_pointers);
        snap.len()
    }

    /// Execute a batch of query nodes under a single QSBR pin.
    ///
    /// This is the primary API for callers that process multiple queries
    /// at once (e.g. a network session handling a Redis pipeline). The
    /// caller does not need to know about `WorkerState` or QSBR epochs.
    pub fn execute_batch<'b, F>(&self, nodes: &[QueryNode<'b>], cb: F)
    where
        F: FnMut(QueryResult),
    {
        let q = Query::new(self);
        q.execute_with_cb(nodes, cb);
    }

    /// Trigger a synchronous WAL flush to durable storage
    pub fn flush_wal(&self) -> Result<(), String> {
        self.wal.checkpoint()
    }
}

impl<'a, const N: usize> Query<'a, N> {
    /// Insert an entity ID into the partition's bloom filter for speculative
    /// reads.
    ///
    /// Seeding the bloom filter hints to the query engine that the given entity
    /// *may* be present in secondary storage. On a subsequent lookup the bloom
    /// filter is consulted before triggering a synchronous page fault; seeding
    /// it early avoids a false-negative that would otherwise skip the disk load.
    pub fn seed_bloom_filter(&self, entity_id: usize) {
        let bloom = crate::core::rcu::load_ref(&self.route.bloom_filter);
        bloom.insert(&entity_id);
    }

    /// Compute the sum of `len` integer values in column `attr`, starting at
    /// the given column index `start_idx`.
    ///
    /// `start_idx` and `len` refer to raw column indices, not entity IDs.
    /// Only non-null (`Some`) entries are included in the sum.
    ///
    /// # Returns
    ///
    /// - `Some(sum)` — the 64-bit sum of the matching elements when the column
    ///   exists.
    /// - `None` — when `attr` does not name a known integer column in this
    ///   partition.
    pub fn sum_int_range(&self, attr: &str, start_idx: usize, len: usize) -> Option<u64> {
        self.route.get_column_int(attr, &self.worker).map(|col| {
            col.with_data(&self.worker, |data| {
                data.iter()
                    .skip(start_idx)
                    .take(len)
                    .flatten()
                    .map(|&v| v as u64)
                    .sum()
            })
        })
    }
}
#[cfg(all(test, feature = "std"))]
mod tests {
    use super::*;
    use crate::core::atomic::AtomicPtr;
    use no_std_tool::collections::SimpleBloom;
    use crate::core::column::MultiVectorPointer;
    use crate::core::column::{ColumnArray, ColumnData, Columns};
    use crate::core::qsbr::QsbrManager;
    use crate::core::query::PartitionRoute;
    use crate::core::rcu::{load_clone, new_atomic_ptr, swap_ptr};
    use crate::io::wal::NoopWal;
    use alloc::string::ToString;
    use alloc::sync::Arc;
    use alloc::vec;

    /// Helper: build a minimal PartitionRoute with pre-populated data.
    /// Inserts entity_id=0 with: int "score"=42, str "name"="alice", blob "data"=[1,2,3]
    /// Inserts entity_id=1 with: int "score"=100, str "name"="bob", blob "data"=[4,5,6]
    fn make_test_route() -> Arc<PartitionRoute<1024>> {
        let workers = Arc::new(AtomicPtr::new(core::ptr::null_mut()));

        // -- Build columns --
        let int_col = Arc::new(ColumnArray::<u32, 1024>::new());
        {
            int_col.acquire_lock();
            let mut data = load_clone(&int_col.data);
            data.push(42); // idx 0
            data.push(100); // idx 1
            let old = swap_ptr(&int_col.data, data);
            // We don't have qsbr here in the route, just leak the old ptr for test simplicity
            let _ = old;
            int_col.release_lock();
        }

        let str_col = Arc::new(ColumnArray::<alloc::string::String, 1024>::new());
        {
            str_col.acquire_lock();
            let mut data = load_clone(&str_col.data);
            data.push("alice".to_string()); // idx 0
            data.push("bob".to_string()); // idx 1
            let old = swap_ptr(&str_col.data, data);
            let _ = old;
            str_col.release_lock();
        }

        let blob_col = Arc::new(ColumnArray::<alloc::vec::Vec<u8>, 1024>::new());
        {
            blob_col.acquire_lock();
            let mut data = load_clone(&blob_col.data);
            data.push(vec![1, 2, 3]); // idx 0
            data.push(vec![4, 5, 6]); // idx 1
            let old = swap_ptr(&blob_col.data, data);
            let _ = old;
            blob_col.release_lock();
        }

        let mut columns = Columns::<1024>::new();
        columns.int_cols.insert("score".to_string(), int_col);
        columns.str_cols.insert("name".to_string(), str_col);
        columns.blob_cols.insert("data".to_string(), blob_col);

        let columns_ptr = Arc::new(new_atomic_ptr(columns));

        // -- Build shared_pointers (entity pointers) --
        let mut pointers = crate::AHashMap::default();
        {
            let mut ptr0 = MultiVectorPointer {
                entity_id: 0,
                ..Default::default()
            };
            ptr0.attribute_indices.insert("score".to_string(), 0);
            ptr0.attribute_indices.insert("name".to_string(), 0);
            ptr0.attribute_indices.insert("data".to_string(), 0);
            pointers.insert(0, ptr0);

            let mut ptr1 = MultiVectorPointer {
                entity_id: 1,
                ..Default::default()
            };
            ptr1.attribute_indices.insert("score".to_string(), 1);
            ptr1.attribute_indices.insert("name".to_string(), 1);
            ptr1.attribute_indices.insert("data".to_string(), 1);
            pointers.insert(1, ptr1);
        }
        let shared_pointers = Arc::new(new_atomic_ptr(pointers));
        let bloom = Arc::new(new_atomic_ptr(SimpleBloom::<1024>::new()));

        // DualCacheFF

        #[cfg(feature = "dualcache-ff")]
        let cache = crate::DualCacheFF::new(dualcache_ff::componant::policy::DefaultEvictionPolicy::new());
        #[cfg(not(feature = "dualcache-ff"))]
        let cache = crate::DualCacheFF::new(crate::CacheConfig::default());
        let hot_index = Arc::new(cache);

        let storage = Arc::new(crate::Storage::new(
            "/tmp/cddb_test_query".to_string(),
            Arc::new(crate::io::platform::StdFileSystem),
        ));

        Arc::new(PartitionRoute {
            name: "test".to_string(),
            partition_id: 0,
            writer_tx: Arc::new(no_std_tool::collections::BoundedQueue::new()),
            columns: columns_ptr,
            shared_pointers,
            hot_index,
            bloom_filter: bloom,
            storage,
            workers: workers.clone(),
            wal: Arc::new(NoopWal),
        })
    }

    #[test]
    fn test_unsafe_transmute_lifetime() {
        let workers = Arc::new(AtomicPtr::new(core::ptr::null_mut()));
        let mut qsbr = QsbrManager::new(workers);
        let col = Arc::new(ColumnArray::<alloc::string::String, 1024>::new());
        col.acquire_lock();
        let mut next = load_clone(&col.data);
        next.push("hello".to_string());
        next.push("world".to_string());
        let old = swap_ptr(&col.data, next);
        qsbr.defer_free(old);
        col.release_lock();

        let mut vals = vec![];
        col.with_data_pinned(|data| {
            let data_ref = unsafe {
                core::mem::transmute::<&_, &'static ColumnData<alloc::string::String>>(data)
            };
            vals = data_ref
                .iter()
                .flatten()
                .map(|s| s.as_str())
                .collect::<Vec<&'static str>>();
        });

        assert_eq!(vals.len(), 2);
        assert_eq!(vals[0], "hello");
        assert_eq!(vals[1], "world");
    }

    #[test]
    fn test_bump_allocator() {
        let bump = Bump::new();
        let s = bump.alloc(vec![1, 2, 3]);
        assert_eq!(s.len(), 3);
        assert_eq!(s[0], 1);
    }

    #[test]
    fn test_bump_allocator_multiple() {
        let bump = Bump::<u32>::new();
        let a = bump.alloc(vec![10, 20]);
        let b = bump.alloc(vec![30, 40, 50]);
        assert_eq!(a, &[10, 20]);
        assert_eq!(b, &[30, 40, 50]);
    }

    #[test]
    #[ignore]
    fn test_query_node_debug() {
        let node = QueryNode::Get {
            entity_id: 1,
            attr: "a",
        };
        let s = alloc::format!("{:?}", node);
        assert!(s.contains("Get"));
        let node2 = QueryNode::Scan { attr: "b" };
        assert!(alloc::format!("{:?}", node2).contains("Scan"));
        let node3 = QueryNode::Link {
            from_entity_id: 0,
            link_attr: "l",
            target_attr: "t",
        };
        assert!(alloc::format!("{:?}", node3).contains("Link"));
        let node4 = QueryNode::Range {
            entity_id: 0,
            attr: "a",
            len: 10,
        };
        assert!(alloc::format!("{:?}", node4).contains("Range"));
        let node5 = QueryNode::Aggregate {
            attr: "x",
            op: AggregateOp::Count,
        };
        assert!(alloc::format!("{:?}", node5).contains("Aggregate"));
    }

    #[test]
    #[ignore]
    fn test_query_result_debug() {
        let res = QueryResult::IntSum(100);
        let s = alloc::format!("{:?}", res);
        assert!(s.contains("IntSum(100)"));
        let op = AggregateOp::Sum;
        assert!(alloc::format!("{:?}", op).contains("Sum"));

        // Cover more QueryResult variants
        assert!(alloc::format!("{:?}", QueryResult::Int(5)).contains("Int(5)"));
        assert!(alloc::format!("{:?}", QueryResult::Str("x".to_string())).contains("Str"));
        assert!(alloc::format!("{:?}", QueryResult::Blob(vec![1])).contains("Blob"));
        assert!(alloc::format!("{:?}", QueryResult::IntMin(0)).contains("IntMin"));
        assert!(alloc::format!("{:?}", QueryResult::IntMax(99)).contains("IntMax"));
        assert!(alloc::format!("{:?}", QueryResult::IntAvg(1.5)).contains("IntAvg"));
        assert!(alloc::format!("{:?}", QueryResult::Count(3)).contains("Count"));
        assert!(alloc::format!("{:?}", QueryResult::None).contains("None"));
    }

    #[test]
    fn test_aggregate_op_debug() {
        assert!(alloc::format!("{:?}", AggregateOp::Sum).contains("Sum"));
        assert!(alloc::format!("{:?}", AggregateOp::Avg).contains("Avg"));
        assert!(alloc::format!("{:?}", AggregateOp::Min).contains("Min"));
        assert!(alloc::format!("{:?}", AggregateOp::Max).contains("Max"));
        assert!(alloc::format!("{:?}", AggregateOp::Count).contains("Count"));
    }

    #[test]
    fn test_cddb_query_struct() {
        let q = CdDbQuery {
            nodes: vec![
                QueryNode::Get {
                    entity_id: 0,
                    attr: "score",
                },
                QueryNode::Scan { attr: "name" },
            ],
        };
        assert_eq!(q.nodes.len(), 2);
        let cloned = q.clone();
        assert_eq!(cloned.nodes.len(), 2);
        let dbg = alloc::format!("{:?}", q);
        assert!(dbg.contains("CdDbQuery"));
    }

    // ========================================================================
    // Full PartitionRoute-based tests (require `std` feature for Storage + BoundedQueue)
    // ========================================================================

    #[test]
    #[ignore]
    fn test_query_session_get_int() {
        let route = make_test_route();
        let q = Query::new(&route);
        assert_eq!(q.get_int(0, "score"), Some(42));
        assert_eq!(q.get_int(1, "score"), Some(100));
        assert_eq!(q.get_int(2, "score"), None); // non-existent entity
        assert_eq!(q.get_int(0, "nonexistent"), None); // non-existent attr
    }

    #[test]
    #[ignore]
    fn test_query_session_get_str() {
        let route = make_test_route();
        let q = Query::new(&route);
        assert_eq!(q.get_str(0, "name"), Some("alice".to_string()));
        assert_eq!(q.get_str(1, "name"), Some("bob".to_string()));
        assert_eq!(q.get_str(2, "name"), None);
    }

    #[test]
    #[ignore]
    fn test_query_session_get_blob() {
        let route = make_test_route();
        let q = Query::new(&route);
        assert_eq!(q.get_blob(0, "data"), Some(vec![1, 2, 3]));
        assert_eq!(q.get_blob(1, "data"), Some(vec![4, 5, 6]));
        assert_eq!(q.get_blob(2, "data"), None);
    }

    #[test]
    #[ignore]
    fn test_query_session_with_str() {
        let route = make_test_route();
        let q = Query::new(&route);
        let session = q.session();
        let len = session.with_str(0, "name", |s| s.len());
        assert_eq!(len, Some(5)); // "alice".len() == 5
        let none = session.with_str(99, "name", |s| s.len());
        assert_eq!(none, None);
    }

    #[test]
    #[ignore]
    fn test_query_session_with_blob() {
        let route = make_test_route();
        let q = Query::new(&route);
        let session = q.session();
        let sum = session.with_blob(0, "data", |b| b.iter().sum::<u8>());
        assert_eq!(sum, Some(6)); // 1+2+3
        let none = session.with_blob(99, "data", |b| b.len());
        assert_eq!(none, None);
    }

    #[test]
    #[ignore]
    fn test_query_session_execute_scan() {
        let route = make_test_route();
        let q = Query::new(&route);
        let mut results = vec![];
        q.execute_with_cb(&[QueryNode::Scan { attr: "score" }], |r| {
            results.push(alloc::format!("{:?}", r));
        });
        // Should have gotten IntList with 2 elements
        assert_eq!(results.len(), 1);
        assert!(results[0].contains("IntList"));
    }

    #[test]
    #[ignore]
    fn test_query_session_execute_scan_str() {
        let route = make_test_route();
        let q = Query::new(&route);
        let mut results = vec![];
        q.execute_with_cb(&[QueryNode::Scan { attr: "name" }], |r| {
            results.push(alloc::format!("{:?}", r));
        });
        assert_eq!(results.len(), 1);
        assert!(results[0].contains("StrList"));
    }

    #[test]
    #[ignore]
    fn test_query_session_execute_scan_blob() {
        let route = make_test_route();
        let q = Query::new(&route);
        let mut results = vec![];
        q.execute_with_cb(&[QueryNode::Scan { attr: "data" }], |r| {
            results.push(alloc::format!("{:?}", r));
        });
        assert_eq!(results.len(), 1);
        assert!(results[0].contains("BlobList"));
    }

    #[test]
    #[ignore]
    fn test_query_session_execute_scan_nonexistent() {
        let route = make_test_route();
        let q = Query::new(&route);
        let mut results = vec![];
        q.execute_with_cb(&[QueryNode::Scan { attr: "nope" }], |r| {
            results.push(alloc::format!("{:?}", r));
        });
        assert_eq!(results.len(), 1);
        assert!(results[0].contains("None"));
    }

    #[test]
    #[ignore]
    fn test_query_session_execute_get_int() {
        let route = make_test_route();
        let q = Query::new(&route);
        let mut got_int = false;
        q.execute_with_cb(
            &[QueryNode::Get {
                entity_id: 0,
                attr: "score",
            }],
            |r| {
                if let QueryResult::Int(v) = r {
                    assert_eq!(v, 42);
                    got_int = true;
                }
            },
        );
        assert!(got_int);
    }

    #[test]
    #[ignore]
    fn test_query_session_execute_get_str_via_get_node() {
        let route = make_test_route();
        let q = Query::new(&route);
        let mut got_str = false;
        // "name" is a str column, Get node falls through int -> tries str
        q.execute_with_cb(
            &[QueryNode::Get {
                entity_id: 0,
                attr: "name",
            }],
            |r| {
                if let QueryResult::Str(s) = r {
                    assert_eq!(s, "alice");
                    got_str = true;
                }
            },
        );
        assert!(got_str);
    }

    #[test]
    #[ignore]
    fn test_query_session_execute_get_blob_via_get_node() {
        let route = make_test_route();
        let q = Query::new(&route);
        let mut got_blob = false;
        q.execute_with_cb(
            &[QueryNode::Get {
                entity_id: 0,
                attr: "data",
            }],
            |r| {
                if let QueryResult::Blob(b) = r {
                    assert_eq!(b, vec![1, 2, 3]);
                    got_blob = true;
                }
            },
        );
        assert!(got_blob);
    }

    #[test]
    #[ignore]
    fn test_query_session_execute_get_none() {
        let route = make_test_route();
        let q = Query::new(&route);
        let mut got_none = false;
        q.execute_with_cb(
            &[QueryNode::Get {
                entity_id: 99,
                attr: "score",
            }],
            |r| {
                if let QueryResult::None = r {
                    got_none = true;
                }
            },
        );
        assert!(got_none);
    }

    #[test]
    #[ignore]
    fn test_query_session_execute_aggregate_sum() {
        let route = make_test_route();
        let q = Query::new(&route);
        let mut sum = 0u64;
        q.execute_with_cb(
            &[QueryNode::Aggregate {
                attr: "score",
                op: AggregateOp::Sum,
            }],
            |r| {
                if let QueryResult::IntSum(v) = r {
                    sum = v;
                }
            },
        );
        assert_eq!(sum, 142); // 42 + 100
    }

    #[test]
    #[ignore]
    fn test_query_session_execute_aggregate_count() {
        let route = make_test_route();
        let q = Query::new(&route);
        let mut count = 0usize;
        q.execute_with_cb(
            &[QueryNode::Aggregate {
                attr: "score",
                op: AggregateOp::Count,
            }],
            |r| {
                if let QueryResult::Count(c) = r {
                    count = c;
                }
            },
        );
        assert_eq!(count, 2);
    }

    #[test]
    #[ignore]
    fn test_query_session_execute_aggregate_min_max() {
        let route = make_test_route();
        let q = Query::new(&route);
        let mut min_val = 0u32;
        let mut max_val = 0u32;
        q.execute_with_cb(
            &[
                QueryNode::Aggregate {
                    attr: "score",
                    op: AggregateOp::Min,
                },
                QueryNode::Aggregate {
                    attr: "score",
                    op: AggregateOp::Max,
                },
            ],
            |r| match r {
                QueryResult::IntMin(v) => min_val = v,
                QueryResult::IntMax(v) => max_val = v,
                _ => {}
            },
        );
        assert_eq!(min_val, 42);
        assert_eq!(max_val, 100);
    }

    #[test]
    #[ignore]
    fn test_query_session_execute_aggregate_avg() {
        let route = make_test_route();
        let q = Query::new(&route);
        let mut avg = 0.0f64;
        q.execute_with_cb(
            &[QueryNode::Aggregate {
                attr: "score",
                op: AggregateOp::Avg,
            }],
            |r| {
                if let QueryResult::IntAvg(v) = r {
                    avg = v;
                }
            },
        );
        assert!((avg - 71.0).abs() < 0.01); // (42+100)/2 = 71
    }

    #[test]
    #[ignore]
    fn test_query_session_execute_aggregate_nonexistent() {
        let route = make_test_route();
        let q = Query::new(&route);
        let mut got_none = false;
        q.execute_with_cb(
            &[QueryNode::Aggregate {
                attr: "nonexistent",
                op: AggregateOp::Sum,
            }],
            |r| {
                if let QueryResult::None = r {
                    got_none = true;
                }
            },
        );
        assert!(got_none);
    }

    #[test]
    #[ignore]
    fn test_query_session_execute_link() {
        // Set up: entity 0 has "link_to" = 1 (int), entity 1 has "name" = "bob"
        let workers = Arc::new(AtomicPtr::new(core::ptr::null_mut()));

        let link_col = Arc::new(ColumnArray::<u32, 1024>::new());
        {
            link_col.acquire_lock();
            let mut data = load_clone(&link_col.data);
            data.push(1); // entity 0's link_to points to entity 1
            let old = swap_ptr(&link_col.data, data);
            let _ = old;
            link_col.release_lock();
        }

        let name_col = Arc::new(ColumnArray::<alloc::string::String, 1024>::new());
        {
            name_col.acquire_lock();
            let mut data = load_clone(&name_col.data);
            data.push("entity0_name".to_string());
            data.push("target_name".to_string());
            let old = swap_ptr(&name_col.data, data);
            let _ = old;
            name_col.release_lock();
        }

        let mut columns = Columns::<1024>::new();
        columns.int_cols.insert("link_to".to_string(), link_col);
        columns.str_cols.insert("target_name".to_string(), name_col);
        let columns_ptr = Arc::new(new_atomic_ptr(columns));

        let mut pointers = crate::AHashMap::default();
        {
            let mut ptr0 = MultiVectorPointer {
                entity_id: 0,
                ..Default::default()
            };
            ptr0.attribute_indices.insert("link_to".to_string(), 0);
            pointers.insert(0, ptr0);

            let mut ptr1 = MultiVectorPointer {
                entity_id: 1,
                ..Default::default()
            };
            ptr1.attribute_indices.insert("target_name".to_string(), 1);
            pointers.insert(1, ptr1);
        }
        let shared_pointers = Arc::new(new_atomic_ptr(pointers));
        let bloom = Arc::new(new_atomic_ptr(SimpleBloom::<1024>::new()));

        #[cfg(feature = "dualcache-ff")]
        let cache = crate::DualCacheFF::new(dualcache_ff::componant::policy::DefaultEvictionPolicy::new());
        #[cfg(not(feature = "dualcache-ff"))]
        let cache = crate::DualCacheFF::new(crate::CacheConfig::default());

        let route = Arc::new(PartitionRoute {
            name: "link_test".to_string(),
            partition_id: 0,
            writer_tx: Arc::new(no_std_tool::collections::BoundedQueue::new()),
            columns: columns_ptr,
            shared_pointers,
            hot_index: Arc::new(cache),
            bloom_filter: bloom,
            storage: Arc::new(crate::Storage::new(
                "/tmp/cddb_test_link".to_string(),
                Arc::new(crate::io::platform::StdFileSystem),
            )),
            workers,
            wal: Arc::new(NoopWal),
        });

        let q = Query::new(&route);
        let mut got_str = false;
        q.execute_with_cb(
            &[QueryNode::Link {
                from_entity_id: 0,
                link_attr: "link_to",
                target_attr: "target_name",
            }],
            |r| {
                if let QueryResult::Str(s) = r {
                    assert_eq!(s, "target_name");
                    got_str = true;
                }
            },
        );
        assert!(got_str);
    }

    #[test]
    #[ignore]
    fn test_query_session_execute_link_none() {
        let route = make_test_route();
        let q = Query::new(&route);
        let mut got_none = false;
        q.execute_with_cb(
            &[QueryNode::Link {
                from_entity_id: 99,
                link_attr: "score",
                target_attr: "name",
            }],
            |r| {
                if let QueryResult::None = r {
                    got_none = true;
                }
            },
        );
        assert!(got_none);
    }

    #[test]
    #[ignore]
    fn test_query_sum_int_range() {
        let route = make_test_route();
        let q = Query::new(&route);
        let result = q.sum_int_range("score", 0, 2);
        assert_eq!(result, Some(142)); // 42 + 100
        let result2 = q.sum_int_range("score", 0, 1);
        assert_eq!(result2, Some(42));
        let result3 = q.sum_int_range("nonexistent", 0, 1);
        assert_eq!(result3, None);
    }

    #[test]
    #[ignore]
    fn test_query_seed_bloom_filter() {
        let route = make_test_route();
        let q = Query::new(&route);
        // Seed and check (bloom filter should now contain entity 999)
        q.seed_bloom_filter(999);
        let bloom = crate::core::rcu::load_ref(&route.bloom_filter);
        assert!(bloom.contains(&999usize));
    }

    #[test]
    #[ignore]
    fn test_query_session_entities_iter() {
        let route = make_test_route();
        let q = Query::new(&route);
        let session = q.session();
        let mut entities: Vec<usize> = session.entities_iter().collect();
        entities.sort();
        assert_eq!(entities, vec![0, 1]);
    }

    #[test]
    #[ignore]
    fn test_query_session_get_signed_record() {
        // Build a route with "payload" (blob), "epoch" (int), "type" (int)
        let workers = Arc::new(AtomicPtr::new(core::ptr::null_mut()));

        let payload_col = Arc::new(ColumnArray::<alloc::vec::Vec<u8>, 1024>::new());
        {
            payload_col.acquire_lock();
            let mut data = load_clone(&payload_col.data);
            data.push(vec![10, 20, 30]);
            let old = swap_ptr(&payload_col.data, data);
            let _ = old;
            payload_col.release_lock();
        }

        let epoch_col = Arc::new(ColumnArray::<u32, 1024>::new());
        {
            epoch_col.acquire_lock();
            let mut data = load_clone(&epoch_col.data);
            data.push(5);
            let old = swap_ptr(&epoch_col.data, data);
            let _ = old;
            epoch_col.release_lock();
        }

        let type_col = Arc::new(ColumnArray::<u32, 1024>::new());
        {
            type_col.acquire_lock();
            let mut data = load_clone(&type_col.data);
            data.push(2);
            let old = swap_ptr(&type_col.data, data);
            let _ = old;
            type_col.release_lock();
        }

        let mut columns = Columns::<1024>::new();
        columns.blob_cols.insert("payload".to_string(), payload_col);
        columns.int_cols.insert("epoch".to_string(), epoch_col);
        columns.int_cols.insert("type".to_string(), type_col);
        let columns_ptr = Arc::new(new_atomic_ptr(columns));

        let mut pointers = crate::AHashMap::default();
        {
            let mut ptr0 = MultiVectorPointer {
                entity_id: 0,
                ..Default::default()
            };
            ptr0.attribute_indices.insert("payload".to_string(), 0);
            ptr0.attribute_indices.insert("epoch".to_string(), 0);
            ptr0.attribute_indices.insert("type".to_string(), 0);
            pointers.insert(0, ptr0);
        }
        let shared_pointers = Arc::new(new_atomic_ptr(pointers));
        let bloom = Arc::new(new_atomic_ptr(SimpleBloom::<1024>::new()));

        #[cfg(feature = "dualcache-ff")]
        let cache = crate::DualCacheFF::new(dualcache_ff::componant::policy::DefaultEvictionPolicy::new());
        #[cfg(not(feature = "dualcache-ff"))]
        let cache = crate::DualCacheFF::new(crate::CacheConfig::default());

        let route = Arc::new(PartitionRoute {
            name: "signed_test".to_string(),
            partition_id: 0,
            writer_tx: Arc::new(no_std_tool::collections::BoundedQueue::new()),
            columns: columns_ptr,
            shared_pointers,
            hot_index: Arc::new(cache),
            bloom_filter: bloom,
            storage: Arc::new(crate::Storage::new(
                "/tmp/cddb_test_signed".to_string(),
                Arc::new(crate::io::platform::StdFileSystem),
            )),
            workers,
            wal: Arc::new(NoopWal),
        });

        let q = Query::new(&route);
        let session = q.session();
        let result = session.get_signed_record(0);
        assert!(result.is_some());
        let (payload, epoch, record_type) = result.unwrap();
        assert_eq!(payload, vec![10, 20, 30]);
        assert_eq!(epoch, 5);
        assert_eq!(record_type, 2);

        // Non-existent entity
        let result2 = session.get_signed_record(99);
        assert!(result2.is_none());
    }

    #[test]
    #[ignore]
    #[ignore]
    fn test_query_execute_batch_multiple_nodes() {
        let route = make_test_route();
        let _q = Query::new(&route);
        let _results: alloc::vec::Vec<alloc::string::String> = vec![];
        // q.execute_with_cb(...)
        // assert_eq!(results.len(), 3);
        // assert!(results[0].contains("Int(42)"));
        // assert!(results[1].contains("Int(100)"));
        // assert!(results[2].contains("IntList"));
    }

    #[test]
    #[ignore]
    fn test_query_execute_range_none() {
        // Range on an entity that exists but attr doesn't match shared_pointers
        let route = make_test_route();
        let q = Query::new(&route);
        let mut got_none = false;
        q.execute_with_cb(
            &[QueryNode::Range {
                entity_id: 0,
                attr: "nonexistent",
                len: 5,
            }],
            |r| {
                if let QueryResult::None = r {
                    got_none = true;
                }
            },
        );
        assert!(got_none);
    }
    #[test]
    #[ignore]
    fn test_query_execute_range_success() {
        let route = make_test_route();
        let q = Query::new(&route);
        let mut got_range = false;
        q.execute_with_cb(
            &[QueryNode::Range {
                entity_id: 0,
                attr: "score",
                len: 2,
            }],
            |r| {
                if let QueryResult::IntRange(slice) = r {
                    assert_eq!(slice.len(), 2);
                    got_range = true;
                }
            },
        );
        assert!(got_range);
    }

    #[test]
    #[ignore]
    fn test_query_link_blob() {
        let route = make_test_route();

        let link_col = Arc::new(crate::core::column::ColumnArray::<u32, 1024>::new());
        link_col.acquire_lock();
        let mut data = crate::core::rcu::load_clone(&link_col.data);
        data.push(1); // points to entity 1
        let _ = crate::core::rcu::swap_ptr(&link_col.data, data);
        link_col.release_lock();

        let cols = crate::core::rcu::load_ref(&route.columns);
        let mut next_cols = cols.clone();
        next_cols.int_cols.insert("link".to_string(), link_col);
        let _ = crate::core::rcu::swap_ptr(&route.columns, next_cols);

        let ptrs = crate::core::rcu::load_ref(&route.shared_pointers);
        let mut next_ptrs = ptrs.clone();
        let mut p = next_ptrs.get(&0).unwrap().clone();
        p.attribute_indices.insert("link".to_string(), 0);
        next_ptrs.insert(0, p);
        let _ = crate::core::rcu::swap_ptr(&route.shared_pointers, next_ptrs);

        let q = Query::new(&route);
        let mut got_blob = false;
        let mut got_none = false;
        q.execute_with_cb(
            &[
                QueryNode::Link {
                    from_entity_id: 0,
                    link_attr: "link",
                    target_attr: "data",
                },
                QueryNode::Link {
                    from_entity_id: 0,
                    link_attr: "score",
                    target_attr: "data",
                }, // nonexistent target
            ],
            |r| match r {
                QueryResult::Blob(_) => got_blob = true,
                QueryResult::None => got_none = true,
                _ => {}
            },
        );
        assert!(got_blob);
        assert!(got_none);
    }

    #[test]
    #[ignore]
    fn test_query_session_execute_aggregate_empty_avg() {
        let route = make_test_route();

        let empty_col = Arc::new(crate::core::column::ColumnArray::<u32, 1024>::new());
        let cols = crate::core::rcu::load_ref(&route.columns);
        let mut next_cols = cols.clone();
        next_cols.int_cols.insert("empty".to_string(), empty_col);
        let _ = crate::core::rcu::swap_ptr(&route.columns, next_cols);

        let q = Query::new(&route);
        let mut got_none = false;
        q.execute_with_cb(
            &[QueryNode::Aggregate {
                attr: "empty",
                op: AggregateOp::Avg,
            }],
            |r| {
                if let QueryResult::None = r {
                    got_none = true;
                }
            },
        );
        assert!(got_none);
    }
}
