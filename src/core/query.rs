
use crate::core::AHashMap;
use crate::core::column::{ColumnArray, Columns, MultiVectorPointer};
#[cfg(feature = "std")]
use crate::core::commands::PartitionCommand;
use crate::core::qsbr::{WorkerNode, WorkerState};
use crate::core::rcu::load_ref;
use crate::io::storage::Storage;
use crate::io::wal::WalProvider;
use alloc::string::String;
use alloc::sync::Arc;
#[cfg(all(feature = "dualcache-ff", feature = "std"))]
use alloc::vec::Vec;
use core::sync::atomic::AtomicPtr;
#[cfg(feature = "std")]
use no_std_tool::collections::BoundedQueue;
use no_std_tool::collections::SimpleBloom;
#[doc = " A single logical operation within a query plan."]
#[doc = ""]
#[doc = " Each variant describes one step of work the query engine should perform."]
#[doc = " Multiple `QueryNode`s are combined into a [`CdDbQuery`] and executed"]
#[doc = " sequentially by [`QuerySession::execute_with_cb`]."]
#[derive(Debug, Clone)]
pub enum QueryNode<'a> {
    #[doc = " Look up a single attribute value for a specific entity."]
    #[doc = ""]
    #[doc = " The engine tries integer, string, and blob columns in order and returns"]
    #[doc = " the first match. Yields [`QueryResult::None`] when the entity or"]
    #[doc = " attribute does not exist."]
    Get {
        #[doc = " The entity ID to look up."]
        entity_id: usize,
        #[doc = " The name of the attribute column to read."]
        attr: &'a str,
    },
    #[doc = " Follow an integer foreign-key attribute on one entity to read an"]
    #[doc = " attribute on the referenced entity (within the same partition)."]
    #[doc = ""]
    #[doc = " The value stored at `link_attr` on `from_entity_id` is used as the"]
    #[doc = " target entity ID. Yields [`QueryResult::None`] when either the source"]
    #[doc = " or the resolved target entity/attribute is missing."]
    Link {
        #[doc = " The entity ID whose `link_attr` holds the foreign key."]
        from_entity_id: usize,
        #[doc = " The attribute on the source entity that stores the target entity ID."]
        link_attr: &'a str,
        #[doc = " The attribute to read on the resolved target entity."]
        target_attr: &'a str,
    },
    #[doc = " Read a contiguous slice of integer values starting at a specific entity."]
    #[doc = ""]
    #[doc = " Yields [`QueryResult::IntRange`] containing up to `len` values, or"]
    #[doc = " [`QueryResult::None`] when the entity or attribute is not found."]
    Range {
        #[doc = " The entity ID that anchors the start of the range."]
        entity_id: usize,
        #[doc = " The name of the integer attribute column to read."]
        attr: &'a str,
        #[doc = " The maximum number of successive elements to return."]
        len: usize,
    },
    #[doc = " Perform a full vectorized scan over an entire attribute column."]
    #[doc = ""]
    #[doc = " Returns all non-null values as [`QueryResult::IntList`],"]
    #[doc = " [`QueryResult::StrList`], or [`QueryResult::BlobList`] depending on"]
    #[doc = " the column type. Yields [`QueryResult::None`] when the column does not"]
    #[doc = " exist."]
    Scan {
        #[doc = " The name of the attribute column to scan."]
        attr: &'a str,
    },
    #[doc = " Apply a vectorized aggregation operation to an integer attribute column."]
    #[doc = ""]
    #[doc = " Yields an appropriate [`QueryResult`] variant (e.g. [`QueryResult::IntSum`])."]
    #[doc = " Returns [`QueryResult::None`] when the column is not found."]
    Aggregate {
        #[doc = " The name of the integer attribute column to aggregate."]
        attr: &'a str,
        #[doc = " The aggregation function to apply."]
        op: AggregateOp,
    },
}
#[doc = " Supported aggregation operations."]
#[derive(Debug, Clone)]
pub enum AggregateOp {
    #[doc = " Calculate the sum."]
    Sum,
    #[doc = " Calculate the average."]
    Avg,
    #[doc = " Find the minimum value."]
    Min,
    #[doc = " Find the maximum value."]
    Max,
    #[doc = " Count the total number of elements."]
    Count,
}
#[doc = " Represents a complete query composed of multiple query nodes."]
#[derive(Debug, Clone)]
#[repr(C, align(64))]
pub struct CdDbQuery<'a> {
    #[doc = " The sequence of operations to perform."]
    pub nodes: Vec<QueryNode<'a>>,
}
#[doc = " The output of a query node operation."]
#[derive(Debug, Clone)]
pub enum QueryResult<'a> {
    #[doc = " String result."]
    Str(String),
    #[doc = " Integer result."]
    Int(u32),
    #[doc = " Blob result."]
    Blob(Vec<u8>),
    #[doc = " Range of integers."]
    IntRange(&'a [u32]),
    #[doc = " Sum of integers."]
    IntSum(u64),
    #[doc = " Average of integers."]
    IntAvg(f64),
    #[doc = " Minimum integer."]
    IntMin(u32),
    #[doc = " Maximum integer."]
    IntMax(u32),
    #[doc = " Count result."]
    Count(usize),
    #[doc = " List of integers."]
    IntList(&'a [u32]),
    #[doc = " List of strings."]
    StrList(&'a [&'a str]),
    #[doc = " List of blobs."]
    BlobList(&'a [&'a [u8]]),
    #[doc = " No result."]
    None,
}
#[doc = " A simple bump (arena) allocator that keeps allocated slices alive for"]
#[doc = " the lifetime of the owning [`QuerySession`]."]
#[doc = ""]
#[doc = " Each call to [`Bump::alloc`] moves a `Vec<T>` into internal storage and"]
#[doc = " returns a raw slice reference into that storage. Because the backing"]
#[doc = " storage never reallocates individual chunks, the returned pointers remain"]
#[doc = " stable until the `Bump` itself is dropped. This allows zero-copy results"]
#[doc = " to be returned from scan operations without per-query heap allocation."]
#[repr(C, align(64))]
pub struct Bump<T> {
    chunks: core::cell::RefCell<Vec<Vec<T>>>,
}
impl<T: Clone> Default for Bump<T> {
    fn default() -> Self {
        Self::new()
    }
}
impl<T: Clone> Bump<T> {
    #[doc = " Create a new, empty `Bump` allocator."]
    pub fn new() -> Self {
        Self {
            chunks: core::cell::RefCell::new(Vec::new()),
        }
    }
    #[doc = " Push `data` into the internal arena and return a stable slice reference."]
    #[doc = ""]
    #[doc = " The returned slice is valid for as long as the `Bump` itself is alive."]
    #[doc = " Ownership of `data` is transferred to the arena; no copy is made of the"]
    #[doc = " element values beyond what already exists in the `Vec`."]
    #[doc = ""]
    #[doc = " # Safety"]
    #[doc = ""]
    #[doc = " This function uses `core::slice::from_raw_parts` to extend the lifetime"]
    #[doc = " of the slice reference. Safety is upheld because the backing `Vec` is"]
    #[doc = " stored inside the `Bump` and never removed or reallocated while any"]
    #[doc = " returned reference may still be in use."]
    pub fn alloc(&self, data: Vec<T>) -> &[T] {
        let mut chunks = self.chunks.borrow_mut();
        chunks.push(data);
        let last = chunks.last().unwrap();
        unsafe { core::slice::from_raw_parts(last.as_ptr(), last.len()) }
    }
}
#[doc = " A query executor bound to a specific partition and QSBR worker thread."]
#[doc = ""]
#[doc = " `Query` owns the [`WorkerState`] registration for its lifetime. All query"]
#[doc = " execution is driven through either a short-lived [`QuerySession`] (obtained"]
#[doc = " via [`Query::session`]) or the convenience helper methods."]
#[repr(C, align(64))]
pub struct Query<'a, const N: usize> {
    route: &'a PartitionRoute<N>,
    worker: Arc<WorkerState>,
    #[cfg(all(feature = "dualcache-ff", feature = "std"))]
    cache_handle: crate::dualcache_ff::component::tls::TlsHandle,
}
#[doc = " An active query session that holds arena allocators and a QSBR pin."]
#[doc = ""]
#[doc = " A single QSBR critical-section pin covers the entire session: the pin is"]
#[doc = " entered when the session is created and released when it is dropped."]
#[doc = " All scan and range results backed by the internal [`Bump`] arenas remain"]
#[doc = " valid until the session is dropped."]
#[doc = ""]
#[doc = " Create a session via [`Query::session`] rather than calling"]
#[doc = " [`QuerySession::new`] directly."]
#[repr(C, align(64))]
pub struct QuerySession<'a, const N: usize> {
    route: &'a PartitionRoute<N>,
    worker: &'a WorkerState,
    int_arena: Bump<u32>,
    str_arena: Bump<&'a str>,
    blob_arena: Bump<&'a [u8]>,
    bypass_l1_cache: bool,
    #[cfg(all(feature = "dualcache-ff", feature = "std"))]
    cache_handle: &'a crate::dualcache_ff::component::tls::TlsHandle,
}
impl<'a, const N: usize> Query<'a, N> {
    #[doc = " Create a new `Query` bound to the given partition route."]
    #[doc = ""]
    #[doc = " Registers a QSBR worker for this query executor. The worker remains"]
    #[doc = " registered for the lifetime of the `Query` instance."]
    #[doc = ""]
    #[doc = " # Examples"]
    #[doc = ""]
    #[doc = " ```rust,ignore"]
    #[doc = " let route: &PartitionRoute<1024> = /* … */;"]
    #[doc = " let query = Query::new(route);"]
    #[doc = " let score = query.get_int(0, \"score\");"]
    #[doc = " ```"]
    pub fn new(route: &'a PartitionRoute<N>) -> Self {
        let worker = route.register_worker();
        Self {
            route,
            worker,
            #[cfg(all(feature = "dualcache-ff", feature = "std"))]
            cache_handle: route.hot_index.register_thread(),
        }
    }
    #[doc = " Create a [`QuerySession`], entering the QSBR critical section."]
    #[doc = ""]
    #[doc = " The session borrows `self` for its duration, ensuring the underlying"]
    #[doc = " worker registration stays valid. The QSBR pin is held until the"]
    #[doc = " returned `QuerySession` is dropped."]
    pub fn session(&self) -> QuerySession<'_, N> {
        #[cfg(all(feature = "dualcache-ff", feature = "std"))]
        return QuerySession::new(self.route, &self.worker, &self.cache_handle);
        #[cfg(not(all(feature = "dualcache-ff", feature = "std")))]
        return QuerySession::new(self.route, &self.worker);
    }
    #[doc = " Execute a batch of query nodes, invoking the callback for each result."]
    #[doc = ""]
    #[doc = " This is a convenience wrapper that creates a temporary [`QuerySession`],"]
    #[doc = " runs all nodes through [`QuerySession::execute_with_cb`], and then"]
    #[doc = " drops the session (releasing the QSBR pin)."]
    pub fn execute_with_cb<'b, F>(&self, nodes: &[QueryNode<'b>], cb: F)
    where
        F: FnMut(QueryResult<'_>),
    {
        self.session().execute_with_cb(nodes, cb);
    }
    #[doc = " Helper: Execute a single [`QueryNode::Get`] for an integer attribute."]
    #[doc = ""]
    #[doc = " Returns `Some(value)` if the entity exists and the attribute is an"]
    #[doc = " integer column, otherwise `None`."]
    pub fn get_int(&self, entity_id: usize, attr: &str) -> Option<u32> {
        self.session().get_int(entity_id, attr)
    }
    #[doc = " Helper: Execute a single [`QueryNode::Get`] for a string attribute."]
    #[doc = ""]
    #[doc = " Returns `Some(value)` if the entity exists and the attribute is a"]
    #[doc = " string column, otherwise `None`."]
    pub fn get_str(&self, entity_id: usize, attr: &str) -> Option<String> {
        self.session().get_str(entity_id, attr)
    }
    #[doc = " Helper: Execute a single [`QueryNode::Get`] for a blob attribute."]
    #[doc = ""]
    #[doc = " Returns `Some(value)` if the entity exists and the attribute is a"]
    #[doc = " blob column, otherwise `None`."]
    pub fn get_blob(&self, entity_id: usize, attr: &str) -> Option<Vec<u8>> {
        self.session().get_blob(entity_id, attr)
    }
}
impl<'a, const N: usize> QuerySession<'a, N> {
    #[doc = " Private constructor — enters the QSBR critical section."]
    #[doc = ""]
    #[doc = " Prefer [`Query::session`] over calling this directly. The QSBR pin"]
    #[doc = " acquired here is released by the [`Drop`] implementation."]
    #[cfg(all(feature = "dualcache-ff", feature = "std"))]
    pub fn new(
        route: &'a PartitionRoute<N>,
        worker: &'a WorkerState,
        cache_handle: &'a crate::dualcache_ff::component::tls::TlsHandle,
    ) -> Self {
        worker.enter();
        Self {
            route,
            worker,
            int_arena: Bump::new(),
            str_arena: Bump::new(),
            blob_arena: Bump::new(),
            bypass_l1_cache: false,
            cache_handle,
        }
    }
    #[cfg(not(all(feature = "dualcache-ff", feature = "std")))]
    pub fn new(route: &'a PartitionRoute<N>, worker: &'a WorkerState) -> Self {
        worker.enter();
        Self {
            route,
            worker,
            int_arena: Bump::new(),
            str_arena: Bump::new(),
            blob_arena: Bump::new(),
            bypass_l1_cache: false,
        }
    }
    #[doc = " Modify this session to bypass L1 cache (DualCacheFF) lookups."]
    pub fn with_bypass_l1_cache(mut self, bypass: bool) -> Self {
        self.bypass_l1_cache = bypass;
        self
    }
    #[doc = " Dispatch a slice of [`QueryNode`]s and call `cb` once for each result."]
    #[doc = ""]
    #[doc = " Nodes are processed sequentially in order. For each node the callback"]
    #[doc = " receives exactly one [`QueryResult`]. The callback may not outlive the"]
    #[doc = " session because scan results are backed by the session's internal arenas."]
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
        let snap = load_ref(&self.route.shared_pointers);
        if let Some(p) = snap.get(&entity_id) {
            return Some(p);
        }
        let bloom = crate::core::rcu::load_ref(&self.route.bloom_filter);
        if !bloom.contains(&entity_id) {
            return None;
        }
        if !self.route.storage.contains(entity_id) {
            return None;
        }
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
            let snap = load_ref(&self.route.shared_pointers);
            snap.get(&entity_id)
        }
        #[cfg(not(feature = "std"))]
        {
            None
        }
    }
    #[doc = " Fetch a string attribute for an entity."]
    pub fn get_str(&self, entity_id: usize, attr: &str) -> Option<String> {
        #[cfg(all(feature = "dualcache-ff", feature = "std"))]
        {
            if !self.bypass_l1_cache {
                let handle = &self.cache_handle;
                #[cfg(all(feature = "dualcache-ff", feature = "std"))]
                if self
                    .route
                    .hot_index
                    .is_hot(self.route.partition_id, entity_id, handle)
                {
                    let node = self.get_pointer(entity_id)?;
                    let idx = *node.attribute_indices.get(attr)?;
                    let cols = crate::core::rcu::load_ref(&self.route.columns);
                    let col = cols.str_cols.get(attr)?;
                    return col.get_element_pinned(idx);
                }
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
    #[doc = " Fetch an integer attribute for an entity."]
    pub fn get_int(&self, entity_id: usize, attr: &str) -> Option<u32> {
        #[cfg(all(feature = "dualcache-ff", feature = "std"))]
        {
            if !self.bypass_l1_cache {
                let handle = &self.cache_handle;
                #[cfg(all(feature = "dualcache-ff", feature = "std"))]
                if self
                    .route
                    .hot_index
                    .is_hot(self.route.partition_id, entity_id, handle)
                {
                    let node = self.get_pointer(entity_id)?;
                    let idx = *node.attribute_indices.get(attr)?;
                    let cols = crate::core::rcu::load_ref(&self.route.columns);
                    let col = cols.int_cols.get(attr)?;
                    return col.get_element_pinned(idx);
                }
            }
        }
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
    #[doc = " Fetch a blob attribute for an entity."]
    pub fn get_blob(&self, entity_id: usize, attr: &str) -> Option<Vec<u8>> {
        #[cfg(all(feature = "dualcache-ff", feature = "std"))]
        {
            if !self.bypass_l1_cache {
                let handle = &self.cache_handle;
                #[cfg(all(feature = "dualcache-ff", feature = "std"))]
                if self
                    .route
                    .hot_index
                    .is_hot(self.route.partition_id, entity_id, handle)
                {
                    let node = self.get_pointer(entity_id)?;
                    let idx = *node.attribute_indices.get(attr)?;
                    let cols = crate::core::rcu::load_ref(&self.route.columns);
                    let col = cols.blob_cols.get(attr)?;
                    return col.get_element_pinned(idx);
                }
            }
        }
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
    #[doc = " Zero-Copy: Execute a function with a reference to the string element"]
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
    #[doc = " Zero-Copy: Execute a function with a reference to the blob element"]
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
    #[doc = " Optimized: Fetch payload, epoch, and record_type in a single atomic RCU lookup"]
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
    #[doc = " Return an iterator over all entity IDs that have at least one attribute"]
    #[doc = " stored in this partition."]
    #[doc = ""]
    #[doc = " The snapshot used for iteration is kept alive by the QSBR pin held by"]
    #[doc = " this `QuerySession`. Entities with empty attribute-index maps are"]
    #[doc = " filtered out."]
    pub fn entities_iter(&self) -> impl Iterator<Item = usize> {
        let snap = load_ref(&self.route.shared_pointers);
        snap.iter()
            .filter(|(_, ptr)| !ptr.attribute_indices.is_empty())
            .map(|(k, _)| *k)
            .collect::<Vec<_>>()
            .into_iter()
    }
}
#[doc = " Leaves the QSBR critical section when the session is dropped, allowing"]
#[doc = " reclamation of any deferred memory freed during this session."]
impl<'a, const N: usize> Drop for QuerySession<'a, N> {
    fn drop(&mut self) {
        self.worker.leave();
    }
}
#[doc = " A `PartitionRoute<N>` is created during partition registration and inserted"]
#[doc = " into [`CdDBDispatcher::route_table`]. It is cheaply `Arc`-cloned: the"]
#[doc = " dispatcher, the [`UserWriter`], and every active query thread all hold a"]
#[doc = " reference to the same route without copying any data."]
#[doc = ""]
#[doc = " All pointer fields that are updated by the background write thread (columns,"]
#[doc = " bloom filter, shared pointers) use RCU-style [`AtomicPtr`] swaps coordinated"]
#[doc = " by the QSBR epoch mechanism, making reads fully wait-free."]
#[derive(Clone)]
#[repr(C, align(64))]
pub struct PartitionRoute<const N: usize> {
    #[doc = " Human-readable name used to look up this route in"]
    #[doc = " [`CdDBDispatcher::route_table`]."]
    pub name: String,
    #[doc = " Unique numeric identifier for this partition, scoped to the"]
    #[doc = " dispatcher instance. Used to namespace cache keys as"]
    #[doc = " `(partition_id, entity_id)`."]
    pub partition_id: u32,
    #[doc = " Sending end of the partition's lock-free command queue (`std` build)."]
    #[doc = " Write commands are pushed here by [`UserWriter::send`] /"]
    #[doc = " [`UserWriter::try_send`] and drained by the background worker thread."]
    #[cfg(feature = "std")]
    pub writer_tx: Arc<BoundedQueue<PartitionCommand, 262144>>,
    #[doc = " Sending end of the partition's command channel (`no_std` build)."]
    #[doc = " The concrete type is provided by the application and must implement"]
    #[doc = " [`MessageSender`](crate::io::platform::MessageSender)."]
    #[cfg(not(feature = "std"))]
    pub writer_tx: Arc<dyn crate::io::platform::MessageSender>,
    #[doc = " RCU pointer to the partition's [`Columns<N>`] store. Readers load this"]
    #[doc = " atomically while QSBR-pinned; the background thread swaps it after each"]
    #[doc = " schema-modifying write."]
    pub columns: Arc<AtomicPtr<Columns<N>>>,
    #[doc = " RCU pointer to the map from entity ID to [`MultiVectorPointer`], which"]
    #[doc = " locates an entity's data across all column arrays. Updated atomically"]
    #[doc = " by the write thread using QSBR epoch synchronisation."]
    pub shared_pointers: Arc<AtomicPtr<AHashMap<usize, MultiVectorPointer>>>,
    #[doc = " Reference to the global [`DualCacheFF`] hot-index shared by all"]
    #[doc = " partitions. Keyed by `(partition_id, entity_id)` so that cache"]
    #[doc = " eviction decisions span the full working set."]
    #[cfg(all(feature = "dualcache-ff", feature = "std"))]
    pub hot_index: Arc<dyn crate::core::hot_index::HotIndexProvider<Handle = crate::dualcache_ff::component::tls::TlsHandle>>,
    #[doc = " RCU pointer to the partition's [`SimpleBloom<N>`] filter. Consulted"]
    #[doc = " during reads to short-circuit storage lookups for absent keys."]
    pub bloom_filter: Arc<AtomicPtr<SimpleBloom<N>>>,
    #[doc = " Persistent key-value storage engine for this partition. Used for"]
    #[doc = " spilling data beyond the in-memory budget and for recovery after restart."]
    pub storage: Arc<Storage>,
    #[doc = " Atomic pointer to the head of this partition's QSBR worker linked-list."]
    #[doc = " Each read thread that registers via [`register_worker`](Self::register_worker)"]
    #[doc = " prepends a [`WorkerNode`] here so the write path can track epoch"]
    #[doc = " progress across all readers."]
    pub workers: Arc<AtomicPtr<WorkerNode>>,
    #[doc = " Write-ahead log provider for this partition. Receives serialised"]
    #[doc = " [`WriteCommand`]s before they are applied in-memory, enabling crash"]
    #[doc = " recovery via [`Partition::replay_wal`]."]
    pub wal: Arc<dyn WalProvider>,
}
impl<const N: usize> PartitionRoute<N> {
    #[doc = " Get a point-in-time snapshot of the shared multi-vector pointers for safe reading."]
    pub fn get_snapshot(&self) -> AHashMap<usize, MultiVectorPointer> {
        crate::core::rcu::load_clone(&self.shared_pointers)
    }
    #[doc = " Register a new QSBR worker thread and return its state tracker."]
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
    #[doc = " Look up a string column by name."]
    #[doc = ""]
    #[doc = " **Caller contract**: this must be invoked while the calling thread is"]
    #[doc = " already within a QSBR-pinned region (i.e. inside a `QuerySession`, or"]
    #[doc = " after a manual `worker.enter()` call). The method itself does **not**"]
    #[doc = " call `enter()`/`leave()` — doing so inside an already-pinned session"]
    #[doc = " would cause spurious double epoch-writes on the worker's `local_epoch`"]
    #[doc = " cache line, degrading coherency under multi-thread read pressure."]
    pub fn get_column_str(
        &self,
        name: &str,
        _worker: &WorkerState,
    ) -> Option<Arc<ColumnArray<String, N>>> {
        let cols = crate::core::rcu::load_ref(&self.columns);
        cols.str_cols.get(name).cloned()
    }
    #[doc = " Look up an integer column by name."]
    #[doc = ""]
    #[doc = " See `get_column_str` for the caller QSBR contract."]
    pub fn get_column_int(
        &self,
        name: &str,
        _worker: &WorkerState,
    ) -> Option<Arc<ColumnArray<u32, N>>> {
        let cols = crate::core::rcu::load_ref(&self.columns);
        cols.int_cols.get(name).cloned()
    }
    #[doc = " Look up a blob column by name."]
    #[doc = ""]
    #[doc = " See `get_column_str` for the caller QSBR contract."]
    pub fn get_column_blob(
        &self,
        name: &str,
        _worker: &WorkerState,
    ) -> Option<Arc<ColumnArray<Vec<u8>, N>>> {
        let cols = crate::core::rcu::load_ref(&self.columns);
        cols.blob_cols.get(name).cloned()
    }
    #[doc = " Return the number of entities currently resident in memory."]
    #[doc = ""]
    #[doc = " See `get_column_str` for the caller QSBR contract."]
    pub fn len(&self, _worker: &WorkerState) -> usize {
        let snap = crate::core::rcu::load_ref(&self.shared_pointers);
        snap.len()
    }
    #[doc = " Execute a batch of query nodes under a single QSBR pin."]
    #[doc = ""]
    #[doc = " This is the primary API for callers that process multiple queries"]
    #[doc = " at once (e.g. a network session handling a Redis pipeline). The"]
    #[doc = " caller does not need to know about `WorkerState` or QSBR epochs."]
    pub fn execute_batch<'b, F>(&self, nodes: &[QueryNode<'b>], cb: F)
    where
        F: FnMut(QueryResult),
    {
        let q = Query::new(self);
        q.execute_with_cb(nodes, cb);
    }
    #[doc = " Trigger a synchronous WAL flush to durable storage"]
    pub fn flush_wal(&self) -> Result<(), String> {
        self.wal.checkpoint()
    }
}
impl<'a, const N: usize> Query<'a, N> {
    #[doc = " Insert an entity ID into the partition's bloom filter for speculative"]
    #[doc = " reads."]
    #[doc = ""]
    #[doc = " Seeding the bloom filter hints to the query engine that the given entity"]
    #[doc = " *may* be present in secondary storage. On a subsequent lookup the bloom"]
    #[doc = " filter is consulted before triggering a synchronous page fault; seeding"]
    #[doc = " it early avoids a false-negative that would otherwise skip the disk load."]
    pub fn seed_bloom_filter(&self, entity_id: usize) {
        let bloom = crate::core::rcu::load_ref(&self.route.bloom_filter);
        bloom.insert(&entity_id);
    }
    #[doc = " Compute the sum of `len` integer values in column `attr`, starting at"]
    #[doc = " the given column index `start_idx`."]
    #[doc = ""]
    #[doc = " `start_idx` and `len` refer to raw column indices, not entity IDs."]
    #[doc = " Only non-null (`Some`) entries are included in the sum."]
    #[doc = ""]
    #[doc = " # Returns"]
    #[doc = ""]
    #[doc = " - `Some(sum)` — the 64-bit sum of the matching elements when the column"]
    #[doc = "   exists."]
    #[doc = " - `None` — when `attr` does not name a known integer column in this"]
    #[doc = "   partition."]
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
    use crate::core::column::MultiVectorPointer;
    use crate::core::column::{ColumnArray, ColumnData, Columns};
    use crate::core::qsbr::QsbrManager;
    use crate::core::query::PartitionRoute;
    use crate::core::rcu::{load_clone, new_atomic_ptr, swap_ptr};
    use crate::io::wal::NoopWal;
    use alloc::string::ToString;
    use alloc::sync::Arc;
    use alloc::vec;
    use no_std_tool::collections::SimpleBloom;
    #[doc = " Helper: build a minimal PartitionRoute with pre-populated data."]
    #[doc = " Inserts entity_id=0 with: int \"score\"=42, str \"name\"=\"alice\", blob \"data\"=[1,2,3]"]
    #[doc = " Inserts entity_id=1 with: int \"score\"=100, str \"name\"=\"bob\", blob \"data\"=[4,5,6]"]
    fn make_test_route() -> Arc<PartitionRoute<1024>> {
        let workers = Arc::new(AtomicPtr::new(core::ptr::null_mut()));
        let int_col = Arc::new(ColumnArray::<u32, 1024>::new());
        {
            int_col.acquire_lock();
            let mut data = load_clone(&int_col.data);
            data.push(42);
            data.push(100);
            let old = swap_ptr(&int_col.data, data);
            let _ = old;
            int_col.release_lock();
        }
        let str_col = Arc::new(ColumnArray::<alloc::string::String, 1024>::new());
        {
            str_col.acquire_lock();
            let mut data = load_clone(&str_col.data);
            data.push("alice".to_string());
            data.push("bob".to_string());
            let old = swap_ptr(&str_col.data, data);
            let _ = old;
            str_col.release_lock();
        }
        let blob_col = Arc::new(ColumnArray::<Vec<u8>, 1024>::new());
        {
            blob_col.acquire_lock();
            let mut data = load_clone(&blob_col.data);
            data.push(vec![1, 2, 3]);
            data.push(vec![4, 5, 6]);
            let old = swap_ptr(&blob_col.data, data);
            let _ = old;
            blob_col.release_lock();
        }
        let mut columns = Columns::<1024>::new();
        columns.int_cols.insert("score".to_string(), int_col);
        columns.str_cols.insert("name".to_string(), str_col);
        columns.blob_cols.insert("data".to_string(), blob_col);
        let columns_ptr = Arc::new(new_atomic_ptr(columns));
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
        let cache: crate::DualCacheFF<(u32, usize), (), 64, 4096, 262144, 266304> = cfg_select! { all (feature = "dualcache-ff" , feature = "std") => crate :: DualCacheFF :: new () , _ => crate :: DualCacheFF :: new (crate :: CacheConfig :: default ()) , };
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
    #[test]
    #[ignore]
    fn test_query_session_get_int() {
        let route = make_test_route();
        let q = Query::new(&route);
        assert_eq!(q.get_int(0, "score"), Some(42));
        assert_eq!(q.get_int(1, "score"), Some(100));
        assert_eq!(q.get_int(2, "score"), None);
        assert_eq!(q.get_int(0, "nonexistent"), None);
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
        assert_eq!(len, Some(5));
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
        assert_eq!(sum, Some(6));
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
        assert_eq!(sum, 142);
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
        assert!((avg - 71.0).abs() < 0.01);
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
        let workers = Arc::new(AtomicPtr::new(core::ptr::null_mut()));
        let link_col = Arc::new(ColumnArray::<u32, 1024>::new());
        {
            link_col.acquire_lock();
            let mut data = load_clone(&link_col.data);
            data.push(1);
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
        let cache: crate::DualCacheFF<(u32, usize), (), 64, 4096, 262144, 266304> = cfg_select! { all (feature = "dualcache-ff" , feature = "std") => crate :: DualCacheFF :: new () , _ => crate :: DualCacheFF :: new (crate :: CacheConfig :: default ()) , };
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
        assert_eq!(result, Some(142));
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
        let workers = Arc::new(AtomicPtr::new(core::ptr::null_mut()));
        let payload_col = Arc::new(ColumnArray::<Vec<u8>, 1024>::new());
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
        let cache: crate::DualCacheFF<(u32, usize), (), 64, 4096, 262144, 266304> = cfg_select! { all (feature = "dualcache-ff" , feature = "std") => crate :: DualCacheFF :: new () , _ => crate :: DualCacheFF :: new (crate :: CacheConfig :: default ()) , };
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
        let result2 = session.get_signed_record(99);
        assert!(result2.is_none());
    }
    #[test]
    #[ignore]
    fn test_query_execute_batch_multiple_nodes() {
        let route = make_test_route();
        let _q = Query::new(&route);
        let _results: Vec<alloc::string::String> = vec![];
    }
    #[test]
    #[ignore]
    fn test_query_execute_range_none() {
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
        data.push(1);
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
                },
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
