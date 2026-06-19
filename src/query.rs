use alloc::sync::Arc;
use alloc::vec::Vec;
use alloc::string::String;
use crate::partition::MultiVectorPointer;
use crate::dispatcher::PartitionRoute;
use crate::qsbr::WorkerState;
use crate::unsafe_core::load_ref;
#[cfg(feature = "std")]
use crate::commands::PartitionCommand;

/// 4. 查詢接口 (Query Engine)
/// 實踐 SPEC 中提到的「極速多重指標跳轉」

#[derive(Debug, Clone)]
pub enum QueryNode<'a> {
    /// 精準取值
    Get { entity_id: usize, attr: &'a str },
    /// 跨陣列跳轉 (目前限於同分區內)
    Link {
        from_entity_id: usize,
        link_attr: &'a str,
        target_attr: &'a str,
    },
    /// 範圍取值
    Range {
        entity_id: usize, // 起始 ID
        attr: &'a str,
        len: usize,
    },
    /// 全域掃描 (Vectorized Scan)
    Scan { attr: &'a str },
    /// 聚合計算 (Vectorized Aggregate)
    Aggregate {
        attr: &'a str,
        op: AggregateOp,
    },
}

#[derive(Debug, Clone)]
pub enum AggregateOp {
    Sum,
    Avg,
    Min,
    Max,
    Count,
}

#[derive(Debug, Clone)]
pub struct CdDbQuery<'a> {
    pub nodes: Vec<QueryNode<'a>>,
}

#[derive(Debug, Clone)]
pub enum QueryResult<'a> {
    Str(String),
    Int(u32),
    Blob(Vec<u8>),
    IntRange(&'a [u32]),
    IntSum(u64),
    IntAvg(f64),
    IntMin(u32),
    IntMax(u32),
    Count(usize),
    IntList(&'a [u32]),
    StrList(&'a [&'a str]),
    BlobList(&'a [&'a [u8]]),
    None,
}

pub struct Bump<T> {
    chunks: core::cell::RefCell<Vec<Vec<T>>>,
}

impl<T: Clone> Bump<T> {
    pub fn new() -> Self {
        Self { chunks: core::cell::RefCell::new(Vec::new()) }
    }
    
    pub fn alloc(&self, data: Vec<T>) -> &[T] {
        let mut chunks = self.chunks.borrow_mut();
        chunks.push(data);
        let last = chunks.last().unwrap();
        unsafe { core::slice::from_raw_parts(last.as_ptr(), last.len()) }
    }
}

pub struct Query<'a, const N: usize> {
    route: &'a PartitionRoute<N>,
    worker: Arc<WorkerState>,
}

pub struct QuerySession<'a, const N: usize> {
    route: &'a PartitionRoute<N>,
    worker: &'a WorkerState,
    int_arena: Bump<u32>,
    str_arena: Bump<&'a str>,
    blob_arena: Bump<&'a [u8]>,
}

impl<'a, const N: usize> Query<'a, N> {
    pub fn new(route: &'a PartitionRoute<N>) -> Self {
        let worker = route.register_worker();
        Self { route, worker }
    }

    pub fn session(&self) -> QuerySession<'_, N> {
        QuerySession::new(self.route, &self.worker)
    }

    pub fn execute_with_cb<'b, F>(&self, nodes: &[QueryNode<'b>], cb: F)
    where
        F: FnMut(QueryResult<'_>),
    {
        self.session().execute_with_cb(nodes, cb);
    }

    pub fn get_int(&self, entity_id: usize, attr: &str) -> Option<u32> {
        self.session().get_int(entity_id, attr)
    }

    pub fn get_str(&self, entity_id: usize, attr: &str) -> Option<String> {
        self.session().get_str(entity_id, attr)
    }

    pub fn get_blob(&self, entity_id: usize, attr: &str) -> Option<Vec<u8>> {
        self.session().get_blob(entity_id, attr)
    }
}

impl<'a, const N: usize> QuerySession<'a, N> {
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
                            && let Some(col) = self.route.get_column_int(attr, self.worker) {
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
                            let data_ref = unsafe { core::mem::transmute::<&_, &'a crate::column::ColumnData<String>>(data) };
                            data_ref.iter().flatten().map(|s| s.as_str()).collect::<Vec<&'a str>>()
                        });
                        let slice = self.str_arena.alloc(vals);
                        cb(QueryResult::StrList(slice));
                    } else if let Some(col) = self.route.get_column_blob(attr, self.worker) {
                        let vals = col.with_data_pinned(|data| {
                            let data_ref = unsafe { core::mem::transmute::<&_, &'a crate::column::ColumnData<Vec<u8>>>(data) };
                            data_ref.iter().flatten().map(|s| s.as_slice()).collect::<Vec<&'a [u8]>>()
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
            let _ = self.route.hot_index.get(&(self.route.partition_id, entity_id)); // Track hit
            return Some(p);
        }

        // 2. Bloom Filter Check
        let bloom = crate::unsafe_core::load_ref(&self.route.bloom_filter);
        if !bloom.contains(&entity_id) {
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

    pub fn get_str(&self, entity_id: usize, attr: &str) -> Option<String> {
        if let Some(ptr) = self.get_pointer(entity_id)
            && let Some(&idx) = ptr.attribute_indices.get(attr) {
                return self
                    .route
                    .get_column_str(attr, self.worker)
                    .and_then(|col| col.get_element_pinned(idx));
            }
        None
    }

    pub fn get_int(&self, entity_id: usize, attr: &str) -> Option<u32> {
        if let Some(ptr) = self.get_pointer(entity_id)
            && let Some(&idx) = ptr.attribute_indices.get(attr) {
                return self
                    .route
                    .get_column_int(attr, self.worker)
                    .and_then(|col| col.get_element_pinned(idx));
            }
        None
    }

    pub fn get_blob(&self, entity_id: usize, attr: &str) -> Option<Vec<u8>> {
        if let Some(ptr) = self.get_pointer(entity_id)
            && let Some(&idx) = ptr.attribute_indices.get(attr) {
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
            && let Some(&idx) = ptr.attribute_indices.get(attr) {
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
            && let Some(&idx) = ptr.attribute_indices.get(attr) {
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
            
            let payload = self.route.get_column_blob("payload", self.worker)?
                .get_element_pinned(*payload_idx)?;
            let epoch = self.route.get_column_int("epoch", self.worker)?
                .get_element_pinned(*epoch_idx)?;
            let record_type = self.route.get_column_int("type", self.worker)?
                .get_element_pinned(*type_idx)?;
                
            return Some((payload, epoch, record_type));
        }
        None
    }

    /// Item 3: Safe Snapshot Iterator
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

impl<'a, const N: usize> Drop for QuerySession<'a, N> {
    fn drop(&mut self) {
        self.worker.leave();
    }
}

impl<'a, const N: usize> Query<'a, N> {
    pub fn seed_bloom_filter(&self, entity_id: usize) {
        let bloom = crate::unsafe_core::load_ref(&self.route.bloom_filter);
        bloom.insert(&entity_id);
    }

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
#[cfg(all(test, not(feature = "loom")))]
mod tests {
    use super::*;
    use crate::column::{ColumnArray, ColumnData};
    use crate::qsbr::QsbrManager;
    use crate::unsafe_core::{load_clone, swap_ptr};
    use alloc::sync::Arc;
    use alloc::vec;
    use alloc::string::ToString;

    #[test]
    fn test_unsafe_transmute_lifetime() {
        use crate::sync::atomic::AtomicPtr;
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
            let data_ref = unsafe { core::mem::transmute::<&_, &'static ColumnData<alloc::string::String>>(data) };
            vals = data_ref.iter().flatten().map(|s| s.as_str()).collect::<Vec<&'static str>>();
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
    fn test_query_node_debug() {
        let node = QueryNode::Get { entity_id: 1, attr: "a" };
        let s = alloc::format!("{:?}", node);
        assert!(s.contains("Get"));
        let node2 = QueryNode::Scan { attr: "b" };
        assert!(alloc::format!("{:?}", node2).contains("Scan"));
    }

    #[test]
    fn test_query_result_debug() {
        let res = QueryResult::IntSum(100);
        let s = alloc::format!("{:?}", res);
        assert!(s.contains("IntSum(100)"));
        let op = AggregateOp::Sum;
        assert!(alloc::format!("{:?}", op).contains("Sum"));
    }
}
