use alloc::sync::Arc;
use alloc::vec::Vec;
use alloc::string::String;
use crate::partition::MultiVectorPointer;
use crate::dispatcher::PartitionRoute;
use crate::qsbr::WorkerState;
use crate::unsafe_core::load_ref;
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
pub enum QueryResult {
    Str(String),
    Int(u32),
    Blob(Vec<u8>),
    IntRange(Vec<u32>),
    IntSum(u64),
    IntAvg(f64),
    IntMin(u32),
    IntMax(u32),
    Count(usize),
    IntList(Vec<u32>),
    StrList(Vec<String>),
    BlobList(Vec<Vec<u8>>),
    None,
}

pub struct Query<'a> {
    route: &'a PartitionRoute,
    worker: Arc<WorkerState>,
}

pub struct QuerySession<'a> {
    route: &'a PartitionRoute,
    worker: &'a WorkerState,
}

impl<'a> Query<'a> {
    pub fn new(route: &'a PartitionRoute) -> Self {
        let worker = route.register_worker();
        Self { route, worker }
    }

    pub fn session(&self) -> QuerySession<'_> {
        QuerySession::new(self.route, &self.worker)
    }

    pub fn execute<'b>(&self, query: CdDbQuery<'b>) -> Vec<QueryResult> {
        let session = self.session();
        let mut results = Vec::new();
        session.execute_with_cb(&query.nodes, |res| results.push(res));
        results
    }

    pub fn execute_with_cb<'b, F>(&self, nodes: &[QueryNode<'b>], cb: F)
    where
        F: FnMut(QueryResult),
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

impl<'a> QuerySession<'a> {
    pub fn new(route: &'a PartitionRoute, worker: &'a WorkerState) -> Self {
        worker.enter();
        Self { route, worker }
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
                    if let Some(ptr) = self.get_pointer(*entity_id) {
                        if let Some(&start_idx) = ptr.attribute_indices.get(*attr) {
                            if let Some(col) = self.route.get_column_int(attr, &self.worker) {
                                let range_vals = col.with_data(&self.worker, |data| {
                                    data.iter()
                                        .skip(start_idx)
                                        .take(*len)
                                        .flatten()
                                        .cloned()
                                        .collect()
                                });
                                cb(QueryResult::IntRange(range_vals));
                                continue;
                            }
                        }
                    }
                    cb(QueryResult::None);
                }
                QueryNode::Scan { attr } => {
                    if let Some(col) = self.route.get_column_int(attr, &self.worker) {
                        let vals = col.with_data(&self.worker, |data| {
                            data.iter().flatten().cloned().collect()
                        });
                        cb(QueryResult::IntList(vals));
                    } else if let Some(col) = self.route.get_column_str(attr, &self.worker) {
                        let vals = col.with_data(&self.worker, |data| {
                            data.iter().flatten().cloned().collect()
                        });
                        cb(QueryResult::StrList(vals));
                    } else if let Some(col) = self.route.get_column_blob(attr, &self.worker) {
                        let vals = col.with_data(&self.worker, |data| {
                            data.iter().flatten().cloned().collect()
                        });
                        cb(QueryResult::BlobList(vals));
                    } else {
                        cb(QueryResult::None);
                    }
                }
                QueryNode::Aggregate { attr, op } => {
                    if let Some(col) = self.route.get_column_int(attr, &self.worker) {
                        let res = col.with_data(&self.worker, |data| {
                            let it = data.iter().flatten().map(|&v| v);
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
            let _ = self.route.hot_index.get(&entity_id); // Track hit
            return Some(p);
        }

        // 2. Bloom Filter Check (Locks for disk read avoidance)
        {
            #[cfg(feature = "std")]
            let bloom = self.route.bloom_filter.lock().unwrap();
            #[cfg(not(feature = "std"))]
            let bloom = self.route.bloom_filter.lock();
            
            if !bloom.contains(&entity_id) {
                return None;
            }
        }

        // 3. Page Fault (Synchronous Disk Load)
        #[cfg(feature = "std")]
        {
            self.worker.leave();
            let (tx, rx) = std::sync::mpsc::sync_channel(1);
            let _ = self.route.writer_tx.send(PartitionCommand::InternalLoad {
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
        if let Some(ptr) = self.get_pointer(entity_id) {
            if let Some(&idx) = ptr.attribute_indices.get(attr) {
                return self
                    .route
                    .get_column_str(attr, &self.worker)
                    .and_then(|col| col.get_element(idx, &self.worker));
            }
        }
        None
    }

    pub fn get_int(&self, entity_id: usize, attr: &str) -> Option<u32> {
        if let Some(ptr) = self.get_pointer(entity_id) {
            if let Some(&idx) = ptr.attribute_indices.get(attr) {
                return self
                    .route
                    .get_column_int(attr, &self.worker)
                    .and_then(|col| col.get_element(idx, &self.worker));
            }
        }
        None
    }

    pub fn get_blob(&self, entity_id: usize, attr: &str) -> Option<Vec<u8>> {
        if let Some(ptr) = self.get_pointer(entity_id) {
            if let Some(&idx) = ptr.attribute_indices.get(attr) {
                return self
                    .route
                    .get_column_blob(attr, &self.worker)
                    .and_then(|col| col.get_element(idx, &self.worker));
            }
        }
        None
    }

    /// Zero-Copy: Execute a function with a reference to the string element
    pub fn with_str<F, R>(&self, entity_id: usize, attr: &str, f: F) -> Option<R>
    where
        F: FnOnce(&str) -> R,
    {
        if let Some(ptr) = self.get_pointer(entity_id) {
            if let Some(&idx) = ptr.attribute_indices.get(attr) {
                return self
                    .route
                    .get_column_str(attr, &self.worker)
                    .and_then(|col| col.with_element(idx, &self.worker, |s| f(s)));
            }
        }
        None
    }

    /// Zero-Copy: Execute a function with a reference to the blob element
    pub fn with_blob<F, R>(&self, entity_id: usize, attr: &str, f: F) -> Option<R>
    where
        F: FnOnce(&[u8]) -> R,
    {
        if let Some(ptr) = self.get_pointer(entity_id) {
            if let Some(&idx) = ptr.attribute_indices.get(attr) {
                return self
                    .route
                    .get_column_blob(attr, &self.worker)
                    .and_then(|col| col.with_element(idx, &self.worker, |b| f(b)));
            }
        }
        None
    }

    /// Item 3: Safe Snapshot Iterator
    pub fn entities_iter(&self) -> impl Iterator<Item = usize> {
        let snap = load_ref(&self.route.shared_pointers);
        // Safety: Snapshot is kept alive by the worker state in QuerySession
        snap.keys().cloned().collect::<Vec<_>>().into_iter()
    }
}

impl<'a> Drop for QuerySession<'a> {
    fn drop(&mut self) {
        self.worker.leave();
    }
}

impl<'a> Query<'a> {
    pub fn seed_bloom_filter(&self, entity_id: usize) {
        #[cfg(feature = "std")]
        let mut bloom = self.route.bloom_filter.lock().unwrap();
        #[cfg(not(feature = "std"))]
        let mut bloom = self.route.bloom_filter.lock();
        bloom.insert(&entity_id);
    }

    pub fn entities(&self) -> Vec<usize> {
        Vec::new()
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
