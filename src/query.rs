use std::sync::Arc;
use serde::{Deserialize, Serialize};
use crossbeam_channel::bounded;
use crate::partition::MultiVectorPointer;
use crate::dispatcher::PartitionRoute;
use crate::qsbr::WorkerState;
use crate::unsafe_core::load_ref;
use crate::commands::PartitionCommand;

/// 4. 查詢接口 (Query Engine)
/// 實踐 SPEC 中提到的「極速多重指標跳轉」

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum QueryNode {
    /// 精準取值
    Get { entity_id: usize, attr: String },
    /// 跨陣列跳轉 (目前限於同分區內)
    Link {
        from_entity_id: usize,
        link_attr: String,
        target_attr: String,
    },
    /// 範圍取值
    Range {
        entity_id: usize, // 起始 ID
        attr: String,
        len: usize,
    },
    /// 全域掃描 (Vectorized Scan)
    Scan { attr: String },
    /// 聚合計算 (Vectorized Aggregate)
    Aggregate {
        attr: String,
        op: AggregateOp,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum AggregateOp {
    Sum,
    Avg,
    Min,
    Max,
    Count,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CdDbQuery {
    pub nodes: Vec<QueryNode>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum QueryResult {
    Str(String),
    Int(u32),
    IntRange(Vec<u32>),
    IntSum(u64),
    IntAvg(f64),
    IntMin(u32),
    IntMax(u32),
    Count(usize),
    IntList(Vec<u32>),
    StrList(Vec<String>),
    None,
}

pub struct Query<'a> {
    route: &'a PartitionRoute,
    worker: Arc<WorkerState>,
}

impl<'a> Query<'a> {
    pub fn new(route: &'a PartitionRoute) -> Self {
        let worker = route.register_worker();
        Self { route, worker }
    }

    pub fn execute(&self, query: CdDbQuery) -> Vec<QueryResult> {
        let mut results = Vec::new();
        for node in query.nodes {
            match node {
                QueryNode::Get { entity_id, attr } => {
                    if let Some(v) = self.get_int(entity_id, &attr) {
                        results.push(QueryResult::Int(v));
                    } else if let Some(v) = self.get_str(entity_id, &attr) {
                        results.push(QueryResult::Str(v));
                    } else {
                        results.push(QueryResult::None);
                    }
                }
                QueryNode::Link {
                    from_entity_id,
                    link_attr,
                    target_attr,
                } => {
                    if let Some(target_id) = self.get_int(from_entity_id, &link_attr) {
                        let target_id = target_id as usize;
                        if let Some(v) = self.get_int(target_id, &target_attr) {
                            results.push(QueryResult::Int(v));
                        } else if let Some(v) = self.get_str(target_id, &target_attr) {
                            results.push(QueryResult::Str(v));
                        } else {
                            results.push(QueryResult::None);
                        }
                    } else {
                        results.push(QueryResult::None);
                    }
                }
                QueryNode::Range {
                    entity_id,
                    attr,
                    len,
                } => {
                    // Range scan usually happens on hot data or specific blocks
                    if let Some(ptr) = self.get_pointer(entity_id) {
                        if let Some(&start_idx) = ptr.attribute_indices.get(&attr) {
                            if let Some(col) = self.route.get_column_int(&attr, &self.worker) {
                                let range_vals = col.with_data(&self.worker, |data| {
                                    data.iter()
                                        .skip(start_idx)
                                        .take(len)
                                        .flatten()
                                        .cloned()
                                        .collect()
                                });
                                results.push(QueryResult::IntRange(range_vals));
                                continue;
                            }
                        }
                    }
                    results.push(QueryResult::None);
                }
                QueryNode::Scan { attr } => {
                    if let Some(col) = self.route.get_column_int(&attr, &self.worker) {
                        let vals = col.with_data(&self.worker, |data| {
                            data.iter().flatten().cloned().collect()
                        });
                        results.push(QueryResult::IntList(vals));
                    } else if let Some(col) = self.route.get_column_str(&attr, &self.worker) {
                        let vals = col.with_data(&self.worker, |data| {
                            data.iter().flatten().cloned().collect()
                        });
                        results.push(QueryResult::StrList(vals));
                    } else {
                        results.push(QueryResult::None);
                    }
                }
                QueryNode::Aggregate { attr, op } => {
                    if let Some(col) = self.route.get_column_int(&attr, &self.worker) {
                        let res = col.with_data(&self.worker, |data| {
                            let it = data.iter().flatten().map(|&v| v);
                            match op {
                                AggregateOp::Sum => {
                                    QueryResult::IntSum(it.map(|v| v as u64).sum())
                                }
                                AggregateOp::Count => {
                                    QueryResult::Count(it.count())
                                }
                                AggregateOp::Min => {
                                    QueryResult::IntMin(it.min().unwrap_or(0))
                                }
                                AggregateOp::Max => {
                                    QueryResult::IntMax(it.max().unwrap_or(0))
                                }
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
                        results.push(res);
                    } else {
                        results.push(QueryResult::None);
                    }
                }
            }
        }
        results
    }

    fn get_pointer(&self, entity_id: usize) -> Option<MultiVectorPointer> {
        // 1. Bloom Filter Check
        {
            let bloom = self.route.bloom_filter.lock().unwrap();
            if !bloom.contains(&entity_id) {
                return None;
            }
        }

        // 2. Full Memory Index Check (Wait-Free RCU)
        {
            self.worker.enter();
            let snap = load_ref(&self.route.shared_pointers);
            let ptr = snap.get(&entity_id).cloned();
            self.worker.leave();
            if let Some(p) = ptr {
                self.route.hot_index.insert(entity_id, ()); // Track hit
                return Some(p);
            }
        }

        // 3. Page Fault (Synchronous Disk Load)
        let (tx, rx) = bounded(1);
        let _ = self.route.writer_tx.send(PartitionCommand::InternalLoad {
            entity_id,
            response_tx: tx,
        });
        
        rx.recv().unwrap_or(None)
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

    pub fn seed_bloom_filter(&self, entity_id: usize) {
        let mut bloom = self.route.bloom_filter.lock().unwrap();
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
