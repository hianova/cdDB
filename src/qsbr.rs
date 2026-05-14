use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use crate::unsafe_core::GarbageEntry;

/// 全域邏輯時鐘
pub static GLOBAL_EPOCH: AtomicUsize = AtomicUsize::new(1);

/// Worker 狀態：0 代表靜止 (Quiescent), >0 代表正在讀取的 Epoch
pub struct WorkerState {
    pub local_epoch: AtomicUsize,
}

impl WorkerState {
    pub fn new() -> Self {
        Self {
            local_epoch: AtomicUsize::new(0),
        }
    }

    /// 進入讀取路徑 (打卡)
    #[inline(always)]
    pub fn enter(&self) {
        let current = GLOBAL_EPOCH.load(Ordering::Relaxed);
        self.local_epoch.store(current, Ordering::Release);
    }

    /// 離開讀取路徑 (登出)
    #[inline(always)]
    pub fn leave(&self) {
        self.local_epoch.store(0, Ordering::Release);
    }
}

/// QSBR 管理器：追蹤所有 Worker 並執行垃圾回收
pub struct QsbrManager {
    workers: Arc<Mutex<Vec<Arc<WorkerState>>>>,
    garbage: Vec<GarbageEntry>,
}

impl QsbrManager {
    pub fn new(workers: Arc<Mutex<Vec<Arc<WorkerState>>>>) -> Self {
        Self {
            workers,
            garbage: Vec::new(),
        }
    }

    /// 註冊垃圾 (由 Daemon 調用)
    pub fn defer_free<T>(&mut self, ptr: *mut T) {
        if ptr.is_null() { return; }
        
        self.garbage.push(GarbageEntry::new(
            ptr,
            GLOBAL_EPOCH.load(Ordering::Relaxed),
        ));
    }

    /// 執行維護：推進 Epoch 並清理過期的垃圾
    pub fn maintenance(&mut self) {
        // 1. 推進全域時鐘
        GLOBAL_EPOCH.fetch_add(1, Ordering::Relaxed);

        // 2. 獲取所有活躍 Worker 的最小 Epoch
        let mut min_active = usize::MAX;
        {
            let workers = self.workers.lock().unwrap();
            for worker in workers.iter() {
                let e = worker.local_epoch.load(Ordering::Acquire);
                if e != 0 && e < min_active {
                    min_active = e;
                }
            }
        }

        // 3. 清理：如果垃圾的 Epoch < min_active，代表沒有 Worker 在看它
        // GarbageEntry implements Drop, so retain(false) will trigger the drop logic.
        self.garbage.retain(|entry| entry.epoch >= min_active);
    }
}
