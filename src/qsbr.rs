use crate::platform::atomic::{AtomicUsize, AtomicPtr, Ordering};
use alloc::sync::Arc;
use alloc::vec::Vec;
use crate::unsafe_core::GarbageEntry;

pub struct WorkerNode {
    pub worker: Arc<WorkerState>,
    pub next: AtomicPtr<WorkerNode>,
}

/// 全域邏輯時鐘
pub static GLOBAL_EPOCH: core::sync::atomic::AtomicUsize = core::sync::atomic::AtomicUsize::new(1);

/// RCU 的執行緒本地狀態
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
        let global = GLOBAL_EPOCH.load(Ordering::Acquire);
        self.local_epoch.store(global, Ordering::Release);
    }

    /// 離開讀取路徑 (登出)
    #[inline(always)]
    pub fn leave(&self) {
        self.local_epoch.store(0, Ordering::Release);
    }
}

/// QSBR 管理器：追蹤所有 Worker 並執行垃圾回收
pub struct QsbrManager {
    workers: Arc<AtomicPtr<WorkerNode>>,
    garbage: Vec<GarbageEntry>,
}

impl QsbrManager {
    pub fn new(workers: Arc<AtomicPtr<WorkerNode>>) -> Self {
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

        let current_global = GLOBAL_EPOCH.load(Ordering::Acquire);
        let mut min_epoch = current_global;

        let mut curr_ptr = self.workers.load(Ordering::Acquire);
        while let Some(node) = crate::unsafe_core::load_node(curr_ptr) {
            let epoch = node.worker.local_epoch.load(Ordering::Acquire);
            if epoch != 0 && epoch < min_epoch {
                min_epoch = epoch;
            }
            curr_ptr = node.next.load(Ordering::Acquire);
        }

        if min_epoch == current_global {
            GLOBAL_EPOCH.fetch_add(1, Ordering::Release);
        }

        // 3. 清理：如果垃圾的 Epoch < min_active，代表沒有 Worker 在看它
        // GarbageEntry implements Drop, so retain(false) will trigger the drop logic.
        self.garbage.retain(|entry| entry.epoch >= min_epoch);
    }
}
