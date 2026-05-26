use crate::AHashMap;
use alloc::sync::Arc;
use alloc::vec::Vec;
use alloc::string::String;
use crate::platform::atomic::{AtomicBool, AtomicPtr, Ordering};
use crate::unsafe_core::{load_clone, load_ref, new_atomic_ptr};
use crate::qsbr::WorkerState;

#[derive(Clone)]
pub struct Columns<const N: usize> {
    pub str_cols: AHashMap<String, Arc<ColumnArray<String, N>>>,
    pub int_cols: AHashMap<String, Arc<ColumnArray<u32, N>>>,
    pub blob_cols: AHashMap<String, Arc<ColumnArray<Vec<u8>, N>>>,
}

impl<const N: usize> Columns<N> {
    pub fn new() -> Self {
        Self {
            str_cols: AHashMap::default(),
            int_cols: AHashMap::default(),
            blob_cols: AHashMap::default(),
        }
    }
}

/// 1. 最底層的連續資料陣列 (Column / DOD 結構)
///
/// 使用自定義 QSBR 實現 Wait-Free 讀取
pub struct ColumnArray<T, const N: usize> {
    pub data: AtomicPtr<Vec<Option<T>>>,
    pub waitlist: AtomicPtr<Vec<usize>>,
    pub(crate) write_guard: AtomicBool,
}

impl<T, const N: usize> ColumnArray<T, N> {
    pub fn new() -> Self {
        Self {
            data: new_atomic_ptr(Vec::new()),
            waitlist: new_atomic_ptr(Vec::new()),
            write_guard: AtomicBool::new(false),
        }
    }

    pub fn acquire_lock(&self) {
        if self.write_guard.swap(true, Ordering::SeqCst) {
            panic!(
                "Data Race Detected: Multiple writers attempted to access the same ColumnArray!"
            );
        }
    }

    pub fn release_lock(&self) {
        self.write_guard.store(false, Ordering::SeqCst);
    }

    pub fn get_element(&self, idx: usize, worker: &WorkerState) -> Option<T>
    where
        T: Clone,
    {
        worker.enter();
        let data = load_ref(&self.data);
        let val = data.get(idx).and_then(|v| v.as_ref().cloned());
        worker.leave();
        val
    }

    #[inline(always)]
    pub fn get_element_pinned(&self, idx: usize) -> Option<T>
    where
        T: Clone,
    {
        let data = load_ref(&self.data);
        data.get(idx).and_then(|v| v.as_ref().cloned())
    }

    pub fn with_element<F, R>(&self, idx: usize, worker: &WorkerState, f: F) -> Option<R>
    where
        F: FnOnce(&T) -> R,
    {
        worker.enter();
        let data = load_ref(&self.data);
        let res = data.get(idx).and_then(|v| v.as_ref().map(f));
        worker.leave();
        res
    }

    #[inline(always)]
    pub fn with_element_pinned<F, R>(&self, idx: usize, f: F) -> Option<R>
    where
        F: FnOnce(&T) -> R,
    {
        let data = load_ref(&self.data);
        data.get(idx).and_then(|v| v.as_ref().map(f))
    }

    pub fn get_data_snapshot(&self, worker: &WorkerState) -> Vec<Option<T>>
    where
        T: Clone,
    {
        worker.enter();
        let data = load_clone(&self.data);
        worker.leave();
        data
    }

    pub fn get_waitlist_snapshot(&self, worker: &WorkerState) -> Vec<usize> {
        worker.enter();
        let wl = load_clone(&self.waitlist);
        worker.leave();
        wl
    }

    pub fn data_len(&self, worker: &WorkerState) -> usize {
        worker.enter();
        let data = load_ref(&self.data);
        let len = data.len();
        worker.leave();
        len
    }

    /// Optimized: Enter QSBR once for multiple reads
    pub fn with_data<F, R>(&self, worker: &WorkerState, f: F) -> R
    where
        F: FnOnce(&Vec<Option<T>>) -> R,
    {
        worker.enter();
        let data = load_ref(&self.data);
        let res = f(data);
        worker.leave();
        res
    }

    #[inline(always)]
    pub fn with_data_pinned<F, R>(&self, f: F) -> R
    where
        F: FnOnce(&Vec<Option<T>>) -> R,
    {
        let data = load_ref(&self.data);
        f(data)
    }
}

impl<T, const N: usize> Default for ColumnArray<T, N> {
    fn default() -> Self {
        Self::new()
    }
}
