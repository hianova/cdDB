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

#[derive(Clone)]
pub struct ColumnData<T> {
    pub data: Vec<T>,
    pub valid: Vec<u64>,
}

impl<T: Default + Clone> ColumnData<T> {
    pub fn new() -> Self {
        Self {
            data: Vec::new(),
            valid: Vec::new(),
        }
    }

    #[inline(always)]
    pub fn is_valid(&self, idx: usize) -> bool {
        let block = idx / 64;
        let bit = idx % 64;
        if block < self.valid.len() {
            (self.valid[block] & (1 << bit)) != 0
        } else {
            false
        }
    }

    pub fn set_valid(&mut self, idx: usize, valid: bool) {
        let block = idx / 64;
        let bit = idx % 64;
        if block >= self.valid.len() {
            self.valid.resize(block + 1, 0);
        }
        if valid {
            self.valid[block] |= 1 << bit;
        } else {
            self.valid[block] &= !(1 << bit);
        }
    }

    #[inline(always)]
    pub fn get(&self, idx: usize) -> Option<&T> {
        if self.is_valid(idx) {
            self.data.get(idx)
        } else {
            None
        }
    }

    pub fn len(&self) -> usize {
        self.data.len()
    }

    pub fn push(&mut self, val: T) {
        let idx = self.data.len();
        self.data.push(val);
        self.set_valid(idx, true);
    }
    
    pub fn set(&mut self, idx: usize, val: T) {
        if idx >= self.data.len() {
            self.data.resize(idx + 1, T::default());
        }
        self.data[idx] = val;
        self.set_valid(idx, true);
    }

    pub fn iter(&self) -> ColumnDataIter<'_, T> {
        ColumnDataIter { col: self, idx: 0 }
    }
}

pub struct ColumnDataIter<'a, T> {
    col: &'a ColumnData<T>,
    idx: usize,
}

impl<'a, T: Default + Clone> Iterator for ColumnDataIter<'a, T> {
    type Item = Option<&'a T>;
    fn next(&mut self) -> Option<Self::Item> {
        if self.idx < self.col.len() {
            let res = self.col.get(self.idx);
            self.idx += 1;
            Some(res)
        } else {
            None
        }
    }
}

/// 1. 最底層的連續資料陣列 (Column / DOD 結構)
///
/// 使用自定義 QSBR 實現 Wait-Free 讀取
pub struct ColumnArray<T, const N: usize> {
    pub data: AtomicPtr<ColumnData<T>>,
    pub waitlist: AtomicPtr<Vec<usize>>,
    pub(crate) write_guard: AtomicBool,
}

impl<T: Default + Clone, const N: usize> ColumnArray<T, N> {
    pub fn new() -> Self {
        Self {
            data: new_atomic_ptr(ColumnData::new()),
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

    pub fn get_element(&self, idx: usize, worker: &WorkerState) -> Option<T> {
        worker.enter();
        let data = load_ref(&self.data);
        let val = data.get(idx).cloned();
        worker.leave();
        val
    }

    #[inline(always)]
    pub fn get_element_pinned(&self, idx: usize) -> Option<T> {
        let data = load_ref(&self.data);
        data.get(idx).cloned()
    }

    pub fn with_element<F, R>(&self, idx: usize, worker: &WorkerState, f: F) -> Option<R>
    where
        F: FnOnce(&T) -> R,
    {
        worker.enter();
        let data = load_ref(&self.data);
        let res = data.get(idx).map(f);
        worker.leave();
        res
    }

    #[inline(always)]
    pub fn with_element_pinned<F, R>(&self, idx: usize, f: F) -> Option<R>
    where
        F: FnOnce(&T) -> R,
    {
        let data = load_ref(&self.data);
        data.get(idx).map(f)
    }

    pub fn get_data_snapshot(&self, worker: &WorkerState) -> ColumnData<T> {
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
        F: FnOnce(&ColumnData<T>) -> R,
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
        F: FnOnce(&ColumnData<T>) -> R,
    {
        let data = load_ref(&self.data);
        f(data)
    }
}

impl<T: Default + Clone, const N: usize> Default for ColumnArray<T, N> {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(all(test, not(feature = "loom")))]
mod tests {
    use super::*;
    use crate::qsbr::QsbrManager;
    use crate::unsafe_core::{load_clone, swap_ptr};

    #[test]
    fn test_column_array_insertion() {
        use crate::platform::atomic::AtomicPtr;
        use alloc::sync::Arc;
        let workers = Arc::new(AtomicPtr::new(core::ptr::null_mut()));
        let mut qsbr = QsbrManager::new(workers);
        let col = ColumnArray::<u32, 1024>::new();
        col.acquire_lock();
        
        let mut next = load_clone(&col.data);
        let idx1 = next.len();
        next.push(42);
        let mut next2 = next.clone();
        let idx2 = next2.len();
        next2.push(100);
        
        let old = swap_ptr(&col.data, next2);
        qsbr.defer_free(old);
        col.release_lock();

        assert_eq!(idx1, 0);
        assert_eq!(idx2, 1);
        
        let val1 = col.get_element_pinned(0).unwrap();
        let val2 = col.get_element_pinned(1).unwrap();
        
        assert_eq!(val1, 42);
        assert_eq!(val2, 100);
    }
}
