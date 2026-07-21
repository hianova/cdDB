use crate::AHashMap;
use crate::core::atomic::{AtomicBool, AtomicPtr, Ordering};
use crate::core::qsbr::WorkerState;
use crate::core::rcu::{load_clone, load_ref, new_atomic_ptr};
use alloc::string::String;
use alloc::sync::Arc;
use alloc::vec::Vec;
#[doc = " A typed collection of named columnar arrays, grouped by element type."]
#[doc = ""]
#[doc = " `Columns<N>` is the top-level container that maps column names to their"]
#[doc = " corresponding [`ColumnArray`] instances.  Three column families are"]
#[doc = " supported out of the box:"]
#[doc = ""]
#[doc = " * **str_cols** — UTF-8 string columns"]
#[doc = " * **int_cols** — 32-bit unsigned integer columns"]
#[doc = " * **blob_cols** — arbitrary byte-blob columns"]
#[doc = ""]
#[doc = " The const generic `N` is forwarded to every [`ColumnArray`] and controls"]
#[doc = " the internal chunking granularity."]
#[derive(Clone)]
#[repr(C, align(64))]
pub struct Columns<const N: usize> {
    #[doc = " Named columns whose values are UTF-8 [`String`]s."]
    pub str_cols: AHashMap<String, Arc<ColumnArray<String, N>>>,
    #[doc = " Named columns whose values are unsigned 32-bit integers ([`u32`])."]
    pub int_cols: AHashMap<String, Arc<ColumnArray<u32, N>>>,
    #[doc = " Named columns whose values are raw byte blobs ([`Vec<u8>`])."]
    pub blob_cols: AHashMap<String, Arc<ColumnArray<Vec<u8>, N>>>,
}
impl<const N: usize> Default for Columns<N> {
    fn default() -> Self {
        Self::new()
    }
}
impl<const N: usize> Columns<N> {
    #[doc = " Creates a new, empty [`Columns`] container with no columns registered."]
    #[doc = ""]
    #[doc = " All three column families (`str_cols`, `int_cols`, `blob_cols`) start"]
    #[doc = " as empty hash-maps."]
    #[doc = ""]
    #[doc = " # Examples"]
    #[doc = ""]
    #[doc = " ```"]
    #[doc = " let cols = Columns::<1024>::new();"]
    #[doc = " assert!(cols.str_cols.is_empty());"]
    #[doc = " ```"]
    pub fn new() -> Self {
        Self {
            str_cols: AHashMap::default(),
            int_cols: AHashMap::default(),
            blob_cols: AHashMap::default(),
        }
    }
}
#[doc = " An RCU snapshot pointer for a single entity, containing the entity ID and a map of"]
#[doc = " attribute names to their column array indices."]
#[doc = ""]
#[doc = " This pointer is conceptually the \"row\" in our columnar store. By cloning this"]
#[doc = " pointer (which only clones an `AHashMap` of `usize`), a reader can safely pin"]
#[doc = " a specific snapshot of an entity's data."]
#[derive(Clone, Debug, Default)]
#[repr(C, align(64))]
pub struct MultiVectorPointer {
    pub entity_id: usize,
    pub attribute_indices: AHashMap<String, usize>,
}
#[doc = " Contiguous columnar storage for values of type `T`, paired with a"]
#[doc = " compact validity bitvector."]
#[doc = ""]
#[doc = " Each logical row maps to one element in `data` at the same index.  Whether"]
#[doc = " that element holds a meaningful value is tracked by `valid`, a packed array"]
#[doc = " of 64-bit words where bit `i` of word `i / 64` corresponds to row `i`."]
#[doc = " A set bit means the slot is valid (non-null); a cleared bit means it is"]
#[doc = " absent (null)."]
#[doc = ""]
#[doc = " `ColumnData<T>` is the snapshot type returned by"]
#[doc = " [`ColumnArray::get_data_snapshot`] and the underlying value swapped"]
#[doc = " atomically by writers."]
#[derive(Clone)]
#[repr(C, align(64))]
pub struct ColumnData<T> {
    #[doc = " The raw row values stored contiguously.  Slots whose validity bit is"]
    #[doc = " cleared may contain stale or default-initialised data and **must not**"]
    #[doc = " be read without first checking [`ColumnData::is_valid`]."]
    pub data: Vec<T>,
    #[doc = " Packed validity bitvector.  Each `u64` word covers 64 consecutive rows."]
    #[doc = " Bit `idx % 64` of word `idx / 64` is set when row `idx` is valid."]
    pub valid: Vec<u64>,
}
impl<T: Default + Clone> Default for ColumnData<T> {
    fn default() -> Self {
        Self::new()
    }
}
impl<T: Default + Clone> ColumnData<T> {
    #[doc = " Creates a new, empty [`ColumnData`] with no rows."]
    #[doc = ""]
    #[doc = " Both the data vector and the validity bitvector start empty."]
    #[doc = ""]
    #[doc = " # Examples"]
    #[doc = ""]
    #[doc = " ```"]
    #[doc = " let cd = ColumnData::<u32>::new();"]
    #[doc = " assert!(cd.is_empty());"]
    #[doc = " ```"]
    pub fn new() -> Self {
        Self {
            data: Vec::new(),
            valid: Vec::new(),
        }
    }
    #[doc = " Returns `true` if row `idx` is marked as valid (non-null)."]
    #[doc = ""]
    #[doc = " Rows that have never been written, or that have been explicitly"]
    #[doc = " invalidated with [`set_valid`](Self::set_valid), return `false`."]
    #[doc = " Indices beyond the current bitvector range are always `false`."]
    #[doc = ""]
    #[doc = " # Examples"]
    #[doc = ""]
    #[doc = " ```"]
    #[doc = " let mut cd = ColumnData::<u32>::new();"]
    #[doc = " cd.push(42);"]
    #[doc = " assert!(cd.is_valid(0));"]
    #[doc = " assert!(!cd.is_valid(1)); // out of range"]
    #[doc = " ```"]
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
    #[doc = " Sets or clears the validity bit for row `idx`."]
    #[doc = ""]
    #[doc = " If `idx` falls in a bitvector word that does not yet exist, the"]
    #[doc = " `valid` vector is grown and zero-filled up to the required word."]
    #[doc = ""]
    #[doc = " # Examples"]
    #[doc = ""]
    #[doc = " ```"]
    #[doc = " let mut cd = ColumnData::<u32>::new();"]
    #[doc = " cd.push(10);"]
    #[doc = " cd.set_valid(0, false); // logically delete the row"]
    #[doc = " assert!(!cd.is_valid(0));"]
    #[doc = " cd.set_valid(0, true);  // restore it"]
    #[doc = " assert!(cd.is_valid(0));"]
    #[doc = " ```"]
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
    #[doc = " Returns a reference to the value at row `idx`, or `None` if the row"]
    #[doc = " is invalid or `idx` is out of bounds."]
    #[doc = ""]
    #[doc = " This is the safe, validity-checked accessor.  It is equivalent to"]
    #[doc = " calling [`is_valid`](Self::is_valid) followed by indexing into"]
    #[doc = " `self.data`."]
    #[doc = ""]
    #[doc = " # Examples"]
    #[doc = ""]
    #[doc = " ```"]
    #[doc = " let mut cd = ColumnData::<u32>::new();"]
    #[doc = " cd.push(99);"]
    #[doc = " assert_eq!(cd.get(0), Some(&99));"]
    #[doc = " assert_eq!(cd.get(1), None); // out of bounds"]
    #[doc = " ```"]
    #[inline(always)]
    pub fn get(&self, idx: usize) -> Option<&T> {
        if self.is_valid(idx) {
            self.data.get(idx)
        } else {
            None
        }
    }
    #[doc = " Returns the total number of rows allocated in the data vector,"]
    #[doc = " **including** any invalid (null) slots."]
    #[doc = ""]
    #[doc = " # Examples"]
    #[doc = ""]
    #[doc = " ```"]
    #[doc = " let mut cd = ColumnData::<u32>::new();"]
    #[doc = " assert_eq!(cd.len(), 0);"]
    #[doc = " cd.push(1);"]
    #[doc = " assert_eq!(cd.len(), 1);"]
    #[doc = " ```"]
    pub fn len(&self) -> usize {
        self.data.len()
    }
    #[doc = " Returns `true` if the data vector contains no rows."]
    #[doc = ""]
    #[doc = " # Examples"]
    #[doc = ""]
    #[doc = " ```"]
    #[doc = " let cd = ColumnData::<u32>::new();"]
    #[doc = " assert!(cd.is_empty());"]
    #[doc = " ```"]
    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }
    #[doc = " Appends `val` as a new valid row at the end of the column."]
    #[doc = ""]
    #[doc = " The validity bit for the new row is automatically set to `true`."]
    #[doc = ""]
    #[doc = " # Examples"]
    #[doc = ""]
    #[doc = " ```"]
    #[doc = " let mut cd = ColumnData::<u32>::new();"]
    #[doc = " cd.push(7);"]
    #[doc = " assert_eq!(cd.get(0), Some(&7));"]
    #[doc = " ```"]
    pub fn push(&mut self, val: T) {
        let idx = self.data.len();
        self.data.push(val);
        self.set_valid(idx, true);
    }
    #[doc = " Writes `val` to row `idx` and marks that row as valid."]
    #[doc = ""]
    #[doc = " If `idx` is beyond the current length of the data vector, the vector"]
    #[doc = " is grown with [`T::default()`](Default::default) fill values.  The"]
    #[doc = " intermediate rows created by the resize are **not** marked valid."]
    #[doc = ""]
    #[doc = " # Examples"]
    #[doc = ""]
    #[doc = " ```"]
    #[doc = " let mut cd = ColumnData::<u32>::new();"]
    #[doc = " cd.set(5, 42); // rows 0-4 are allocated but invalid"]
    #[doc = " assert_eq!(cd.len(), 6);"]
    #[doc = " assert_eq!(cd.get(5), Some(&42));"]
    #[doc = " assert!(!cd.is_valid(0)); // gap rows are not valid"]
    #[doc = " ```"]
    pub fn set(&mut self, idx: usize, val: T) {
        if idx >= self.data.len() {
            self.data.resize(idx + 1, T::default());
        }
        self.data[idx] = val;
        self.set_valid(idx, true);
    }
    #[doc = " Returns an iterator over all rows, yielding `Option<&T>`."]
    #[doc = ""]
    #[doc = " Each call to [`Iterator::next`] advances by one row.  Valid rows"]
    #[doc = " produce `Some(&value)`; invalid (null) rows produce `None`."]
    #[doc = " The iterator always yields exactly [`len`](Self::len) items."]
    #[doc = ""]
    #[doc = " See also [`ColumnDataIter`]."]
    #[doc = ""]
    #[doc = " # Examples"]
    #[doc = ""]
    #[doc = " ```"]
    #[doc = " let mut cd = ColumnData::<u32>::new();"]
    #[doc = " cd.push(1);"]
    #[doc = " cd.push(2);"]
    #[doc = " cd.set_valid(0, false); // mark first row invalid"]
    #[doc = " let items: Vec<_> = cd.iter().collect();"]
    #[doc = " assert_eq!(items, vec![None, Some(&2)]);"]
    #[doc = " ```"]
    pub fn iter(&self) -> ColumnDataIter<'_, T> {
        ColumnDataIter { col: self, idx: 0 }
    }
}
#[doc = " An iterator over the rows of a [`ColumnData<T>`]."]
#[doc = ""]
#[doc = " Created by [`ColumnData::iter`].  Each call to [`Iterator::next`] returns"]
#[doc = " `Some(Option<&T>)` until all rows have been visited, then `None`."]
#[doc = ""]
#[doc = " * `Some(Some(&value))` — the row is valid and contains `value`."]
#[doc = " * `Some(None)`         — the row exists but is invalid (null)."]
#[doc = " * `None`               — the iterator is exhausted."]
#[repr(C, align(64))]
pub struct ColumnDataIter<'a, T> {
    #[doc = " The [`ColumnData`] being iterated."]
    col: &'a ColumnData<T>,
    #[doc = " The index of the next row to yield."]
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
#[doc = " The core wait-free columnar storage structure."]
#[doc = ""]
#[doc = " `ColumnArray<T, N>` is the lowest-level building block of the database's"]
#[doc = " column-oriented (Data-Oriented Design) storage layer.  It manages a"]
#[doc = " heap-allocated [`ColumnData<T>`] behind an [`AtomicPtr`], enabling"]
#[doc = " **wait-free reads** via Quiescent-State-Based Reclamation (QSBR):"]
#[doc = ""]
#[doc = " * **Readers** enter a QSBR-pinned region, load the pointer, perform their"]
#[doc = "   work, and leave — all without taking any lock."]
#[doc = " * **Writers** must call [`acquire_lock`](Self::acquire_lock) first, clone"]
#[doc = "   the current data, mutate the clone, atomically swap the pointer, then"]
#[doc = "   call [`release_lock`](Self::release_lock).  The old pointer is deferred"]
#[doc = "   to the QSBR manager for safe reclamation once all readers have left."]
#[doc = ""]
#[doc = " The const generic `N` is reserved for future chunking/segmentation and is"]
#[doc = " currently forwarded through the type system."]
#[doc = ""]
#[doc = " A companion [`waitlist`](Self::waitlist) pointer tracks row indices that"]
#[doc = " are pending some deferred operation (e.g. delete tombstones)."]
#[repr(C, align(64))]
pub struct ColumnArray<T, const N: usize> {
    #[doc = " Atomically-swapped pointer to the current [`ColumnData<T>`] snapshot."]
    #[doc = ""]
    #[doc = " Readers load this with `Ordering::Acquire` (via [`load_ref`]) while"]
    #[doc = " inside a QSBR-pinned region.  Writers swap it with a freshly built"]
    #[doc = " clone after acquiring [`write_guard`](Self::write_guard)."]
    pub data: AtomicPtr<ColumnData<T>>,
    #[doc = " Atomically-swapped pointer to the pending-operation waitlist."]
    #[doc = ""]
    #[doc = " The waitlist is a `Vec<usize>` of row indices that have been logically"]
    #[doc = " deleted (or otherwise flagged) but not yet physically reclaimed."]
    pub waitlist: AtomicPtr<Vec<usize>>,
    #[doc = " Spin-lock flag that serialises concurrent writers."]
    #[doc = ""]
    #[doc = " `true` means a writer currently holds the lock.  Attempting to acquire"]
    #[doc = " the lock while it is already held panics with a \"Data Race Detected\""]
    #[doc = " message — see [`acquire_lock`](Self::acquire_lock)."]
    pub(crate) write_guard: AtomicBool,
}
impl<T: Default + Clone, const N: usize> ColumnArray<T, N> {
    #[doc = " Creates a new [`ColumnArray`] with an empty [`ColumnData`] and an"]
    #[doc = " empty waitlist, both heap-allocated and owned by atomic pointers."]
    #[doc = ""]
    #[doc = " The write guard is initialised to `false` (unlocked)."]
    #[doc = ""]
    #[doc = " # Examples"]
    #[doc = ""]
    #[doc = " ```"]
    #[doc = " let col = ColumnArray::<u32, 1024>::new();"]
    #[doc = " assert_eq!(col.get_element_pinned(0), None);"]
    #[doc = " ```"]
    pub fn new() -> Self {
        Self {
            data: new_atomic_ptr(ColumnData::new()),
            waitlist: new_atomic_ptr(Vec::new()),
            write_guard: AtomicBool::new(false),
        }
    }
    #[doc = " Acquires the exclusive write lock for this column."]
    #[doc = ""]
    #[doc = " Uses an atomic swap on [`write_guard`](Self::write_guard) with"]
    #[doc = " `SeqCst` ordering to ensure all prior memory operations are visible"]
    #[doc = " before the lock is considered held."]
    #[doc = ""]
    #[doc = " After acquiring the lock, the caller must:"]
    #[doc = " 1. Clone the current data via [`get_data_snapshot`](Self::get_data_snapshot)"]
    #[doc = "    or a direct `load_clone`."]
    #[doc = " 2. Mutate the clone."]
    #[doc = " 3. Atomically swap the new data pointer in."]
    #[doc = " 4. Defer the old pointer to the QSBR manager for safe reclamation."]
    #[doc = " 5. Call [`release_lock`](Self::release_lock) when done."]
    #[doc = ""]
    #[doc = " # Panics"]
    #[doc = ""]
    #[doc = " Panics immediately with `\"Data Race Detected: Multiple writers"]
    #[doc = " attempted to access the same ColumnArray!\"` if the lock is already"]
    #[doc = " held.  This enforces the single-writer invariant at runtime."]
    pub fn acquire_lock(&self) {
        if self.write_guard.swap(true, Ordering::SeqCst) {
            panic!(
                "Data Race Detected: Multiple writers attempted to access the same ColumnArray!"
            );
        }
    }
    #[doc = " Releases the exclusive write lock, making the column available for"]
    #[doc = " subsequent writers."]
    #[doc = ""]
    #[doc = " This performs a `SeqCst` store of `false` to [`write_guard`](Self::write_guard)."]
    #[doc = " It should always be called after [`acquire_lock`](Self::acquire_lock),"]
    #[doc = " even if the write operation encounters an error."]
    pub fn release_lock(&self) {
        self.write_guard.store(false, Ordering::SeqCst);
    }
    #[doc = " Returns a clone of the element at row `idx`, or `None` if the row is"]
    #[doc = " invalid or `idx` is out of bounds."]
    #[doc = ""]
    #[doc = " This method manages its own QSBR epoch: it calls `worker.enter()`,"]
    #[doc = " reads the element, then calls `worker.leave()` before returning.  Use"]
    #[doc = " [`get_element_pinned`](Self::get_element_pinned) when the caller is"]
    #[doc = " already inside a pinned region."]
    #[doc = ""]
    #[doc = " # Examples"]
    #[doc = ""]
    #[doc = " ```"]
    #[doc = " let worker = WorkerState::new();"]
    #[doc = " let col = ColumnArray::<u32, 1024>::new();"]
    #[doc = " assert_eq!(col.get_element(0, &worker), None);"]
    #[doc = " ```"]
    pub fn get_element(&self, idx: usize, worker: &WorkerState) -> Option<T> {
        worker.enter();
        let data = load_ref(&self.data);
        let val = data.get(idx).cloned();
        worker.leave();
        val
    }
    #[doc = " Returns a clone of the element at row `idx`, or `None` if the row is"]
    #[doc = " invalid or `idx` is out of bounds."]
    #[doc = ""]
    #[doc = " **QSBR contract**: the caller **must** already be inside a"]
    #[doc = " QSBR-pinned region (i.e. `worker.enter()` has been called and"]
    #[doc = " `worker.leave()` has *not* yet been called).  This method does not"]
    #[doc = " call `enter`/`leave` itself, making it cheaper for hot paths that"]
    #[doc = " batch multiple reads inside a single pinned region."]
    #[doc = ""]
    #[doc = " # Examples"]
    #[doc = ""]
    #[doc = " ```"]
    #[doc = " // Caller is responsible for the QSBR epoch:"]
    #[doc = " worker.enter();"]
    #[doc = " let val = col.get_element_pinned(0);"]
    #[doc = " worker.leave();"]
    #[doc = " ```"]
    #[inline(always)]
    pub fn get_element_pinned(&self, idx: usize) -> Option<T> {
        let data = load_ref(&self.data);
        data.get(idx).cloned()
    }
    #[doc = " Applies the closure `f` to a reference of the element at row `idx`,"]
    #[doc = " returning `Some(R)` if the row is valid, or `None` otherwise."]
    #[doc = ""]
    #[doc = " This method manages its own QSBR epoch (`worker.enter()` /"]
    #[doc = " `worker.leave()`), so the reference passed to `f` is guaranteed to"]
    #[doc = " be live for the duration of the call.  Prefer"]
    #[doc = " [`with_element_pinned`](Self::with_element_pinned) when already inside"]
    #[doc = " a pinned region."]
    #[doc = ""]
    #[doc = " # Examples"]
    #[doc = ""]
    #[doc = " ```"]
    #[doc = " let worker = WorkerState::new();"]
    #[doc = " let length = col.with_element(0, &worker, |s: &String| s.len());"]
    #[doc = " ```"]
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
    #[doc = " Applies the closure `f` to a reference of the element at row `idx`,"]
    #[doc = " returning `Some(R)` if the row is valid, or `None` otherwise."]
    #[doc = ""]
    #[doc = " **QSBR contract**: the caller **must** already be inside a"]
    #[doc = " QSBR-pinned region.  This method skips the `enter`/`leave` calls,"]
    #[doc = " making it the preferred choice when batching multiple element accesses"]
    #[doc = " inside a single pinned region."]
    #[doc = ""]
    #[doc = " # Examples"]
    #[doc = ""]
    #[doc = " ```"]
    #[doc = " worker.enter();"]
    #[doc = " let len = col.with_element_pinned(0, |s: &String| s.len());"]
    #[doc = " worker.leave();"]
    #[doc = " ```"]
    #[inline(always)]
    pub fn with_element_pinned<F, R>(&self, idx: usize, f: F) -> Option<R>
    where
        F: FnOnce(&T) -> R,
    {
        let data = load_ref(&self.data);
        data.get(idx).map(f)
    }
    #[doc = " Returns an owned deep clone of the current [`ColumnData<T>`] snapshot."]
    #[doc = ""]
    #[doc = " The clone is taken while holding a QSBR epoch (via `worker.enter()` /"]
    #[doc = " `worker.leave()`), ensuring the pointer remains valid for the duration"]
    #[doc = " of the clone.  The returned value is fully owned and independent of the"]
    #[doc = " live atomic pointer."]
    #[doc = ""]
    #[doc = " This is the recommended way for a writer to read the current state"]
    #[doc = " before constructing a modified copy."]
    #[doc = ""]
    #[doc = " # Examples"]
    #[doc = ""]
    #[doc = " ```"]
    #[doc = " let worker = WorkerState::new();"]
    #[doc = " let snapshot = col.get_data_snapshot(&worker);"]
    #[doc = " assert_eq!(snapshot.len(), col.data_len(&worker));"]
    #[doc = " ```"]
    pub fn get_data_snapshot(&self, worker: &WorkerState) -> ColumnData<T> {
        worker.enter();
        let data = load_clone(&self.data);
        worker.leave();
        data
    }
    #[doc = " Returns an owned clone of the current waitlist (`Vec<usize>`)."]
    #[doc = ""]
    #[doc = " The waitlist contains row indices that are pending deferred operations"]
    #[doc = " (e.g. logical deletes awaiting physical reclamation).  The clone is"]
    #[doc = " taken under a QSBR epoch for pointer safety."]
    #[doc = ""]
    #[doc = " # Examples"]
    #[doc = ""]
    #[doc = " ```"]
    #[doc = " let worker = WorkerState::new();"]
    #[doc = " let wl = col.get_waitlist_snapshot(&worker);"]
    #[doc = " // wl is an independent Vec<usize>"]
    #[doc = " ```"]
    pub fn get_waitlist_snapshot(&self, worker: &WorkerState) -> Vec<usize> {
        worker.enter();
        let wl = load_clone(&self.waitlist);
        worker.leave();
        wl
    }
    #[doc = " Returns the number of rows currently in the column (including invalid"]
    #[doc = " slots), entering and leaving a QSBR epoch around the read."]
    #[doc = ""]
    #[doc = " # Examples"]
    #[doc = ""]
    #[doc = " ```"]
    #[doc = " let worker = WorkerState::new();"]
    #[doc = " assert_eq!(col.data_len(&worker), 0);"]
    #[doc = " ```"]
    pub fn data_len(&self, worker: &WorkerState) -> usize {
        worker.enter();
        let data = load_ref(&self.data);
        let len = data.len();
        worker.leave();
        len
    }
    #[doc = " Enters a QSBR epoch once and calls `f` with a reference to the"]
    #[doc = " current [`ColumnData<T>`], then leaves the epoch."]
    #[doc = ""]
    #[doc = " Use this method when you need to perform **multiple reads** from the"]
    #[doc = " same snapshot in a single operation — it is cheaper than calling"]
    #[doc = " [`get_element`](Self::get_element) repeatedly, because only one"]
    #[doc = " `enter`/`leave` pair is incurred regardless of how many fields of"]
    #[doc = " the data are accessed inside `f`."]
    #[doc = ""]
    #[doc = " # Examples"]
    #[doc = ""]
    #[doc = " ```"]
    #[doc = " let worker = WorkerState::new();"]
    #[doc = " let sum = col.with_data(&worker, |data| {"]
    #[doc = "     data.iter().flatten().sum::<u32>()"]
    #[doc = " });"]
    #[doc = " ```"]
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
    #[doc = " Calls `f` with a reference to the current [`ColumnData<T>`] without"]
    #[doc = " managing the QSBR epoch."]
    #[doc = ""]
    #[doc = " **QSBR contract**: the caller **must** already be inside a"]
    #[doc = " QSBR-pinned region.  This is the pinned-region counterpart of"]
    #[doc = " [`with_data`](Self::with_data) and is appropriate when several"]
    #[doc = " `ColumnArray` accesses are batched inside a single epoch."]
    #[doc = ""]
    #[doc = " # Examples"]
    #[doc = ""]
    #[doc = " ```"]
    #[doc = " worker.enter();"]
    #[doc = " let count = col.with_data_pinned(|data| data.len());"]
    #[doc = " worker.leave();"]
    #[doc = " ```"]
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
#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::qsbr::QsbrManager;
    use crate::core::rcu::{load_clone, swap_ptr};
    #[test]
    fn test_column_array_insertion() {
        use crate::core::atomic::AtomicPtr;
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
    #[test]
    fn test_column_data_basics() {
        let mut cd = ColumnData::<u32>::new();
        assert!(cd.is_empty());
        assert_eq!(cd.len(), 0);
        cd.push(10);
        cd.push(20);
        cd.push(30);
        assert_eq!(cd.len(), 3);
        assert!(!cd.is_empty());
        assert_eq!(cd.get(0), Some(&10));
        assert_eq!(cd.get(1), Some(&20));
        assert_eq!(cd.get(2), Some(&30));
        assert_eq!(cd.get(3), None);
        assert!(cd.is_valid(0));
        assert!(cd.is_valid(1));
        assert!(cd.is_valid(2));
        assert!(!cd.is_valid(3));
        assert!(!cd.is_valid(1000));
    }
    #[test]
    fn test_column_data_set_valid() {
        let mut cd = ColumnData::<u32>::new();
        cd.push(10);
        cd.push(20);
        cd.push(30);
        cd.set_valid(1, false);
        assert!(!cd.is_valid(1));
        assert_eq!(cd.get(1), None);
        cd.set_valid(1, true);
        assert!(cd.is_valid(1));
        assert_eq!(cd.get(1), Some(&20));
        cd.set_valid(200, true);
        assert!(cd.is_valid(200));
        cd.set_valid(200, false);
        assert!(!cd.is_valid(200));
    }
    #[test]
    fn test_column_data_set() {
        let mut cd = ColumnData::<u32>::new();
        cd.set(5, 42);
        assert_eq!(cd.len(), 6);
        assert_eq!(cd.get(5), Some(&42));
        assert!(!cd.is_valid(0));
        assert!(!cd.is_valid(4));
        cd.set(5, 99);
        assert_eq!(cd.get(5), Some(&99));
    }
    #[test]
    fn test_column_data_iter_skips_invalid() {
        let mut cd = ColumnData::<u32>::new();
        cd.push(10);
        cd.push(20);
        cd.push(30);
        cd.set_valid(1, false);
        let items: Vec<Option<&u32>> = cd.iter().collect();
        assert_eq!(items.len(), 3);
        assert_eq!(items[0], Some(&10));
        assert_eq!(items[1], None);
        assert_eq!(items[2], Some(&30));
    }
    #[test]
    fn test_column_data_iter_empty() {
        let cd = ColumnData::<String>::new();
        let items: Vec<Option<&String>> = cd.iter().collect();
        assert!(items.is_empty());
    }
    #[test]
    fn test_column_data_default() {
        let cd = ColumnData::<u32>::default();
        assert!(cd.is_empty());
    }
    #[test]
    fn test_columns_new_and_default() {
        let cols = Columns::<1024>::new();
        assert!(cols.str_cols.is_empty());
        assert!(cols.int_cols.is_empty());
        assert!(cols.blob_cols.is_empty());
        let cols2 = Columns::<1024>::default();
        assert!(cols2.str_cols.is_empty());
    }
    #[test]
    fn test_column_array_get_element_pinned() {
        use crate::core::atomic::AtomicPtr;
        let workers = Arc::new(AtomicPtr::new(core::ptr::null_mut()));
        let mut qsbr = QsbrManager::new(workers);
        let col = ColumnArray::<u32, 1024>::new();
        col.acquire_lock();
        let mut next = load_clone(&col.data);
        next.push(100);
        next.push(200);
        let old = swap_ptr(&col.data, next);
        qsbr.defer_free(old);
        col.release_lock();
        assert_eq!(col.get_element_pinned(0), Some(100));
        assert_eq!(col.get_element_pinned(1), Some(200));
        assert_eq!(col.get_element_pinned(2), None);
    }
    #[test]
    fn test_column_array_with_element_pinned() {
        use crate::core::atomic::AtomicPtr;
        let workers = Arc::new(AtomicPtr::new(core::ptr::null_mut()));
        let mut qsbr = QsbrManager::new(workers);
        let col = ColumnArray::<String, 1024>::new();
        col.acquire_lock();
        let mut next = load_clone(&col.data);
        next.push("hello".into());
        next.push("world".into());
        let old = swap_ptr(&col.data, next);
        qsbr.defer_free(old);
        col.release_lock();
        let len = col.with_element_pinned(0, |s| s.len());
        assert_eq!(len, Some(5));
        let none_result: Option<usize> = col.with_element_pinned(10, |s| s.len());
        assert_eq!(none_result, None);
    }
    #[test]
    fn test_column_array_with_element() {
        use crate::core::atomic::AtomicPtr;
        let workers_ptr = Arc::new(AtomicPtr::new(core::ptr::null_mut()));
        let mut qsbr = QsbrManager::new(workers_ptr);
        let col = ColumnArray::<u32, 1024>::new();
        col.acquire_lock();
        let mut next = load_clone(&col.data);
        next.push(42);
        let old = swap_ptr(&col.data, next);
        qsbr.defer_free(old);
        col.release_lock();
        let worker = crate::core::qsbr::WorkerState::new();
        let doubled = col.with_element(0, &worker, |v| *v * 2);
        assert_eq!(doubled, Some(84));
        let missing = col.with_element(5, &worker, |v| *v * 2);
        assert_eq!(missing, None);
    }
    #[test]
    fn test_column_array_get_element_with_worker() {
        use crate::core::atomic::AtomicPtr;
        let workers_ptr = Arc::new(AtomicPtr::new(core::ptr::null_mut()));
        let mut qsbr = QsbrManager::new(workers_ptr);
        let col = ColumnArray::<u32, 1024>::new();
        col.acquire_lock();
        let mut next = load_clone(&col.data);
        next.push(77);
        let old = swap_ptr(&col.data, next);
        qsbr.defer_free(old);
        col.release_lock();
        let worker = crate::core::qsbr::WorkerState::new();
        assert_eq!(col.get_element(0, &worker), Some(77));
        assert_eq!(col.get_element(1, &worker), None);
    }
    #[test]
    fn test_column_array_with_data() {
        use crate::core::atomic::AtomicPtr;
        let workers_ptr = Arc::new(AtomicPtr::new(core::ptr::null_mut()));
        let mut qsbr = QsbrManager::new(workers_ptr);
        let col = ColumnArray::<u32, 1024>::new();
        col.acquire_lock();
        let mut next = load_clone(&col.data);
        next.push(1);
        next.push(2);
        next.push(3);
        let old = swap_ptr(&col.data, next);
        qsbr.defer_free(old);
        col.release_lock();
        let worker = crate::core::qsbr::WorkerState::new();
        let sum = col.with_data(&worker, |data| data.iter().flatten().sum::<u32>());
        assert_eq!(sum, 6);
    }
    #[test]
    fn test_column_array_with_data_pinned() {
        use crate::core::atomic::AtomicPtr;
        let workers_ptr = Arc::new(AtomicPtr::new(core::ptr::null_mut()));
        let mut qsbr = QsbrManager::new(workers_ptr);
        let col = ColumnArray::<u32, 1024>::new();
        col.acquire_lock();
        let mut next = load_clone(&col.data);
        next.push(10);
        next.push(20);
        let old = swap_ptr(&col.data, next);
        qsbr.defer_free(old);
        col.release_lock();
        let count = col.with_data_pinned(|data| data.len());
        assert_eq!(count, 2);
    }
    #[test]
    fn test_column_array_get_data_snapshot() {
        use crate::core::atomic::AtomicPtr;
        let workers_ptr = Arc::new(AtomicPtr::new(core::ptr::null_mut()));
        let mut qsbr = QsbrManager::new(workers_ptr);
        let col = ColumnArray::<u32, 1024>::new();
        col.acquire_lock();
        let mut next = load_clone(&col.data);
        next.push(5);
        next.push(15);
        let old = swap_ptr(&col.data, next);
        qsbr.defer_free(old);
        col.release_lock();
        let worker = crate::core::qsbr::WorkerState::new();
        let snapshot = col.get_data_snapshot(&worker);
        assert_eq!(snapshot.len(), 2);
        assert_eq!(snapshot.get(0), Some(&5));
        assert_eq!(snapshot.get(1), Some(&15));
    }
    #[test]
    fn test_column_array_get_waitlist_snapshot() {
        use crate::core::atomic::AtomicPtr;
        let workers_ptr = Arc::new(AtomicPtr::new(core::ptr::null_mut()));
        let _qsbr = QsbrManager::new(workers_ptr);
        let col = ColumnArray::<u32, 1024>::new();
        let worker = crate::core::qsbr::WorkerState::new();
        let wl = col.get_waitlist_snapshot(&worker);
        assert!(wl.is_empty());
    }
    #[test]
    fn test_column_array_data_len() {
        use crate::core::atomic::AtomicPtr;
        let workers_ptr = Arc::new(AtomicPtr::new(core::ptr::null_mut()));
        let mut qsbr = QsbrManager::new(workers_ptr);
        let col = ColumnArray::<u32, 1024>::new();
        let worker = crate::core::qsbr::WorkerState::new();
        assert_eq!(col.data_len(&worker), 0);
        col.acquire_lock();
        let mut next = load_clone(&col.data);
        next.push(1);
        let old = swap_ptr(&col.data, next);
        qsbr.defer_free(old);
        col.release_lock();
        assert_eq!(col.data_len(&worker), 1);
    }
    #[test]
    fn test_column_array_default() {
        let col = ColumnArray::<u32, 1024>::default();
        assert_eq!(col.get_element_pinned(0), None);
    }
    #[test]
    #[should_panic(expected = "Data Race Detected")]
    fn test_column_array_double_lock_panics() {
        let col = ColumnArray::<u32, 1024>::new();
        col.acquire_lock();
        col.acquire_lock();
    }
}
