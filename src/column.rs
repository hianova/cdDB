use crate::AHashMap;
use alloc::sync::Arc;
use alloc::vec::Vec;
use alloc::string::String;
use crate::sync::atomic::{AtomicBool, AtomicPtr, Ordering};
use crate::unsafe_core::{load_clone, load_ref, new_atomic_ptr};
use crate::qsbr::WorkerState;

/// A typed collection of named columnar arrays, grouped by element type.
///
/// `Columns<N>` is the top-level container that maps column names to their
/// corresponding [`ColumnArray`] instances.  Three column families are
/// supported out of the box:
///
/// * **str_cols** — UTF-8 string columns
/// * **int_cols** — 32-bit unsigned integer columns
/// * **blob_cols** — arbitrary byte-blob columns
///
/// The const generic `N` is forwarded to every [`ColumnArray`] and controls
/// the internal chunking granularity.
#[derive(Clone)]
pub struct Columns<const N: usize> {
    /// Named columns whose values are UTF-8 [`String`]s.
    pub str_cols: AHashMap<String, Arc<ColumnArray<String, N>>>,
    /// Named columns whose values are unsigned 32-bit integers ([`u32`]).
    pub int_cols: AHashMap<String, Arc<ColumnArray<u32, N>>>,
    /// Named columns whose values are raw byte blobs ([`Vec<u8>`]).
    pub blob_cols: AHashMap<String, Arc<ColumnArray<Vec<u8>, N>>>,
}

impl<const N: usize> Default for Columns<N> {
    fn default() -> Self {
        Self::new()
    }
}

impl<const N: usize> Columns<N> {
    /// Creates a new, empty [`Columns`] container with no columns registered.
    ///
    /// All three column families (`str_cols`, `int_cols`, `blob_cols`) start
    /// as empty hash-maps.
    ///
    /// # Examples
    ///
    /// ```
    /// let cols = Columns::<1024>::new();
    /// assert!(cols.str_cols.is_empty());
    /// ```
    pub fn new() -> Self {
        Self {
            str_cols: AHashMap::default(),
            int_cols: AHashMap::default(),
            blob_cols: AHashMap::default(),
        }
    }
}

/// Contiguous columnar storage for values of type `T`, paired with a
/// compact validity bitvector.
///
/// Each logical row maps to one element in `data` at the same index.  Whether
/// that element holds a meaningful value is tracked by `valid`, a packed array
/// of 64-bit words where bit `i` of word `i / 64` corresponds to row `i`.
/// A set bit means the slot is valid (non-null); a cleared bit means it is
/// absent (null).
///
/// `ColumnData<T>` is the snapshot type returned by
/// [`ColumnArray::get_data_snapshot`] and the underlying value swapped
/// atomically by writers.
#[derive(Clone)]
pub struct ColumnData<T> {
    /// The raw row values stored contiguously.  Slots whose validity bit is
    /// cleared may contain stale or default-initialised data and **must not**
    /// be read without first checking [`ColumnData::is_valid`].
    pub data: Vec<T>,
    /// Packed validity bitvector.  Each `u64` word covers 64 consecutive rows.
    /// Bit `idx % 64` of word `idx / 64` is set when row `idx` is valid.
    pub valid: Vec<u64>,
}

impl<T: Default + Clone> Default for ColumnData<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T: Default + Clone> ColumnData<T> {
    /// Creates a new, empty [`ColumnData`] with no rows.
    ///
    /// Both the data vector and the validity bitvector start empty.
    ///
    /// # Examples
    ///
    /// ```
    /// let cd = ColumnData::<u32>::new();
    /// assert!(cd.is_empty());
    /// ```
    pub fn new() -> Self {
        Self {
            data: Vec::new(),
            valid: Vec::new(),
        }
    }

    /// Returns `true` if row `idx` is marked as valid (non-null).
    ///
    /// Rows that have never been written, or that have been explicitly
    /// invalidated with [`set_valid`](Self::set_valid), return `false`.
    /// Indices beyond the current bitvector range are always `false`.
    ///
    /// # Examples
    ///
    /// ```
    /// let mut cd = ColumnData::<u32>::new();
    /// cd.push(42);
    /// assert!(cd.is_valid(0));
    /// assert!(!cd.is_valid(1)); // out of range
    /// ```
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

    /// Sets or clears the validity bit for row `idx`.
    ///
    /// If `idx` falls in a bitvector word that does not yet exist, the
    /// `valid` vector is grown and zero-filled up to the required word.
    ///
    /// # Examples
    ///
    /// ```
    /// let mut cd = ColumnData::<u32>::new();
    /// cd.push(10);
    /// cd.set_valid(0, false); // logically delete the row
    /// assert!(!cd.is_valid(0));
    /// cd.set_valid(0, true);  // restore it
    /// assert!(cd.is_valid(0));
    /// ```
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

    /// Returns a reference to the value at row `idx`, or `None` if the row
    /// is invalid or `idx` is out of bounds.
    ///
    /// This is the safe, validity-checked accessor.  It is equivalent to
    /// calling [`is_valid`](Self::is_valid) followed by indexing into
    /// `self.data`.
    ///
    /// # Examples
    ///
    /// ```
    /// let mut cd = ColumnData::<u32>::new();
    /// cd.push(99);
    /// assert_eq!(cd.get(0), Some(&99));
    /// assert_eq!(cd.get(1), None); // out of bounds
    /// ```
    #[inline(always)]
    pub fn get(&self, idx: usize) -> Option<&T> {
        if self.is_valid(idx) {
            self.data.get(idx)
        } else {
            None
        }
    }

    /// Returns the total number of rows allocated in the data vector,
    /// **including** any invalid (null) slots.
    ///
    /// # Examples
    ///
    /// ```
    /// let mut cd = ColumnData::<u32>::new();
    /// assert_eq!(cd.len(), 0);
    /// cd.push(1);
    /// assert_eq!(cd.len(), 1);
    /// ```
    pub fn len(&self) -> usize {
        self.data.len()
    }

    /// Returns `true` if the data vector contains no rows.
    ///
    /// # Examples
    ///
    /// ```
    /// let cd = ColumnData::<u32>::new();
    /// assert!(cd.is_empty());
    /// ```
    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    /// Appends `val` as a new valid row at the end of the column.
    ///
    /// The validity bit for the new row is automatically set to `true`.
    ///
    /// # Examples
    ///
    /// ```
    /// let mut cd = ColumnData::<u32>::new();
    /// cd.push(7);
    /// assert_eq!(cd.get(0), Some(&7));
    /// ```
    pub fn push(&mut self, val: T) {
        let idx = self.data.len();
        self.data.push(val);
        self.set_valid(idx, true);
    }

    /// Writes `val` to row `idx` and marks that row as valid.
    ///
    /// If `idx` is beyond the current length of the data vector, the vector
    /// is grown with [`T::default()`](Default::default) fill values.  The
    /// intermediate rows created by the resize are **not** marked valid.
    ///
    /// # Examples
    ///
    /// ```
    /// let mut cd = ColumnData::<u32>::new();
    /// cd.set(5, 42); // rows 0-4 are allocated but invalid
    /// assert_eq!(cd.len(), 6);
    /// assert_eq!(cd.get(5), Some(&42));
    /// assert!(!cd.is_valid(0)); // gap rows are not valid
    /// ```
    pub fn set(&mut self, idx: usize, val: T) {
        if idx >= self.data.len() {
            self.data.resize(idx + 1, T::default());
        }
        self.data[idx] = val;
        self.set_valid(idx, true);
    }

    /// Returns an iterator over all rows, yielding `Option<&T>`.
    ///
    /// Each call to [`Iterator::next`] advances by one row.  Valid rows
    /// produce `Some(&value)`; invalid (null) rows produce `None`.
    /// The iterator always yields exactly [`len`](Self::len) items.
    ///
    /// See also [`ColumnDataIter`].
    ///
    /// # Examples
    ///
    /// ```
    /// let mut cd = ColumnData::<u32>::new();
    /// cd.push(1);
    /// cd.push(2);
    /// cd.set_valid(0, false); // mark first row invalid
    /// let items: Vec<_> = cd.iter().collect();
    /// assert_eq!(items, vec![None, Some(&2)]);
    /// ```
    pub fn iter(&self) -> ColumnDataIter<'_, T> {
        ColumnDataIter { col: self, idx: 0 }
    }
}

/// An iterator over the rows of a [`ColumnData<T>`].
///
/// Created by [`ColumnData::iter`].  Each call to [`Iterator::next`] returns
/// `Some(Option<&T>)` until all rows have been visited, then `None`.
///
/// * `Some(Some(&value))` — the row is valid and contains `value`.
/// * `Some(None)`         — the row exists but is invalid (null).
/// * `None`               — the iterator is exhausted.
pub struct ColumnDataIter<'a, T> {
    /// The [`ColumnData`] being iterated.
    col: &'a ColumnData<T>,
    /// The index of the next row to yield.
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

/// The core wait-free columnar storage structure.
///
/// `ColumnArray<T, N>` is the lowest-level building block of the database's
/// column-oriented (Data-Oriented Design) storage layer.  It manages a
/// heap-allocated [`ColumnData<T>`] behind an [`AtomicPtr`], enabling
/// **wait-free reads** via Quiescent-State-Based Reclamation (QSBR):
///
/// * **Readers** enter a QSBR-pinned region, load the pointer, perform their
///   work, and leave — all without taking any lock.
/// * **Writers** must call [`acquire_lock`](Self::acquire_lock) first, clone
///   the current data, mutate the clone, atomically swap the pointer, then
///   call [`release_lock`](Self::release_lock).  The old pointer is deferred
///   to the QSBR manager for safe reclamation once all readers have left.
///
/// The const generic `N` is reserved for future chunking/segmentation and is
/// currently forwarded through the type system.
///
/// A companion [`waitlist`](Self::waitlist) pointer tracks row indices that
/// are pending some deferred operation (e.g. delete tombstones).
pub struct ColumnArray<T, const N: usize> {
    /// Atomically-swapped pointer to the current [`ColumnData<T>`] snapshot.
    ///
    /// Readers load this with `Ordering::Acquire` (via [`load_ref`]) while
    /// inside a QSBR-pinned region.  Writers swap it with a freshly built
    /// clone after acquiring [`write_guard`](Self::write_guard).
    pub data: AtomicPtr<ColumnData<T>>,
    /// Atomically-swapped pointer to the pending-operation waitlist.
    ///
    /// The waitlist is a `Vec<usize>` of row indices that have been logically
    /// deleted (or otherwise flagged) but not yet physically reclaimed.
    pub waitlist: AtomicPtr<Vec<usize>>,
    /// Spin-lock flag that serialises concurrent writers.
    ///
    /// `true` means a writer currently holds the lock.  Attempting to acquire
    /// the lock while it is already held panics with a "Data Race Detected"
    /// message — see [`acquire_lock`](Self::acquire_lock).
    pub(crate) write_guard: AtomicBool,
}

impl<T: Default + Clone, const N: usize> ColumnArray<T, N> {
    /// Creates a new [`ColumnArray`] with an empty [`ColumnData`] and an
    /// empty waitlist, both heap-allocated and owned by atomic pointers.
    ///
    /// The write guard is initialised to `false` (unlocked).
    ///
    /// # Examples
    ///
    /// ```
    /// let col = ColumnArray::<u32, 1024>::new();
    /// assert_eq!(col.get_element_pinned(0), None);
    /// ```
    pub fn new() -> Self {
        Self {
            data: new_atomic_ptr(ColumnData::new()),
            waitlist: new_atomic_ptr(Vec::new()),
            write_guard: AtomicBool::new(false),
        }
    }

    /// Acquires the exclusive write lock for this column.
    ///
    /// Uses an atomic swap on [`write_guard`](Self::write_guard) with
    /// `SeqCst` ordering to ensure all prior memory operations are visible
    /// before the lock is considered held.
    ///
    /// After acquiring the lock, the caller must:
    /// 1. Clone the current data via [`get_data_snapshot`](Self::get_data_snapshot)
    ///    or a direct `load_clone`.
    /// 2. Mutate the clone.
    /// 3. Atomically swap the new data pointer in.
    /// 4. Defer the old pointer to the QSBR manager for safe reclamation.
    /// 5. Call [`release_lock`](Self::release_lock) when done.
    ///
    /// # Panics
    ///
    /// Panics immediately with `"Data Race Detected: Multiple writers
    /// attempted to access the same ColumnArray!"` if the lock is already
    /// held.  This enforces the single-writer invariant at runtime.
    pub fn acquire_lock(&self) {
        if self.write_guard.swap(true, Ordering::SeqCst) {
            panic!(
                "Data Race Detected: Multiple writers attempted to access the same ColumnArray!"
            );
        }
    }

    /// Releases the exclusive write lock, making the column available for
    /// subsequent writers.
    ///
    /// This performs a `SeqCst` store of `false` to [`write_guard`](Self::write_guard).
    /// It should always be called after [`acquire_lock`](Self::acquire_lock),
    /// even if the write operation encounters an error.
    pub fn release_lock(&self) {
        self.write_guard.store(false, Ordering::SeqCst);
    }

    /// Returns a clone of the element at row `idx`, or `None` if the row is
    /// invalid or `idx` is out of bounds.
    ///
    /// This method manages its own QSBR epoch: it calls `worker.enter()`,
    /// reads the element, then calls `worker.leave()` before returning.  Use
    /// [`get_element_pinned`](Self::get_element_pinned) when the caller is
    /// already inside a pinned region.
    ///
    /// # Examples
    ///
    /// ```
    /// let worker = WorkerState::new();
    /// let col = ColumnArray::<u32, 1024>::new();
    /// assert_eq!(col.get_element(0, &worker), None);
    /// ```
    pub fn get_element(&self, idx: usize, worker: &WorkerState) -> Option<T> {
        worker.enter();
        let data = load_ref(&self.data);
        let val = data.get(idx).cloned();
        worker.leave();
        val
    }

    /// Returns a clone of the element at row `idx`, or `None` if the row is
    /// invalid or `idx` is out of bounds.
    ///
    /// **QSBR contract**: the caller **must** already be inside a
    /// QSBR-pinned region (i.e. `worker.enter()` has been called and
    /// `worker.leave()` has *not* yet been called).  This method does not
    /// call `enter`/`leave` itself, making it cheaper for hot paths that
    /// batch multiple reads inside a single pinned region.
    ///
    /// # Examples
    ///
    /// ```
    /// // Caller is responsible for the QSBR epoch:
    /// worker.enter();
    /// let val = col.get_element_pinned(0);
    /// worker.leave();
    /// ```
    #[inline(always)]
    pub fn get_element_pinned(&self, idx: usize) -> Option<T> {
        let data = load_ref(&self.data);
        data.get(idx).cloned()
    }

    /// Applies the closure `f` to a reference of the element at row `idx`,
    /// returning `Some(R)` if the row is valid, or `None` otherwise.
    ///
    /// This method manages its own QSBR epoch (`worker.enter()` /
    /// `worker.leave()`), so the reference passed to `f` is guaranteed to
    /// be live for the duration of the call.  Prefer
    /// [`with_element_pinned`](Self::with_element_pinned) when already inside
    /// a pinned region.
    ///
    /// # Examples
    ///
    /// ```
    /// let worker = WorkerState::new();
    /// let length = col.with_element(0, &worker, |s: &String| s.len());
    /// ```
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

    /// Applies the closure `f` to a reference of the element at row `idx`,
    /// returning `Some(R)` if the row is valid, or `None` otherwise.
    ///
    /// **QSBR contract**: the caller **must** already be inside a
    /// QSBR-pinned region.  This method skips the `enter`/`leave` calls,
    /// making it the preferred choice when batching multiple element accesses
    /// inside a single pinned region.
    ///
    /// # Examples
    ///
    /// ```
    /// worker.enter();
    /// let len = col.with_element_pinned(0, |s: &String| s.len());
    /// worker.leave();
    /// ```
    #[inline(always)]
    pub fn with_element_pinned<F, R>(&self, idx: usize, f: F) -> Option<R>
    where
        F: FnOnce(&T) -> R,
    {
        let data = load_ref(&self.data);
        data.get(idx).map(f)
    }

    /// Returns an owned deep clone of the current [`ColumnData<T>`] snapshot.
    ///
    /// The clone is taken while holding a QSBR epoch (via `worker.enter()` /
    /// `worker.leave()`), ensuring the pointer remains valid for the duration
    /// of the clone.  The returned value is fully owned and independent of the
    /// live atomic pointer.
    ///
    /// This is the recommended way for a writer to read the current state
    /// before constructing a modified copy.
    ///
    /// # Examples
    ///
    /// ```
    /// let worker = WorkerState::new();
    /// let snapshot = col.get_data_snapshot(&worker);
    /// assert_eq!(snapshot.len(), col.data_len(&worker));
    /// ```
    pub fn get_data_snapshot(&self, worker: &WorkerState) -> ColumnData<T> {
        worker.enter();
        let data = load_clone(&self.data);
        worker.leave();
        data
    }

    /// Returns an owned clone of the current waitlist (`Vec<usize>`).
    ///
    /// The waitlist contains row indices that are pending deferred operations
    /// (e.g. logical deletes awaiting physical reclamation).  The clone is
    /// taken under a QSBR epoch for pointer safety.
    ///
    /// # Examples
    ///
    /// ```
    /// let worker = WorkerState::new();
    /// let wl = col.get_waitlist_snapshot(&worker);
    /// // wl is an independent Vec<usize>
    /// ```
    pub fn get_waitlist_snapshot(&self, worker: &WorkerState) -> Vec<usize> {
        worker.enter();
        let wl = load_clone(&self.waitlist);
        worker.leave();
        wl
    }

    /// Returns the number of rows currently in the column (including invalid
    /// slots), entering and leaving a QSBR epoch around the read.
    ///
    /// # Examples
    ///
    /// ```
    /// let worker = WorkerState::new();
    /// assert_eq!(col.data_len(&worker), 0);
    /// ```
    pub fn data_len(&self, worker: &WorkerState) -> usize {
        worker.enter();
        let data = load_ref(&self.data);
        let len = data.len();
        worker.leave();
        len
    }

    /// Enters a QSBR epoch once and calls `f` with a reference to the
    /// current [`ColumnData<T>`], then leaves the epoch.
    ///
    /// Use this method when you need to perform **multiple reads** from the
    /// same snapshot in a single operation — it is cheaper than calling
    /// [`get_element`](Self::get_element) repeatedly, because only one
    /// `enter`/`leave` pair is incurred regardless of how many fields of
    /// the data are accessed inside `f`.
    ///
    /// # Examples
    ///
    /// ```
    /// let worker = WorkerState::new();
    /// let sum = col.with_data(&worker, |data| {
    ///     data.iter().flatten().sum::<u32>()
    /// });
    /// ```
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

    /// Calls `f` with a reference to the current [`ColumnData<T>`] without
    /// managing the QSBR epoch.
    ///
    /// **QSBR contract**: the caller **must** already be inside a
    /// QSBR-pinned region.  This is the pinned-region counterpart of
    /// [`with_data`](Self::with_data) and is appropriate when several
    /// `ColumnArray` accesses are batched inside a single epoch.
    ///
    /// # Examples
    ///
    /// ```
    /// worker.enter();
    /// let count = col.with_data_pinned(|data| data.len());
    /// worker.leave();
    /// ```
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
    use crate::qsbr::QsbrManager;
    use crate::unsafe_core::{load_clone, swap_ptr};

    #[test]
    fn test_column_array_insertion() {
        use crate::sync::atomic::AtomicPtr;
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

        // get valid items
        assert_eq!(cd.get(0), Some(&10));
        assert_eq!(cd.get(1), Some(&20));
        assert_eq!(cd.get(2), Some(&30));
        // out of bounds
        assert_eq!(cd.get(3), None);

        // is_valid
        assert!(cd.is_valid(0));
        assert!(cd.is_valid(1));
        assert!(cd.is_valid(2));
        assert!(!cd.is_valid(3));
        // block boundary: is_valid for an index whose block doesn't exist
        assert!(!cd.is_valid(1000));
    }

    #[test]
    fn test_column_data_set_valid() {
        let mut cd = ColumnData::<u32>::new();
        cd.push(10);
        cd.push(20);
        cd.push(30);

        // Invalidate element at index 1
        cd.set_valid(1, false);
        assert!(!cd.is_valid(1));
        assert_eq!(cd.get(1), None);

        // Re-validate it
        cd.set_valid(1, true);
        assert!(cd.is_valid(1));
        assert_eq!(cd.get(1), Some(&20));

        // set_valid on an index that extends the valid bitvec
        cd.set_valid(200, true);
        assert!(cd.is_valid(200));
        cd.set_valid(200, false);
        assert!(!cd.is_valid(200));
    }

    #[test]
    fn test_column_data_set() {
        let mut cd = ColumnData::<u32>::new();
        // set beyond current length (auto-resizes)
        cd.set(5, 42);
        assert_eq!(cd.len(), 6);
        assert_eq!(cd.get(5), Some(&42));
        // indices 0..5 should have default data but NOT be valid (only pushed items are valid)
        assert!(!cd.is_valid(0));
        assert!(!cd.is_valid(4));

        // overwrite an existing element
        cd.set(5, 99);
        assert_eq!(cd.get(5), Some(&99));
    }

    #[test]
    fn test_column_data_iter_skips_invalid() {
        let mut cd = ColumnData::<u32>::new();
        cd.push(10);
        cd.push(20);
        cd.push(30);

        // Invalidate middle element
        cd.set_valid(1, false);

        let items: Vec<Option<&u32>> = cd.iter().collect();
        assert_eq!(items.len(), 3);
        assert_eq!(items[0], Some(&10));
        assert_eq!(items[1], None); // invalid, skipped by get()
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
        use crate::sync::atomic::AtomicPtr;
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

        // get_element_pinned
        assert_eq!(col.get_element_pinned(0), Some(100));
        assert_eq!(col.get_element_pinned(1), Some(200));
        assert_eq!(col.get_element_pinned(2), None);
    }

    #[test]
    fn test_column_array_with_element_pinned() {
        use crate::sync::atomic::AtomicPtr;
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

        // with_element_pinned
        let len = col.with_element_pinned(0, |s| s.len());
        assert_eq!(len, Some(5));
        let none_result: Option<usize> = col.with_element_pinned(10, |s| s.len());
        assert_eq!(none_result, None);
    }

    #[test]
    fn test_column_array_with_element() {
        use crate::sync::atomic::AtomicPtr;
        let workers_ptr = Arc::new(AtomicPtr::new(core::ptr::null_mut()));
        let mut qsbr = QsbrManager::new(workers_ptr);
        let col = ColumnArray::<u32, 1024>::new();
        col.acquire_lock();

        let mut next = load_clone(&col.data);
        next.push(42);
        let old = swap_ptr(&col.data, next);
        qsbr.defer_free(old);
        col.release_lock();

        let worker = crate::qsbr::WorkerState::new();
        let doubled = col.with_element(0, &worker, |v| *v * 2);
        assert_eq!(doubled, Some(84));
        let missing = col.with_element(5, &worker, |v| *v * 2);
        assert_eq!(missing, None);
    }

    #[test]
    fn test_column_array_get_element_with_worker() {
        use crate::sync::atomic::AtomicPtr;
        let workers_ptr = Arc::new(AtomicPtr::new(core::ptr::null_mut()));
        let mut qsbr = QsbrManager::new(workers_ptr);
        let col = ColumnArray::<u32, 1024>::new();
        col.acquire_lock();

        let mut next = load_clone(&col.data);
        next.push(77);
        let old = swap_ptr(&col.data, next);
        qsbr.defer_free(old);
        col.release_lock();

        let worker = crate::qsbr::WorkerState::new();
        assert_eq!(col.get_element(0, &worker), Some(77));
        assert_eq!(col.get_element(1, &worker), None);
    }

    #[test]
    fn test_column_array_with_data() {
        use crate::sync::atomic::AtomicPtr;
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

        let worker = crate::qsbr::WorkerState::new();
        let sum = col.with_data(&worker, |data| {
            data.iter().flatten().sum::<u32>()
        });
        assert_eq!(sum, 6);
    }

    #[test]
    fn test_column_array_with_data_pinned() {
        use crate::sync::atomic::AtomicPtr;
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
        use crate::sync::atomic::AtomicPtr;
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

        let worker = crate::qsbr::WorkerState::new();
        let snapshot = col.get_data_snapshot(&worker);
        assert_eq!(snapshot.len(), 2);
        assert_eq!(snapshot.get(0), Some(&5));
        assert_eq!(snapshot.get(1), Some(&15));
    }

    #[test]
    fn test_column_array_get_waitlist_snapshot() {
        use crate::sync::atomic::AtomicPtr;
        let workers_ptr = Arc::new(AtomicPtr::new(core::ptr::null_mut()));
        let _qsbr = QsbrManager::new(workers_ptr);
        let col = ColumnArray::<u32, 1024>::new();

        let worker = crate::qsbr::WorkerState::new();
        let wl = col.get_waitlist_snapshot(&worker);
        assert!(wl.is_empty());
    }

    #[test]
    fn test_column_array_data_len() {
        use crate::sync::atomic::AtomicPtr;
        let workers_ptr = Arc::new(AtomicPtr::new(core::ptr::null_mut()));
        let mut qsbr = QsbrManager::new(workers_ptr);
        let col = ColumnArray::<u32, 1024>::new();

        let worker = crate::qsbr::WorkerState::new();
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
        col.acquire_lock(); // should panic
    }
}
