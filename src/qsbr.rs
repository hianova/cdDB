use crate::sync::atomic::{AtomicUsize, AtomicPtr, Ordering};
use alloc::sync::Arc;
use alloc::vec::Vec;
use crate::unsafe_core::GarbageEntry;

/// A singly-linked list node that wraps a single [`WorkerState`].
///
/// [`QsbrManager`] maintains a lock-free linked list of `WorkerNode`s, one per
/// registered reader thread. Each node holds a shared reference to its thread's
/// [`WorkerState`] so the manager can inspect that thread's current epoch
/// without requiring a lock.
pub struct WorkerNode {
    /// The epoch state belonging to the reader thread that owns this node.
    ///
    /// Shared via [`Arc`] so both the owning thread and the manager can access
    /// it without copying.
    pub worker: Arc<WorkerState>,

    /// Pointer to the next node in the intrusive linked list, or null if this
    /// is the tail.
    ///
    /// Stored as an [`AtomicPtr`] so the list can be traversed and modified
    /// concurrently without a mutex.
    pub next: AtomicPtr<WorkerNode>,
}

/// The global logical clock shared by all threads.
///
/// `GLOBAL_EPOCH` is a monotonically increasing counter that [`QsbrManager`]
/// increments during each maintenance cycle. Reader threads stamp their
/// [`WorkerState::local_epoch`] with this value when they enter a critical
/// section, allowing the manager to determine the oldest epoch any active
/// reader may still be observing.
///
/// The initial value is `1`; a [`WorkerState::local_epoch`] of `0` means the
/// thread is **outside** any critical section.
pub static GLOBAL_EPOCH: core::sync::atomic::AtomicUsize = core::sync::atomic::AtomicUsize::new(1);

/// Per-thread epoch state used for QSBR quiescent-state detection.
///
/// Each reader thread owns one `WorkerState` (typically wrapped in an [`Arc`]
/// and registered with [`QsbrManager`]). The thread calls [`enter`] before
/// reading shared data and [`leave`] when it is done. The manager reads
/// [`local_epoch`] to determine whether this thread is still inside a critical
/// section that began at or before a given epoch.
///
/// [`enter`]: WorkerState::enter
/// [`leave`]: WorkerState::leave
/// [`local_epoch`]: WorkerState::local_epoch
pub struct WorkerState {
    /// The epoch at which this thread most recently entered a critical section.
    ///
    /// - A value of `0` means the thread is currently **outside** any critical
    ///   section (quiescent state).
    /// - Any non-zero value `e` means the thread entered a critical section
    ///   when [`GLOBAL_EPOCH`] was `e` and has not yet called
    ///   [`WorkerState::leave`].
    pub local_epoch: AtomicUsize,
}

impl Default for WorkerState {
    fn default() -> Self {
        Self::new()
    }
}

impl WorkerState {
    /// Creates a new `WorkerState` with `local_epoch` initialised to `0`,
    /// indicating that the thread starts outside any critical section.
    pub fn new() -> Self {
        Self {
            local_epoch: AtomicUsize::new(0),
        }
    }

    /// Enters a read-side critical section.
    ///
    /// Stamps [`local_epoch`] with the current value of [`GLOBAL_EPOCH`] using
    /// an `Acquire` load / `Release` store pair, signalling to
    /// [`QsbrManager::maintenance`] that this thread is now actively observing
    /// shared data at the current epoch and must not have that data reclaimed
    /// beneath it.
    ///
    /// # Examples
    ///
    /// ```rust,ignore
    /// let state = WorkerState::new();
    /// state.enter();
    /// // … read shared data …
    /// state.leave();
    /// ```
    ///
    /// [`local_epoch`]: WorkerState::local_epoch
    #[inline(always)]
    pub fn enter(&self) {
        let global = GLOBAL_EPOCH.load(Ordering::Acquire);
        self.local_epoch.store(global, Ordering::Release);
    }

    /// Leaves the read-side critical section (quiescent state).
    ///
    /// Stores `0` into [`local_epoch`] with `Release` ordering, signalling to
    /// [`QsbrManager::maintenance`] that this thread is no longer holding a
    /// reference to any epoch of shared data. Once **all** registered threads
    /// have passed through a quiescent state, the manager may safely reclaim
    /// any garbage enqueued before or during the previous epoch.
    ///
    /// [`local_epoch`]: WorkerState::local_epoch
    #[inline(always)]
    pub fn leave(&self) {
        self.local_epoch.store(0, Ordering::Release);
    }
}

/// QSBR (Quiescent-State-Based Reclamation) manager.
///
/// `QsbrManager` owns two resources:
///
/// 1. **Worker list** — a shared, lock-free linked list of [`WorkerNode`]s,
///    one per registered reader thread. The manager walks this list during
///    [`maintenance`] to find the minimum epoch still visible to any active
///    reader.
///
/// 2. **Garbage queue** — a [`Vec`] of [`GarbageEntry`] values, each carrying
///    a raw pointer and the epoch at which it was retired. Entries whose epoch
///    is strictly less than the minimum active epoch are safe to drop because
///    no reader can still hold a reference to them.
///
/// [`maintenance`]: QsbrManager::maintenance
pub struct QsbrManager {
    /// Shared pointer to the head of the [`WorkerNode`] linked list.
    ///
    /// Wrapped in [`Arc`] so that both the manager and the subsystem that
    /// registers new workers can refer to the same list head without copying.
    workers: Arc<AtomicPtr<WorkerNode>>,

    /// Deferred garbage waiting to be reclaimed.
    ///
    /// Each [`GarbageEntry`] records a raw pointer and the [`GLOBAL_EPOCH`]
    /// value at the time it was retired. Entries are dropped (and their
    /// underlying memory freed) by [`QsbrManager::maintenance`] once it is
    /// safe to do so.
    garbage: Vec<GarbageEntry>,
}

impl QsbrManager {
    /// Creates a new `QsbrManager` that tracks the worker list rooted at
    /// `workers`.
    ///
    /// # Arguments
    ///
    /// * `workers` — a shared [`Arc`] pointing to the atomic head pointer of
    ///   the [`WorkerNode`] linked list. The same `Arc` should be held by
    ///   whatever subsystem registers new reader threads so that nodes inserted
    ///   after construction are immediately visible to this manager.
    pub fn new(workers: Arc<AtomicPtr<WorkerNode>>) -> Self {
        Self {
            workers,
            garbage: Vec::new(),
        }
    }

    /// Registers a raw pointer for deferred reclamation.
    ///
    /// The pointed-to value will be dropped (and its memory freed via
    /// [`GarbageEntry`]'s [`Drop`] implementation) during a future call to
    /// [`QsbrManager::maintenance`] once all currently active readers have
    /// passed through a quiescent state.
    ///
    /// # Arguments
    ///
    /// * `ptr` — a raw, exclusively-owned pointer to the value to be retired.
    ///   The caller must ensure it holds **exclusive ownership** of `*ptr` and
    ///   will not access it again after this call.
    ///
    /// # No-op on null
    ///
    /// If `ptr` is null the function returns immediately without pushing
    /// anything onto the garbage queue.
    ///
    /// # Safety
    ///
    /// The caller must guarantee that:
    /// - `ptr` was allocated in a manner compatible with [`GarbageEntry`]'s
    ///   drop logic (i.e. allocated via [`Box`] or an equivalent heap
    ///   allocator).
    /// - No other live reference to `*ptr` exists or will be created after
    ///   this call.
    pub fn defer_free<T>(&mut self, ptr: *mut T) {
        if ptr.is_null() { return; }

        self.garbage.push(GarbageEntry::new(
            ptr,
            GLOBAL_EPOCH.load(Ordering::Relaxed),
        ));
    }

    /// Advances the global epoch and reclaims garbage that is no longer
    /// visible to any active reader.
    ///
    /// This method performs three steps:
    ///
    /// 1. **Increment [`GLOBAL_EPOCH`]** — signals to reader threads that a
    ///    new epoch has begun.
    ///
    /// 2. **Find the minimum active epoch** — walks the [`WorkerNode`] list
    ///    and collects the smallest non-zero `local_epoch` value across all
    ///    currently active (i.e. inside a critical section) threads. If every
    ///    thread is quiescent the minimum stays equal to the newly incremented
    ///    global epoch, which triggers a second increment to push reclamation
    ///    forward, allowing all pending garbage to be collected.
    ///
    /// 3. **Reclaim eligible garbage** — retains only those [`GarbageEntry`]
    ///    values whose recorded epoch is ≥ the minimum active epoch. Entries
    ///    that are not retained are dropped in place, running their [`Drop`]
    ///    implementation and freeing the associated memory.
    ///
    /// # Safety
    ///
    /// This method calls [`crate::unsafe_core::load_node`] internally, which
    /// dereferences raw pointers. The caller must ensure the `workers` list is
    /// always in a consistent, pointer-valid state while `maintenance` runs.
    pub fn maintenance(&mut self) {
        // 1. 推進全域時鐘
        GLOBAL_EPOCH.fetch_add(1, Ordering::Relaxed);

        let current_global = GLOBAL_EPOCH.load(Ordering::Acquire);
        let mut min_epoch = current_global;

        let mut curr_ptr = self.workers.load(Ordering::Acquire);
        while let Some(node) = unsafe { crate::unsafe_core::load_node(curr_ptr) } {
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
