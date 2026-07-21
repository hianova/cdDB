use crate::core::atomic::{AtomicPtr, AtomicUsize, Ordering};
use crate::core::rcu::GarbageEntry;
use alloc::sync::Arc;
use alloc::vec::Vec;
#[doc = " A singly-linked list node that wraps a single [`WorkerState`]."]
#[doc = ""]
#[doc = " [`QsbrManager`] maintains a lock-free linked list of `WorkerNode`s, one per"]
#[doc = " registered reader thread. Each node holds a shared reference to its thread's"]
#[doc = " [`WorkerState`] so the manager can inspect that thread's current epoch"]
#[doc = " without requiring a lock."]
#[repr(C, align(64))]
pub struct WorkerNode {
    #[doc = " The epoch state belonging to the reader thread that owns this node."]
    #[doc = ""]
    #[doc = " Shared via [`Arc`] so both the owning thread and the manager can access"]
    #[doc = " it without copying."]
    pub worker: Arc<WorkerState>,
    #[doc = " Pointer to the next node in the intrusive linked list, or null if this"]
    #[doc = " is the tail."]
    #[doc = ""]
    #[doc = " Stored as an [`AtomicPtr`] so the list can be traversed and modified"]
    #[doc = " concurrently without a mutex."]
    pub next: AtomicPtr<WorkerNode>,
}
#[doc = " The global logical clock shared by all threads."]
#[doc = ""]
#[doc = " `GLOBAL_EPOCH` is a monotonically increasing counter that [`QsbrManager`]"]
#[doc = " increments during each maintenance cycle. Reader threads stamp their"]
#[doc = " [`WorkerState::local_epoch`] with this value when they enter a critical"]
#[doc = " section, allowing the manager to determine the oldest epoch any active"]
#[doc = " reader may still be observing."]
#[doc = ""]
#[doc = " The initial value is `1`; a [`WorkerState::local_epoch`] of `0` means the"]
#[doc = " thread is **outside** any critical section."]
pub static GLOBAL_EPOCH: core::sync::atomic::AtomicUsize = core::sync::atomic::AtomicUsize::new(1);
#[doc = " Per-thread epoch state used for QSBR quiescent-state detection."]
#[doc = ""]
#[doc = " Each reader thread owns one `WorkerState` (typically wrapped in an [`Arc`]"]
#[doc = " and registered with [`QsbrManager`]). The thread calls [`enter`] before"]
#[doc = " reading shared data and [`leave`] when it is done. The manager reads"]
#[doc = " [`local_epoch`] to determine whether this thread is still inside a critical"]
#[doc = " section that began at or before a given epoch."]
#[doc = ""]
#[doc = " [`enter`]: WorkerState::enter"]
#[doc = " [`leave`]: WorkerState::leave"]
#[doc = " [`local_epoch`]: WorkerState::local_epoch"]
#[repr(C, align(64))]
pub struct WorkerState {
    #[doc = " The epoch at which this thread most recently entered a critical section."]
    #[doc = ""]
    #[doc = " - A value of `0` means the thread is currently **outside** any critical"]
    #[doc = "   section (quiescent state)."]
    #[doc = " - Any non-zero value `e` means the thread entered a critical section"]
    #[doc = "   when [`GLOBAL_EPOCH`] was `e` and has not yet called"]
    #[doc = "   [`WorkerState::leave`]."]
    pub local_epoch: AtomicUsize,
    #[doc = " Flag indicating whether this worker is considered \"offline\" (e.g., stalled)."]
    #[doc = ""]
    #[doc = " When set to `true`, the `QsbrManager` will ignore this worker's `local_epoch`"]
    #[doc = " during maintenance, allowing the global epoch to advance and memory to be"]
    #[doc = " reclaimed even if this thread is permanently stuck."]
    pub is_offline: crate::core::atomic::AtomicBool,
}
impl Default for WorkerState {
    fn default() -> Self {
        Self::new()
    }
}
impl WorkerState {
    #[doc = " Creates a new `WorkerState` with `local_epoch` initialised to `0`,"]
    #[doc = " indicating that the thread starts outside any critical section."]
    pub fn new() -> Self {
        Self {
            local_epoch: AtomicUsize::new(0),
            is_offline: crate::core::atomic::AtomicBool::new(false),
        }
    }
    #[doc = " Enters a read-side critical section."]
    #[doc = ""]
    #[doc = " Stamps [`local_epoch`] with the current value of [`GLOBAL_EPOCH`] using"]
    #[doc = " an `Acquire` load / `Release` store pair, signalling to"]
    #[doc = " [`QsbrManager::maintenance`] that this thread is now actively observing"]
    #[doc = " shared data at the current epoch and must not have that data reclaimed"]
    #[doc = " beneath it."]
    #[doc = ""]
    #[doc = " # Examples"]
    #[doc = ""]
    #[doc = " ```rust,ignore"]
    #[doc = " let state = WorkerState::new();"]
    #[doc = " state.enter();"]
    #[doc = " // … read shared data …"]
    #[doc = " state.leave();"]
    #[doc = " ```"]
    #[doc = ""]
    #[doc = " [`local_epoch`]: WorkerState::local_epoch"]
    #[inline(always)]
    pub fn enter(&self) {
        let global = GLOBAL_EPOCH.load(Ordering::Acquire);
        self.local_epoch.store(global, Ordering::Release);
    }
    #[doc = " Leaves the read-side critical section (quiescent state)."]
    #[doc = ""]
    #[doc = " Stores `0` into [`local_epoch`] with `Release` ordering, signalling to"]
    #[doc = " [`QsbrManager::maintenance`] that this thread is no longer holding a"]
    #[doc = " reference to any epoch of shared data. Once **all** registered threads"]
    #[doc = " have passed through a quiescent state, the manager may safely reclaim"]
    #[doc = " any garbage enqueued before or during the previous epoch."]
    #[doc = ""]
    #[doc = " [`local_epoch`]: WorkerState::local_epoch"]
    #[inline(always)]
    pub fn leave(&self) {
        self.local_epoch.store(0, Ordering::Release);
    }
    #[doc = " Forcefully marks this worker as offline, ignoring its epoch in QSBR maintenance."]
    #[inline(always)]
    pub fn force_offline(&self) {
        self.is_offline.store(true, Ordering::Release);
    }
    #[doc = " Resumes this worker as online, participating in QSBR maintenance again."]
    #[inline(always)]
    pub fn resume_online(&self) {
        self.is_offline.store(false, Ordering::Release);
    }
}
#[doc = " QSBR (Quiescent-State-Based Reclamation) manager."]
#[doc = ""]
#[doc = " `QsbrManager` owns two resources:"]
#[doc = ""]
#[doc = " 1. **Worker list** — a shared, lock-free linked list of [`WorkerNode`]s,"]
#[doc = "    one per registered reader thread. The manager walks this list during"]
#[doc = "    [`maintenance`] to find the minimum epoch still visible to any active"]
#[doc = "    reader."]
#[doc = ""]
#[doc = " 2. **Garbage queue** — a [`Vec`] of [`GarbageEntry`] values, each carrying"]
#[doc = "    a raw pointer and the epoch at which it was retired. Entries whose epoch"]
#[doc = "    is strictly less than the minimum active epoch are safe to drop because"]
#[doc = "    no reader can still hold a reference to them."]
#[doc = ""]
#[doc = " [`maintenance`]: QsbrManager::maintenance"]
#[repr(C, align(64))]
pub struct QsbrManager {
    #[doc = " Shared pointer to the head of the [`WorkerNode`] linked list."]
    #[doc = ""]
    #[doc = " Wrapped in [`Arc`] so that both the manager and the subsystem that"]
    #[doc = " registers new workers can refer to the same list head without copying."]
    workers: Arc<AtomicPtr<WorkerNode>>,
    #[doc = " Deferred garbage waiting to be reclaimed."]
    #[doc = ""]
    #[doc = " Each [`GarbageEntry`] records a raw pointer and the [`GLOBAL_EPOCH`]"]
    #[doc = " value at the time it was retired. Entries are dropped (and their"]
    #[doc = " underlying memory freed) by [`QsbrManager::maintenance`] once it is"]
    #[doc = " safe to do so."]
    garbage: Vec<GarbageEntry>,
}
impl QsbrManager {
    #[doc = " Creates a new `QsbrManager` that tracks the worker list rooted at"]
    #[doc = " `workers`."]
    #[doc = ""]
    #[doc = " # Arguments"]
    #[doc = ""]
    #[doc = " * `workers` — a shared [`Arc`] pointing to the atomic head pointer of"]
    #[doc = "   the [`WorkerNode`] linked list. The same `Arc` should be held by"]
    #[doc = "   whatever subsystem registers new reader threads so that nodes inserted"]
    #[doc = "   after construction are immediately visible to this manager."]
    pub fn new(workers: Arc<AtomicPtr<WorkerNode>>) -> Self {
        Self {
            workers,
            garbage: Vec::new(),
        }
    }
    #[doc = " Registers a raw pointer for deferred reclamation."]
    #[doc = ""]
    #[doc = " The pointed-to value will be dropped (and its memory freed via"]
    #[doc = " [`GarbageEntry`]'s [`Drop`] implementation) during a future call to"]
    #[doc = " [`QsbrManager::maintenance`] once all currently active readers have"]
    #[doc = " passed through a quiescent state."]
    #[doc = ""]
    #[doc = " # Arguments"]
    #[doc = ""]
    #[doc = " * `ptr` — a raw, exclusively-owned pointer to the value to be retired."]
    #[doc = "   The caller must ensure it holds **exclusive ownership** of `*ptr` and"]
    #[doc = "   will not access it again after this call."]
    #[doc = ""]
    #[doc = " # No-op on null"]
    #[doc = ""]
    #[doc = " If `ptr` is null the function returns immediately without pushing"]
    #[doc = " anything onto the garbage queue."]
    #[doc = ""]
    #[doc = " # Safety"]
    #[doc = ""]
    #[doc = " The caller must guarantee that:"]
    #[doc = " - `ptr` was allocated in a manner compatible with [`GarbageEntry`]'s"]
    #[doc = "   drop logic (i.e. allocated via [`Box`] or an equivalent heap"]
    #[doc = "   allocator)."]
    #[doc = " - No other live reference to `*ptr` exists or will be created after"]
    #[doc = "   this call."]
    pub fn defer_free<T>(&mut self, ptr: *mut T) {
        if ptr.is_null() {
            return;
        }
        self.garbage
            .push(GarbageEntry::new(ptr, GLOBAL_EPOCH.load(Ordering::Relaxed)));
    }
    #[doc = " Advances the global epoch and reclaims garbage that is no longer"]
    #[doc = " visible to any active reader."]
    #[doc = ""]
    #[doc = " This method performs three steps:"]
    #[doc = ""]
    #[doc = " 1. **Increment [`GLOBAL_EPOCH`]** — signals to reader threads that a"]
    #[doc = "    new epoch has begun."]
    #[doc = ""]
    #[doc = " 2. **Find the minimum active epoch** — walks the [`WorkerNode`] list"]
    #[doc = "    and collects the smallest non-zero `local_epoch` value across all"]
    #[doc = "    currently active (i.e. inside a critical section) threads. If every"]
    #[doc = "    thread is quiescent the minimum stays equal to the newly incremented"]
    #[doc = "    global epoch, which triggers a second increment to push reclamation"]
    #[doc = "    forward, allowing all pending garbage to be collected."]
    #[doc = ""]
    #[doc = " 3. **Reclaim eligible garbage** — retains only those [`GarbageEntry`]"]
    #[doc = "    values whose recorded epoch is ≥ the minimum active epoch. Entries"]
    #[doc = "    that are not retained are dropped in place, running their [`Drop`]"]
    #[doc = "    implementation and freeing the associated memory."]
    #[doc = ""]
    #[doc = " # Safety"]
    #[doc = ""]
    #[doc = " This method calls [`crate::core::rcu::load_node`] internally, which"]
    #[doc = " dereferences raw pointers. The caller must ensure the `workers` list is"]
    #[doc = " always in a consistent, pointer-valid state while `maintenance` runs."]
    pub fn maintenance(&mut self) {
        GLOBAL_EPOCH.fetch_add(1, Ordering::Relaxed);
        let current_global = GLOBAL_EPOCH.load(Ordering::Acquire);
        let mut min_epoch = current_global;
        let mut curr_ptr = self.workers.load(Ordering::Acquire);
        while let Some(node) = unsafe { crate::core::rcu::load_node(curr_ptr) } {
            if !node.worker.is_offline.load(Ordering::Acquire) {
                let epoch = node.worker.local_epoch.load(Ordering::Acquire);
                if epoch != 0 && epoch < min_epoch {
                    min_epoch = epoch;
                }
            }
            curr_ptr = node.next.load(Ordering::Acquire);
        }
        if min_epoch == current_global {
            GLOBAL_EPOCH.fetch_add(1, Ordering::Release);
        }
        self.garbage.retain(|entry| entry.epoch >= min_epoch);
    }
}
