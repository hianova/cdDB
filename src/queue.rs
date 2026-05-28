use core::cell::UnsafeCell;
use core::mem::MaybeUninit;
use core::sync::atomic::{AtomicUsize, AtomicU8, Ordering};
use alloc::vec::Vec;
use alloc::boxed::Box;

// ── Slot state constants ───────────────────────────────────────────────────

const EMPTY: u8 = 0;
const WRITING: u8 = 1;
const READY: u8 = 2;

// ── Slot ──────────────────────────────────────────────────────────────────

struct Slot<T> {
    state: AtomicU8,
    data: UnsafeCell<MaybeUninit<T>>,
}

unsafe impl<T: Send> Send for Slot<T> {}
unsafe impl<T: Send> Sync for Slot<T> {}

// ── BoundedQueue ──────────────────────────────────────────────────────────

/// MPSC wait-free bounded ring buffer.
pub struct BoundedQueue<T> {
    mask: usize,
    /// Producer cursor — FAA, unbounded (wraps via mask).
    tail: AtomicUsize,
    /// Consumer cursor — single-threaded advance.
    head: AtomicUsize,
    buffer: Box<[Slot<T>]>,
}

unsafe impl<T: Send> Send for BoundedQueue<T> {}
unsafe impl<T: Send> Sync for BoundedQueue<T> {}

impl<T> BoundedQueue<T> {
    /// Create a new queue with the given capacity (must be a power of two).
    pub fn new(capacity: usize) -> Self {
        assert!(
            capacity.is_power_of_two(),
            "BoundedQueue capacity must be a power of two"
        );
        let mut buf = Vec::with_capacity(capacity);
        for _ in 0..capacity {
            buf.push(Slot {
                state: AtomicU8::new(EMPTY),
                data: UnsafeCell::new(MaybeUninit::uninit()),
            });
        }
        Self {
            mask: capacity - 1,
            tail: AtomicUsize::new(0),
            head: AtomicUsize::new(0),
            buffer: buf.into_boxed_slice(),
        }
    }

    /// Try to enqueue an item.
    ///
    /// Uses FAA to claim a slot index, then CAS EMPTY→WRITING as a physical
    /// gate. If the slot is occupied (ring buffer lapped or concurrent writer),
    /// returns `Err(item)` immediately — never blocks.
    #[inline(always)]
    pub fn push(&self, item: T) -> Result<(), T> {
        let tail = self.tail.load(Ordering::Relaxed);
        let head = self.head.load(Ordering::Acquire);

        // Pre-check: if physically full, don't even try to FAA.
        if tail.wrapping_sub(head) >= self.buffer.len() {
            return Err(item);
        }

        // FAA claims a position.
        let idx = self.tail.fetch_add(1, Ordering::Relaxed) & self.mask;
        let slot = &self.buffer[idx];

        // Physical gate: only proceed if the slot is truly empty.
        if slot
            .state
            .compare_exchange(EMPTY, WRITING, Ordering::Acquire, Ordering::Relaxed)
            .is_err()
        {
            return Err(item);
        }

        unsafe {
            (*slot.data.get()).write(item);
        }
        slot.state.store(READY, Ordering::Release);
        Ok(())
    }

    /// Try to dequeue one item.
    ///
    /// Single-consumer: only one thread should call this.
    /// Reads from `head`, returns `None` if the slot is not yet READY.
    #[inline(always)]
    pub fn pop(&self) -> Option<T> {
        let idx = self.head.load(Ordering::Relaxed) & self.mask;
        let slot = &self.buffer[idx];

        if slot.state.load(Ordering::Acquire) == READY {
            // Safe read: we are the exclusive consumer.
            let item = unsafe { (*slot.data.get()).assume_init_read() };

            // Reset gate and advance head.
            slot.state.store(EMPTY, Ordering::Release);
            self.head.fetch_add(1, Ordering::Relaxed);
            Some(item)
        } else {
            None
        }
    }
}

impl<T> Drop for BoundedQueue<T> {
    fn drop(&mut self) {
        // Drain any READY items that were never consumed.
        loop {
            let idx = self.head.load(Ordering::Relaxed) & self.mask;
            let slot = &self.buffer[idx];
            if slot.state.load(Ordering::Acquire) == READY {
                unsafe { (*slot.data.get()).assume_init_drop() };
                slot.state.store(EMPTY, Ordering::Relaxed);
                self.head.fetch_add(1, Ordering::Relaxed);
            } else {
                break;
            }
        }
    }
}
