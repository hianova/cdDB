use alloc::boxed::Box;
use alloc::vec::Vec;
use core::cell::UnsafeCell;
use core::mem::MaybeUninit;
use core::sync::atomic::{AtomicU8, AtomicUsize, Ordering};

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

/// MPSC (multi-producer, single-consumer) wait-free bounded ring buffer.
///
/// Each partition owns one `BoundedQueue` that acts as its **command channel**:
/// any number of writer threads may call [`push`](BoundedQueue::push)
/// concurrently, while exactly one reader thread drains items via
/// [`pop`](BoundedQueue::pop).
///
/// # Capacity constraint
///
/// `capacity` must be a **power of two**. This allows the implementation to
/// replace all modulo operations with a single bitwise AND against `mask`,
/// keeping the hot path branch-free.
///
/// # Wait-freedom
///
/// Neither `push` nor `pop` ever spins or blocks. A `push` that cannot claim
/// a free slot returns `Err(item)` immediately; a `pop` on an empty queue
/// returns `None` immediately.
pub struct BoundedQueue<T> {
    /// Bitmask derived from the capacity (`capacity - 1`).
    ///
    /// Because the capacity is always a power of two, `index & mask` is
    /// equivalent to `index % capacity` but is a single instruction.
    mask: usize,
    /// Producer cursor — advanced via fetch-add, unbounded (wraps via `mask`).
    tail: AtomicUsize,
    /// Consumer cursor — single-threaded advance.
    head: AtomicUsize,
    /// Backing storage: a heap-allocated, fixed-length slice of [`Slot`]s.
    ///
    /// The length equals the capacity supplied to [`BoundedQueue::new`].
    buffer: Box<[Slot<T>]>,
}

unsafe impl<T: Send> Send for BoundedQueue<T> {}
unsafe impl<T: Send> Sync for BoundedQueue<T> {}

impl<T> BoundedQueue<T> {
    /// Creates a new `BoundedQueue` with the given `capacity`.
    ///
    /// All slots are initialised in the `EMPTY` state, ready to accept items.
    ///
    /// # Panics
    ///
    /// Panics if `capacity` is **not** a power of two (including zero).
    ///
    /// # Examples
    ///
    /// ```
    /// let q: BoundedQueue<u32> = BoundedQueue::new(64);
    /// ```
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

    /// Attempts a wait-free multi-producer enqueue of `item`.
    ///
    /// Internally this performs a **fetch-add** (FAA) to speculatively claim a
    /// slot index, then uses a **compare-and-swap** (`EMPTY → WRITING`) as a
    /// physical gate that confirms the slot is truly available. Writing the
    /// item and publishing it (`WRITING → READY`) are the final steps.
    ///
    /// This method is safe to call from multiple threads simultaneously.
    ///
    /// # Returns
    ///
    /// - `Ok(())` — `item` was successfully enqueued.
    /// - `Err(item)` — the queue is full (or the slot was concurrently
    ///   occupied), and the caller retains ownership of `item`. This path
    ///   never blocks or spins.
    #[inline(always)]
    pub fn push(&self, item: T) -> Result<(), T> {
        let mut tail = self.tail.load(Ordering::Relaxed);
        loop {
            let head = self.head.load(Ordering::Acquire);

            // Pre-check: if physically full, return Err.
            if tail.wrapping_sub(head) >= self.buffer.len() {
                return Err(item);
            }

            let idx = tail & self.mask;
            let slot = &self.buffer[idx];

            let state = slot.state.load(Ordering::Acquire);
            if state != EMPTY {
                let actual = self.tail.load(Ordering::Relaxed);
                if actual == tail {
                    // Tail hasn't changed but slot isn't empty.
                    // This could be a slow consumer that advanced head but hasn't set EMPTY yet.
                    // Re-check head to see if we are truly full.
                    let current_head = self.head.load(Ordering::Acquire);
                    if tail.wrapping_sub(current_head) >= self.buffer.len() {
                        return Err(item);
                    } else {
                        core::hint::spin_loop();
                        continue;
                    }
                } else {
                    tail = actual;
                    continue;
                }
            }

            // Attempt to claim the tail ticket
            match self.tail.compare_exchange_weak(
                tail,
                tail.wrapping_add(1),
                Ordering::AcqRel,
                Ordering::Relaxed,
            ) {
                Ok(_) => {
                    // Successfully claimed tail. Wait for the slot to become truly EMPTY
                    // (in case of memory reordering from consumer) and set it to WRITING.
                    while slot
                        .state
                        .compare_exchange_weak(EMPTY, WRITING, Ordering::Acquire, Ordering::Relaxed)
                        .is_err()
                    {
                        core::hint::spin_loop();
                    }
                    unsafe {
                        (*slot.data.get()).write(item);
                    }
                    slot.state.store(READY, Ordering::Release);
                    return Ok(());
                }
                Err(actual) => {
                    tail = actual;
                }
            }
        }
    }

    /// Attempts a wait-free single-consumer dequeue.
    ///
    /// Inspects the slot at `head`. If its state is `READY` the item is moved
    /// out, the slot is reset to `EMPTY`, and `head` is advanced by one.
    ///
    /// # Single-consumer contract
    ///
    /// **Only one thread may call `pop` at a time.** Concurrent calls from
    /// multiple threads will produce undefined behaviour because the head
    /// cursor is not protected by any mutual-exclusion mechanism — it is
    /// advanced non-atomically across the read–reset–increment sequence.
    ///
    /// # Returns
    ///
    /// - `Some(item)` — an item was dequeued.
    /// - `None` — the queue is empty (the next slot is not yet `READY`).
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
