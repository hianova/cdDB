use crate::platform::atomic::{AtomicPtr, Ordering};
use alloc::boxed::Box;

/// Safely load a reference from an AtomicPtr.
/// Rationale: In cdDB, this is safe if called within a QSBR enter/leave block
/// and the pointer is managed by QsbrManager.
pub fn load_ref<'a, T>(atomic: &AtomicPtr<T>) -> &'a T {
    let ptr = atomic.load(Ordering::Acquire);
    if ptr.is_null() {
        panic!("AtomicPtr was null");
    }
    unsafe { &*ptr }
}

/// Safely load and clone data from an AtomicPtr.
pub fn load_clone<T: Clone>(atomic: &AtomicPtr<T>) -> T {
    load_ref(atomic).clone()
}

/// Helper to wrap a value in a Box and return as AtomicPtr
pub fn new_atomic_ptr<T>(val: T) -> AtomicPtr<T> {
    AtomicPtr::new(Box::into_raw(Box::new(val)))
}

/// Swap and return old pointer for deferred freeing
pub fn swap_ptr<T>(atomic: &AtomicPtr<T>, val: T) -> *mut T {
    let new_ptr = Box::into_raw(Box::new(val));
    atomic.swap(new_ptr, Ordering::AcqRel)
}

/// Encapsulated Garbage Entry to hide unsafe fields
pub struct GarbageEntry {
    ptr: *mut (),
    pub epoch: usize,
    drop_fn: unsafe fn(*mut ()),
}

unsafe impl Send for GarbageEntry {}
unsafe impl Sync for GarbageEntry {}

impl GarbageEntry {
    pub fn new<T>(ptr: *mut T, epoch: usize) -> Self {
        unsafe fn drop_ptr<T>(p: *mut ()) {
            unsafe {
                let _ = Box::from_raw(p as *mut T);
            }
        }
        
        Self {
            ptr: ptr as *mut (),
            epoch,
            drop_fn: drop_ptr::<T>,
        }
    }
}

impl Drop for GarbageEntry {
    fn drop(&mut self) {
        unsafe { (self.drop_fn)(self.ptr); }
    }
}
