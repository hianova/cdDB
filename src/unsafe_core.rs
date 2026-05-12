use std::sync::atomic::{AtomicPtr, Ordering};

/// Safely load a reference from an AtomicPtr.
/// Must be called within a QSBR enter/leave block.
pub unsafe fn load_ref<'a, T>(atomic: &AtomicPtr<T>) -> &'a T {
    let ptr = atomic.load(Ordering::Acquire);
    if ptr.is_null() {
        panic!("AtomicPtr was null");
    }
    unsafe { &*ptr }
}

/// Safely load and clone data from an AtomicPtr.
pub unsafe fn load_clone<T: Clone>(atomic: &AtomicPtr<T>) -> T {
    unsafe { load_ref(atomic).clone() }
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
