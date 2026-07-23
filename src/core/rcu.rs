use crate::core::atomic::{AtomicPtr, Ordering};
use alloc::boxed::Box;
#[doc = " Safely load a reference from an AtomicPtr."]
pub fn load_ref<'a, T>(atomic: &AtomicPtr<T>) -> &'a T {
    let ptr = atomic.load(Ordering::Acquire);
    if ptr.is_null() {
        panic!("AtomicPtr was null");
    }
    unsafe { &*ptr }
}
#[doc = " Safely load and clone data from an AtomicPtr."]
pub fn load_clone<T: Clone>(atomic: &AtomicPtr<T>) -> T {
    load_ref(atomic).clone()
}
#[doc = " Helper to wrap a value in a Box and return as AtomicPtr"]
pub fn new_atomic_ptr<T>(val: T) -> AtomicPtr<T> {
    AtomicPtr::new(Box::into_raw(Box::new(val)))
}
#[doc = " Helper to wrap a boxed value as AtomicPtr"]
pub fn new_atomic_ptr_from_box<T>(b: Box<T>) -> AtomicPtr<T> {
    AtomicPtr::new(Box::into_raw(b))
}
pub fn swap_ptr<T>(atomic: &AtomicPtr<T>, val: T) -> *mut T {
    let new_ptr = Box::into_raw(Box::new(val));
    atomic.swap(new_ptr, Ordering::AcqRel)
}
#[doc = " Swap and return old pointer for deferred freeing using a Box"]
pub fn swap_ptr_with_box<T>(atomic: &AtomicPtr<T>, b: Box<T>) -> *mut T {
    let new_ptr = Box::into_raw(b);
    atomic.swap(new_ptr, Ordering::AcqRel)
}
#[doc = " Encapsulated Garbage Entry to hide unsafe fields"]
#[repr(C, align(64))]
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
        unsafe {
            (self.drop_fn)(self.ptr);
        }
    }
}
#[doc = " Safely dereference a pointer to a node, returning None if null."]
#[doc = " Loads the node behind the pointer."]
#[doc = ""]
#[doc = " # Safety"]
#[doc = " The caller must guarantee that the pointer is valid and properly aligned,"]
#[doc = " and that no other thread is mutating the node at the same time if it's accessed immutably."]
pub unsafe fn load_node<'a, T>(ptr: *mut T) -> Option<&'a T> {
    if ptr.is_null() {
        None
    } else {
        Some(unsafe { &*ptr })
    }
}
#[doc = " Safely store a pointer into an AtomicPtr field of a node."]
#[doc = " Links a new node to the list."]
#[doc = ""]
#[doc = " # Safety"]
#[doc = " The caller must ensure that `node` and `next_ptr` are valid, and `get_atomic_field` returns a valid pointer."]
pub unsafe fn link_node<T, P>(
    node: *mut T,
    get_atomic_field: impl FnOnce(&T) -> &crate::core::atomic::AtomicPtr<P>,
    next_ptr: *mut P,
) {
    if !node.is_null() {
        let atomic = unsafe { get_atomic_field(&*node) };
        atomic.store(next_ptr, crate::core::atomic::Ordering::Relaxed);
    }
}
