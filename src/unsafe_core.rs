use crate::sync::atomic::{AtomicPtr, Ordering};
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

/// Safely dereference a pointer to a node, returning None if null.
/// # Safety
/// The caller must ensure that `ptr` is either null or points to a valid `T` that is safe to dereference.
pub unsafe fn load_node<'a, T>(ptr: *mut T) -> Option<&'a T> {
    if ptr.is_null() {
        None
    } else {
        Some(unsafe { &*ptr })
    }
}

/// Safely store a pointer into an AtomicPtr field of a node.
/// # Safety
/// The caller must ensure that `node` is either null or points to a valid `T`.
pub unsafe fn link_node<T, P>(
    node: *mut T,
    get_atomic_field: impl FnOnce(&T) -> &crate::sync::atomic::AtomicPtr<P>,
    next_ptr: *mut P,
) {
    if !node.is_null() {
        let atomic = unsafe { get_atomic_field(&*node) };
        atomic.store(next_ptr, crate::sync::atomic::Ordering::Relaxed);
    }
}

#[cfg(not(feature = "std"))]
pub mod no_std_sync {
    use core::sync::atomic::{AtomicBool, Ordering};
    use core::cell::UnsafeCell;
    use core::ops::{Deref, DerefMut};

    pub struct Mutex<T: ?Sized> {
        locked: AtomicBool,
        data: UnsafeCell<T>,
    }

    unsafe impl<T: ?Sized + Send> Send for Mutex<T> {}
    unsafe impl<T: ?Sized + Send> Sync for Mutex<T> {}

    pub struct MutexGuard<'a, T: ?Sized> {
        mutex: &'a Mutex<T>,
    }

    impl<T> Mutex<T> {
        pub const fn new(val: T) -> Self {
            Self {
                locked: AtomicBool::new(false),
                data: UnsafeCell::new(val),
            }
        }
    }

    impl<T: ?Sized> Mutex<T> {
        pub fn lock(&self) -> MutexGuard<'_, T> {
            while self.locked.compare_exchange_weak(false, true, Ordering::Acquire, Ordering::Relaxed).is_err() {
                core::hint::spin_loop();
            }
            MutexGuard { mutex: self }
        }
    }

    impl<T: ?Sized> Deref for MutexGuard<'_, T> {
        type Target = T;
        fn deref(&self) -> &Self::Target {
            unsafe { &*self.mutex.data.get() }
        }
    }

    impl<T: ?Sized> DerefMut for MutexGuard<'_, T> {
        fn deref_mut(&mut self) -> &mut Self::Target {
            unsafe { &mut *self.mutex.data.get() }
        }
    }

    impl<T: ?Sized> Drop for MutexGuard<'_, T> {
        fn drop(&mut self) {
            self.mutex.locked.store(false, Ordering::Release);
        }
    }
}
