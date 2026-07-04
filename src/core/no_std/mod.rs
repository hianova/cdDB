pub use no_std_tool::sync::{SpinMutex as Mutex, SpinMutexGuard as MutexGuard};

pub mod atomic {
    #[cfg(target_has_atomic = "64")]
    pub use no_std_tool::sync::AtomicU64;
    pub use no_std_tool::sync::{AtomicBool, AtomicPtr, AtomicUsize, Ordering};
}
