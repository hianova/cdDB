pub use std::sync::Mutex;

pub mod atomic {
    #[cfg(target_has_atomic = "64")]
    pub use core::sync::atomic::AtomicU64;
    pub use core::sync::atomic::{AtomicBool, AtomicPtr, AtomicUsize, Ordering};
}
