pub use std::sync::Mutex;

pub mod atomic {
    pub use core::sync::atomic::{AtomicBool, AtomicPtr, AtomicUsize, Ordering};
    #[cfg(target_has_atomic = "64")]
    pub use core::sync::atomic::AtomicU64;
}
