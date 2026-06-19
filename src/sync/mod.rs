#[cfg(feature = "std")]
pub use std::sync::Mutex;

#[cfg(not(feature = "std"))]
pub use crate::unsafe_core::no_std_sync::Mutex;

#[cfg(feature = "loom")]
pub mod atomic {
    pub use loom::sync::atomic::{AtomicBool, AtomicPtr, AtomicUsize, Ordering};
}

#[cfg(not(feature = "loom"))]
pub mod atomic {
    pub use core::sync::atomic::{AtomicBool, AtomicPtr, AtomicUsize, Ordering};
    #[cfg(target_has_atomic = "64")]
    pub use core::sync::atomic::AtomicU64;
}
