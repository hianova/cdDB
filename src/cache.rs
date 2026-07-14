use crate::DualCacheFF;
use crate::dualcache_ff::component::tls::TlsHandle;
use std::cell::UnsafeCell;
use std::hash::Hash;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::vec::Vec;

static NEXT_CACHE_ID: AtomicUsize = AtomicUsize::new(0);

std::thread_local! {
    static HANDLES: UnsafeCell<Vec<Option<TlsHandle>>> = const { UnsafeCell::new(Vec::new()) };
}

/// An ergonomic, auto-TLS-managed wrapper around `DualCacheFF`.
/// It provides O(1) lock-free caching performance while hiding thread registration overheads.
pub struct HitCache<
    K: Eq + Hash + Clone + Copy + Send + Sync + 'static,
    V: Clone + Send + Sync + 'static,
    const B0: usize,
    const B1: usize,
    const B2: usize,
    const B3: usize,
> {
    cache_id: usize,
    pub inner: DualCacheFF<K, V, B0, B1, B2, B3>,
}

impl<
    K: Eq + Hash + Clone + Copy + Send + Sync + 'static,
    V: Clone + Send + Sync + 'static,
    const B0: usize,
    const B1: usize,
    const B2: usize,
    const B3: usize,
> HitCache<K, V, B0, B1, B2, B3>
{
    pub fn new() -> Self {
        Self {
            cache_id: NEXT_CACHE_ID.fetch_add(1, Ordering::Relaxed),
            inner: DualCacheFF::new(),
        }
    }

    #[inline(always)]
    fn with_handle<R, F: FnOnce(&TlsHandle) -> R>(&self, f: F) -> R {
        HANDLES.with(|handles| unsafe {
            let vec = &mut *handles.get();
            if vec.len() <= self.cache_id {
                vec.resize_with(self.cache_id + 1, || None);
            }
            if vec[self.cache_id].is_none() {
                vec[self.cache_id] = Some(self.inner.register_thread());
            }
            f(vec[self.cache_id].as_ref().unwrap_unchecked())
        })
    }

    #[inline(always)]
    pub fn get(&self, key: &K) -> Option<V> {
        self.with_handle(|handle| self.inner.get(key, handle))
    }

    #[inline(always)]
    pub fn insert(&self, key: K, value: V) {
        self.with_handle(|handle| self.inner.insert(key, value, handle))
    }
}

impl<
    K: Eq + Hash + Clone + Copy + Send + Sync + 'static,
    V: Clone + Send + Sync + 'static,
    const B0: usize,
    const B1: usize,
    const B2: usize,
    const B3: usize,
> Default for HitCache<K, V, B0, B1, B2, B3>
{
    fn default() -> Self {
        Self::new()
    }
}
