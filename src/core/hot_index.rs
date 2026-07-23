pub trait HotIndexProvider: Send + Sync {
    type Handle;
    fn register_thread(&self) -> Self::Handle;
    fn is_hot(&self, partition_id: u32, entity_id: usize, handle: &Self::Handle) -> bool;
    fn prewarm(&self, partition_id: u32, entity_ids: &[usize]);
}

#[cfg(all(feature = "dualcache-ff", feature = "std"))]
impl<const B0: usize, const B1: usize, const B2: usize, const B3: usize> HotIndexProvider
    for crate::DualCacheFF<(u32, usize), (), dualcache_ff::core::config::DefaultExponentialPolicy, B0, B1, B2, B3>
{
    type Handle = crate::dualcache_ff::component::tls::TlsHandle;

    fn register_thread(&self) -> Self::Handle {
        crate::DualCacheFF::register_thread(self)
    }

    fn is_hot(&self, partition_id: u32, entity_id: usize, handle: &Self::Handle) -> bool {
        self.get_safe(&(partition_id, entity_id), handle).is_some()
    }

    fn prewarm(&self, partition_id: u32, entity_ids: &[usize]) {
        let handle = crate::DualCacheFF::register_thread(self);
        for &id in entity_ids {
            self.warmup((partition_id, id), (), &handle);
        }
    }
}

#[cfg(any(not(feature = "dualcache-ff"), not(feature = "std")))]
impl<
    const P: usize,
    const C2: usize,
    const C1: usize,
    const C0: usize,
    const TC: usize,
    const P4: usize,
    const P5: usize,
    const P6: usize,
> HotIndexProvider for crate::DualCacheFF<(u32, usize), (), dualcache_ff::core::config::DefaultExponentialPolicy, P, C2, C1, C0, TC, P4, P5, P6> {
    type Handle = ();

    fn register_thread(&self) -> Self::Handle {}

    fn is_hot(&self, _partition_id: u32, _entity_id: usize, _handle: &Self::Handle) -> bool {
        false
    }

    fn prewarm(&self, _partition_id: u32, _entity_ids: &[usize]) {}
}
