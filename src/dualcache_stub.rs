#[derive(Clone, Debug)]
pub struct DualCacheFF<K, V> {
    _marker: core::marker::PhantomData<(K, V)>,
}

unsafe impl<K, V> Send for DualCacheFF<K, V> {}
unsafe impl<K, V> Sync for DualCacheFF<K, V> {}

#[derive(Clone, Debug)]
pub struct Config;

impl Config {
    pub fn with_memory_budget(_budget: usize, _percent: usize) -> Self {
        Self
    }
}

impl<K, V> DualCacheFF<K, V> {
    pub fn new(_config: Config) -> Self {
        Self {
            _marker: core::marker::PhantomData,
        }
    }

    pub fn insert(&self, _key: K, _value: V) {}
    pub fn remove(&self, _key: &K) -> Option<V> {
        None
    }
    pub fn get(&self, _key: &K) -> Option<&V> {
        None
    }
}
