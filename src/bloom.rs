use core::hash::Hash;
use ahash::RandomState;

use crate::sync::atomic::{AtomicUsize, Ordering};



pub struct SimpleBloom<const N: usize> {
    bits: [AtomicUsize; N],
    hash_builder: RandomState,
}

impl<const N: usize> SimpleBloom<N> {
    const NUM_BITS: usize = N * core::mem::size_of::<usize>() * 8;

    pub fn new() -> Self {
        Self {
            bits: core::array::from_fn(|_| AtomicUsize::new(0)),
            hash_builder: RandomState::new(),
        }
    }

    pub fn insert<T: Hash>(&self, item: &T) {
        let hash = self.hash_builder.hash_one(item);
        let h1 = hash as u32;
        let h2 = (hash >> 32) as u32;

        for i in 0..4u32 {
            let combined_hash = h1.wrapping_add(i.wrapping_mul(h2)) as usize;
            let bit_idx = combined_hash % Self::NUM_BITS;
            self.bits[bit_idx / 64].fetch_or(1 << (bit_idx % 64), Ordering::Relaxed);
        }
    }

    pub fn contains<T: Hash>(&self, item: &T) -> bool {
        let hash = self.hash_builder.hash_one(item);
        let h1 = hash as u32;
        let h2 = (hash >> 32) as u32;

        for i in 0..4u32 {
            let combined_hash = h1.wrapping_add(i.wrapping_mul(h2)) as usize;
            let bit_idx = combined_hash % Self::NUM_BITS;
            if (self.bits[bit_idx / 64].load(Ordering::Relaxed) & (1 << (bit_idx % 64))) == 0 {
                return false;
            }
        }
        true
    }

    pub fn clear(&self) {
        for word in self.bits.iter() {
            word.store(0, Ordering::Relaxed);
        }
    }
}

#[cfg(all(test, not(feature = "loom")))]
mod tests {
    use super::*;

    #[test]
    fn test_bloom_filter() {
        let bloom = SimpleBloom::<1024>::new();
        let entity_id = 42usize;
        
        assert_eq!(bloom.contains(&entity_id), false);
        bloom.insert(&entity_id);
        assert_eq!(bloom.contains(&entity_id), true);
    }
}
