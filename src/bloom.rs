use core::hash::Hash;
use ahash::RandomState;
use alloc::vec::Vec;
use alloc::vec;

pub struct SimpleBloom {
    bits: Vec<u64>,
    num_bits: usize,
    hash_builder: RandomState,
}

impl SimpleBloom {
    pub fn new(num_bits: usize) -> Self {
        let num_words = (num_bits + 63) / 64;
        Self {
            bits: vec![0; num_words],
            num_bits: num_words * 64,
            hash_builder: RandomState::new(),
        }
    }

    pub fn insert<T: Hash>(&mut self, item: &T) {
        let hash = self.hash_builder.hash_one(item);
        let h1 = hash as u32;
        let h2 = (hash >> 32) as u32;

        for i in 0..4u32 {
            let combined_hash = h1.wrapping_add(i.wrapping_mul(h2)) as usize;
            let bit_idx = combined_hash % self.num_bits;
            self.bits[bit_idx / 64] |= 1 << (bit_idx % 64);
        }
    }

    pub fn contains<T: Hash>(&self, item: &T) -> bool {
        let hash = self.hash_builder.hash_one(item);
        let h1 = hash as u32;
        let h2 = (hash >> 32) as u32;

        for i in 0..4u32 {
            let combined_hash = h1.wrapping_add(i.wrapping_mul(h2)) as usize;
            let bit_idx = combined_hash % self.num_bits;
            if (self.bits[bit_idx / 64] & (1 << (bit_idx % 64))) == 0 {
                return false;
            }
        }
        true
    }

    pub fn clear(&mut self) {
        for word in self.bits.iter_mut() {
            *word = 0;
        }
    }
}
