use core::hash::Hash;
use ahash::RandomState;
use alloc::vec::Vec;
use crate::platform::atomic::{AtomicUsize, Ordering};

const BITS_PER_WORD: usize = core::mem::size_of::<usize>() * 8;

pub struct SimpleBloom {
    bits: Vec<AtomicUsize>,
    num_bits: usize,
    hash_builder: RandomState,
}

impl SimpleBloom {
    pub fn new(num_bits: usize) -> Self {
        let num_words = (num_bits + BITS_PER_WORD - 1) / BITS_PER_WORD;
        let mut bits = Vec::with_capacity(num_words);
        for _ in 0..num_words {
            bits.push(AtomicUsize::new(0));
        }
        Self {
            bits,
            num_bits: num_words * BITS_PER_WORD,
            hash_builder: RandomState::new(),
        }
    }

    pub fn insert<T: Hash>(&self, item: &T) {
        let hash = self.hash_builder.hash_one(item);
        let h1 = hash as u32;
        let h2 = (hash >> 32) as u32;

        for i in 0..4u32 {
            let combined_hash = h1.wrapping_add(i.wrapping_mul(h2)) as usize;
            let bit_idx = combined_hash % self.num_bits;
            self.bits[bit_idx / BITS_PER_WORD].fetch_or(1 << (bit_idx % BITS_PER_WORD), Ordering::Relaxed);
        }
    }

    pub fn contains<T: Hash>(&self, item: &T) -> bool {
        let hash = self.hash_builder.hash_one(item);
        let h1 = hash as u32;
        let h2 = (hash >> 32) as u32;

        for i in 0..4u32 {
            let combined_hash = h1.wrapping_add(i.wrapping_mul(h2)) as usize;
            let bit_idx = combined_hash % self.num_bits;
            if (self.bits[bit_idx / BITS_PER_WORD].load(Ordering::Relaxed) & (1 << (bit_idx % BITS_PER_WORD))) == 0 {
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
