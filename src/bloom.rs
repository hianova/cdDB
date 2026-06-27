use core::hash::Hash;
use ahash::RandomState;

use crate::sync::atomic::{AtomicUsize, Ordering};



/// A lock-free, constant-generic Bloom filter backed by AHash and four hash probes.
///
/// `SimpleBloom<N>` stores its bitset as an array of `N` [`AtomicUsize`] words, giving a
/// total capacity of `N × 64` bits (on 64-bit platforms). Every [`insert`](Self::insert) and
/// [`contains`](Self::contains) operation derives four independent bit positions from a
/// single AHash digest using the double-hashing scheme
/// `h(i) = h1 + i * h2` for `i ∈ {0, 1, 2, 3}`, and manipulates the corresponding
/// bits with relaxed atomic operations so the filter can be used concurrently without
/// any external locks.
///
/// # Type Parameter
///
/// * `N` — the number of **`usize`-words** in the bitset. The total number of
///   addressable bits is `N * 64` (i.e. `N * size_of::<usize>() * 8`).
///
/// # False positives
///
/// Like all Bloom filters, `SimpleBloom` may return `true` from [`contains`](Self::contains)
/// for an item that was never inserted. The false-positive rate rises as the filter
/// fills up; use [`count_set_bits`](Self::count_set_bits) and
/// [`total_bits`](Self::total_bits) to monitor saturation.
///
/// # Examples
///
/// ```
/// use cddb::bloom::SimpleBloom;
///
/// // 1 024 words × 64 bits = 65 536-bit filter
/// let bloom = SimpleBloom::<1024>::new();
///
/// bloom.insert(&"hello");
/// assert!(bloom.contains(&"hello"));
/// assert!(!bloom.contains(&"world")); // probably false, could be a false positive
/// ```
pub struct SimpleBloom<const N: usize> {
    /// The underlying bitset, stored as an array of atomically-accessed `usize` words.
    /// Each word holds 64 independently addressable bits (on 64-bit platforms).
    bits: [AtomicUsize; N],
    /// The AHash [`RandomState`] used to produce a single 64-bit digest that is then
    /// split into two 32-bit halves (`h1`, `h2`) for the double-hashing scheme.
    hash_builder: RandomState,
}

impl<const N: usize> SimpleBloom<N> {
    const NUM_BITS: usize = N * core::mem::size_of::<usize>() * 8;

    /// Creates a new, empty `SimpleBloom` filter with all bits initialised to `0`.
    ///
    /// The AHash [`RandomState`] is seeded randomly at construction time, so each
    /// instance uses a distinct hash function family. This prevents attackers from
    /// crafting worst-case inputs across restarts (hash-flooding resistance).
    ///
    /// # Examples
    ///
    /// ```
    /// use cddb::bloom::SimpleBloom;
    ///
    /// // Create a filter with 512 × 64 = 32 768 bits
    /// let bloom = SimpleBloom::<512>::new();
    /// ```
    pub fn new() -> Self {
        Self {
            bits: core::array::from_fn(|_| AtomicUsize::new(0)),
            hash_builder: RandomState::new(),
        }
    }

    /// Inserts `item` into the filter by setting its four representative bits.
    ///
    /// The method hashes `item` with AHash to obtain a 64-bit digest, then splits it
    /// into two 32-bit halves `h1` and `h2`. Four bit positions are derived via the
    /// double-hashing formula `h1 + i * h2` (mod `N * 64`) for `i ∈ {0, 1, 2, 3}`.
    /// Each corresponding bit in the atomic word array is set with
    /// [`fetch_or`](AtomicUsize::fetch_or) using [`Relaxed`](Ordering::Relaxed)
    /// ordering, making this safe to call concurrently from multiple threads.
    ///
    /// # Examples
    ///
    /// ```
    /// use cddb::bloom::SimpleBloom;
    ///
    /// let bloom = SimpleBloom::<64>::new();
    /// bloom.insert(&42usize);
    /// assert!(bloom.contains(&42usize));
    /// ```
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

    /// Returns `true` if `item` is **possibly** in the filter, `false` if it is
    /// **definitely not**.
    ///
    /// The same four bit positions computed during [`insert`](Self::insert) are
    /// recalculated and each is checked with a [`Relaxed`](Ordering::Relaxed) atomic
    /// load. If every bit is set the method returns `true`; if even one is clear the
    /// item was definitely never inserted and `false` is returned immediately.
    ///
    /// # False positives
    ///
    /// Because multiple items can share bit positions a `true` result does **not**
    /// guarantee that `item` was inserted — it may be a false positive. The
    /// probability increases as the filter becomes more saturated. Use
    /// [`count_set_bits`](Self::count_set_bits) to monitor fill level.
    ///
    /// # Examples
    ///
    /// ```
    /// use cddb::bloom::SimpleBloom;
    ///
    /// let bloom = SimpleBloom::<64>::new();
    /// assert!(!bloom.contains(&"absent"));
    /// bloom.insert(&"present");
    /// assert!(bloom.contains(&"present"));
    /// ```
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

    /// Returns the total number of bits that are currently set to `1` across all words.
    ///
    /// This is useful for estimating filter saturation: a heavily saturated filter
    /// (i.e. `count_set_bits() / total_bits()` approaching `1.0`) will produce many
    /// false positives. Each word is read with [`Relaxed`](Ordering::Relaxed) ordering,
    /// so the result is a best-effort snapshot rather than a linearisable count.
    ///
    /// # Examples
    ///
    /// ```
    /// use cddb::bloom::SimpleBloom;
    ///
    /// let bloom = SimpleBloom::<64>::new();
    /// assert_eq!(bloom.count_set_bits(), 0);
    /// bloom.insert(&1u64);
    /// // Exactly 4 bits are set after one insert (assuming no collisions).
    /// assert!(bloom.count_set_bits() <= 4);
    /// ```
    pub fn count_set_bits(&self) -> usize {
        let mut count = 0;
        for word in self.bits.iter() {
            count += word.load(Ordering::Relaxed).count_ones() as usize;
        }
        count
    }

    /// Returns the total number of bits in this filter, equal to `N * 64`.
    ///
    /// This is a compile-time constant exposed as an instance method for
    /// convenience. Divide [`count_set_bits`](Self::count_set_bits) by this value
    /// to obtain the current fill ratio.
    ///
    /// # Examples
    ///
    /// ```
    /// use cddb::bloom::SimpleBloom;
    ///
    /// let bloom = SimpleBloom::<128>::new();
    /// assert_eq!(bloom.total_bits(), 128 * 64);
    /// ```
    pub fn total_bits(&self) -> usize {
        Self::NUM_BITS
    }

    /// Atomically resets every bit in the filter to `0`, returning it to an empty state.
    ///
    /// Each word is written with [`Relaxed`](Ordering::Relaxed) ordering. The clear
    /// is **not** performed as a single atomic transaction — concurrent readers or
    /// writers may observe the filter in a partially-cleared state during the
    /// operation. If a consistent empty snapshot is required, ensure no concurrent
    /// access occurs while `clear` is running.
    ///
    /// # Examples
    ///
    /// ```
    /// use cddb::bloom::SimpleBloom;
    ///
    /// let bloom = SimpleBloom::<64>::new();
    /// bloom.insert(&99usize);
    /// assert!(bloom.contains(&99usize));
    ///
    /// bloom.clear();
    /// assert_eq!(bloom.count_set_bits(), 0);
    /// ```
    pub fn clear(&self) {
        for word in self.bits.iter() {
            word.store(0, Ordering::Relaxed);
        }
    }
}

#[cfg(test)]
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
