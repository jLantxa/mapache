use crate::mapache::ID;

/// A simple, space-efficient Bloom Filter for deduplication lookups.
///
/// ### How it works
/// A Bloom Filter is a probabilistic data structure that tells you if an element is *definitely not*
/// in a set or *might be* in the set. It provides O(1) performance for insertions and lookups
/// with a tiny memory footprint.
///
/// 1. **Bit Array**: Uses a compact bit array to represent presence.
/// 2. **Double Hashing**: Instead of calculating K independent hashes (which is expensive),
///    we use two 64-bit halves of the ID hash and simulate K hashes using:
///    `h(i) = (h1 + i * h2) % m`.
/// 3. **Power-of-Two Masking**: To ensure maximum speed, the bit array size (`m`) is rounded to the
///    next power of two. This replaces the expensive modulo (`%`) operation with a simple
///    bitwise AND (`&`), drastically reducing CPU cycles per hash.
///
/// ### False Positives
/// This structure can have false positives (reporting "present" when absent), but never
/// false negatives. The probability of a false positive is tuned by `false_positive_rate`
#[derive(Debug, Clone)]
pub struct BloomFilter {
    bits: Vec<u64>,
    num_hashes: u32,
    bit_count: u64,
}

impl BloomFilter {
    /// Create a new Bloom Filter optimized for the given number of items and false positive rate.
    pub fn new(num_items: usize, false_positive_rate: f64) -> Self {
        let num_items = num_items.max(1);
        // m = -(n * ln p) / (ln 2)^2
        let m = -((num_items as f64) * false_positive_rate.ln()) / (2.0f64.ln().powi(2));
        // Round m up to the next power of 2 for fast modulo
        let m = (m.ceil() as u64).next_power_of_two();

        // k = (m / n) * ln 2
        let k = ((m as f64) / (num_items as f64)) * 2.0f64.ln();
        let k = k.ceil() as u32;

        let num_u64s = ((m / 64) as usize).max(1);
        Self {
            bits: vec![0; num_u64s],
            num_hashes: k,
            bit_count: m,
        }
    }

    /// Insert an ID into the filter.
    pub fn insert(&mut self, id: &ID) {
        if self.bits.is_empty() {
            return;
        }
        let (h1, h2) = self.get_hashes(id);
        let mask = self.bit_count - 1;
        for i in 0..self.num_hashes {
            let bit_idx = h1.wrapping_add((i as u64).wrapping_mul(h2)) & mask;
            let word_idx = (bit_idx >> 6) as usize;
            let bit_in_word = (bit_idx & 63) as u32;
            self.bits[word_idx] |= 1u64 << bit_in_word;
        }
    }

    /// Returns `true` if the ID might be in the filter, `false` if it is definitely not.
    pub fn contains(&self, id: &ID) -> bool {
        if self.bits.is_empty() {
            return true;
        }
        let (h1, h2) = self.get_hashes(id);
        let mask = self.bit_count - 1;
        for i in 0..self.num_hashes {
            let bit_idx = h1.wrapping_add((i as u64).wrapping_mul(h2)) & mask;
            let word_idx = (bit_idx >> 6) as usize;
            let bit_in_word = (bit_idx & 63) as u32;
            if (self.bits[word_idx] & (1 << bit_in_word)) == 0 {
                return false;
            }
        }
        true
    }

    #[inline]
    fn get_hashes(&self, id: &ID) -> (u64, u64) {
        let h1 = u64::from_le_bytes(id.0[0..8].try_into().unwrap());
        let h2 = u64::from_le_bytes(id.0[8..16].try_into().unwrap());
        (h1, h2)
    }

    pub fn clear(&mut self) {
        self.bits.fill(0);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bloom_filter() {
        let mut bf = BloomFilter::new(1000, 0.01);
        let id1 = ID::from_content(b"hello");
        let id2 = ID::from_content(b"world");
        let id3 = ID::from_content(b"not in filter");

        bf.insert(&id1);
        bf.insert(&id2);

        assert!(bf.contains(&id1));
        assert!(bf.contains(&id2));
        assert!(!bf.contains(&id3));

        bf.clear();
        assert!(!bf.contains(&id1));
        assert!(!bf.contains(&id2));
    }
}
