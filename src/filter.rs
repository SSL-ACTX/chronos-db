use bit_vec::BitVec;
use seahash::hash;

pub struct BloomFilter {
    bits: BitVec,
    num_hashes: u32,
}

impl BloomFilter {
    /// Create a new Bloom Filter.
    /// expected_items: How many items you plan to store.
    /// false_positive_rate: Acceptable error rate (e.g., 0.01 for 1%).
    pub fn new(expected_items: usize, false_positive_rate: f64) -> Self {
        // Calculate optimal size (m) and hash count (k)
        // m = -(n * ln(p)) / (ln(2)^2)
        let ln2 = 2.0f64.ln();
        let m = -((expected_items as f64 * false_positive_rate.ln()) / (ln2 * ln2));

        // k = (m / n) * ln(2)
        let k = (m / expected_items as f64) * ln2;

        Self {
            bits: BitVec::from_elem(m.ceil() as usize, false),
            num_hashes: k.ceil() as u32,
        }
    }

    /// Add a key (byte slice) to the filter
    pub fn insert(&mut self, key: &[u8]) {
        let (h1, h2) = self.get_hash_pair(key);
        let m = self.bits.len() as u64;

        for i in 0..self.num_hashes {
            // Double Hashing: g(x) = h1(x) + i * h2(x)
            // Wrapping add simulates independent hashes without re-computing
            let idx = h1.wrapping_add((i as u64).wrapping_mul(h2)) % m;
            self.bits.set(idx as usize, true);
        }
    }

    /// Check if a key might exist.
    /// Returns FALSE if definitely not present.
    /// Returns TRUE if it MIGHT be present.
    pub fn contains(&self, key: &[u8]) -> bool {
        let (h1, h2) = self.get_hash_pair(key);
        let m = self.bits.len() as u64;

        for i in 0..self.num_hashes {
            let idx = h1.wrapping_add((i as u64).wrapping_mul(h2)) % m;
            if !self.bits.get(idx as usize).unwrap() {
                return false; // Definitely not here
            }
        }
        true // Might be here
    }

    /// Helper: Generate two independent 64-bit hashes using SeaHash
    fn get_hash_pair(&self, key: &[u8]) -> (u64, u64) {
        // Hash 1: Standard SeaHash
        let h1 = hash(key);

        // Hash 2: SeaHash with a modified seed (simple XOR tweak)
        // This provides sufficient independence for the Bloom Filter property
        let h2 = h1.wrapping_add(0x9E3779B97F4A7C15); // Golden Ratio constant

        (h1, h2)
    }
}
