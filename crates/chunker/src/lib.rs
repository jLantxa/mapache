use crate::lookup::{GEAR, GEAR_LS, MASKS};

mod lookup;

pub enum Normalization {
    None,
    L1,
    L2,
    L3,
}

impl Normalization {
    pub fn to_bits(&self) -> usize {
        match self {
            Normalization::None => 0,
            Normalization::L1 => 1,
            Normalization::L2 => 2,
            Normalization::L3 => 3,
        }
    }
}

pub struct Chunker {
    min_size: usize,
    avg_size: usize,
    max_size: usize,
    normalization: Normalization,

    gear: &'static [u64; 256],
    gear_ls: &'static [u64; 256],

    mask_s: u64,
    mask_l: u64,
    mask_s_ls: u64,
    mask_l_ls: u64,
}

impl Chunker {
    /// Initialize a new Chunker with fix parameters.
    pub fn new(
        min_size: usize,
        avg_size: usize,
        max_size: usize,
        normalization: Normalization,
    ) -> Self {
        let avg_bits = avg_size.ilog2() as usize;
        let norm_bits = normalization.to_bits();

        let mask_s = MASKS[1 + avg_bits - norm_bits];
        let mask_l = MASKS[1 + avg_bits + norm_bits];
        let mask_s_ls = mask_s << 1;
        let mask_l_ls = mask_l << 1;

        Self {
            min_size,
            avg_size,
            max_size,
            normalization,
            gear: &GEAR,
            gear_ls: &GEAR_LS,
            mask_s,
            mask_l,
            mask_s_ls,
            mask_l_ls,
        }
    }

    /// Find a cut point in a slice of bytes.
    pub(crate) fn cut(&self, data: &[u8]) -> usize {
        let mut fp: u64 = 0;
        let remaining: usize = data.len();

        if remaining <= self.min_size {
            return remaining;
        }

        let mut max_cap = self.max_size;
        if remaining < max_cap {
            max_cap = remaining;
        }
        let mut center = self.avg_size;
        if max_cap < center {
            center = max_cap;
        }

        let mut i: usize = self.min_size;

        // Normalized Chunking Phase 1: Small Mask (Harder to find cut)
        // This loop runs from min_size up to the center/average size.
        // We stop at (center / 2) because the loop processes 2 bytes at a time.
        while i < (center / 2) {
            let i2 = i << 1;

            // Even byte
            fp = (fp << 2).wrapping_add(self.gear[data[i2] as usize]);
            if (fp & self.mask_s_ls) == 0 {
                return i2;
            }

            // Odd byte
            fp = fp.wrapping_add(self.gear_ls[data[i2 + 1] as usize]);
            if (fp & self.mask_s) == 0 {
                return i2 + 1;
            }

            i += 1;
        }

        // Normalized Chunking Phase 2: Large Mask (Easier to find cut)
        // This loop runs from the center size up to the determined max_cap.
        // We stop at (max_cap / 2) because the loop processes 2 bytes at a time.
        while i < (max_cap / 2) {
            let i2 = i << 1;

            // Even byte
            fp = (fp << 2).wrapping_add(self.gear[data[i2] as usize]);
            if (fp & self.mask_l_ls) == 0 {
                return i2;
            }

            // Odd byte
            fp = fp.wrapping_add(self.gear_ls[data[i2 + 1] as usize]);
            if (fp & self.mask_l) == 0 {
                return i2 + 1;
            }

            i += 1;
        }

        max_cap
    }
}
