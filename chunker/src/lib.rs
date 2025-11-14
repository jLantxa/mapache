use std::io::Read;

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

        let mask_s = MASKS[avg_bits + norm_bits];
        let mask_l = MASKS[avg_bits - norm_bits];
        let mask_s_ls = mask_s << 1;
        let mask_l_ls = mask_l << 1;

        Self {
            min_size,
            avg_size,
            max_size,
            gear: &GEAR,
            gear_ls: &GEAR_LS,
            mask_s,
            mask_l,
            mask_s_ls,
            mask_l_ls,
        }
    }

    #[inline(always)]
    fn update_fp_even(&self, fp: u64, byte: u8) -> u64 {
        (fp << 2).wrapping_add(self.gear_ls[byte as usize])
    }

    #[inline(always)]
    fn update_fp_odd(&self, fp: u64, byte: u8) -> u64 {
        fp.wrapping_add(self.gear[byte as usize])
    }

    /// Find a cut point in a slice of bytes.
    #[inline(always)]
    pub(crate) fn cut(&self, data: &[u8]) -> usize {
        let len = data.len();
        if len <= self.min_size {
            return len;
        }

        let fp_mask_s = self.mask_s;
        let fp_mask_s_ls = self.mask_s_ls;
        let fp_mask_l = self.mask_l;
        let fp_mask_l_ls = self.mask_l_ls;

        let center = self.avg_size.min(len);
        let max_cap = self.max_size.min(len);

        let mut fp: u64 = 0;

        // i points to *pair index*, real byte index = i*2
        let mut i = self.min_size >> 1;

        let phase1_end = center >> 1;
        let phase2_end = max_cap >> 1;

        while i < phase1_end {
            let idx = i << 1;

            let b0 = unsafe { *data.get_unchecked(idx) };
            fp = self.update_fp_even(fp, b0);
            if (fp & fp_mask_s_ls) == 0 {
                return idx;
            }

            let b1 = unsafe { *data.get_unchecked(idx + 1) };
            fp = self.update_fp_odd(fp, b1);
            if (fp & fp_mask_s) == 0 {
                return idx + 1;
            }

            i += 1;
        }

        while i < phase2_end {
            let idx = i << 1;

            let b0 = unsafe { *data.get_unchecked(idx) };
            fp = self.update_fp_even(fp, b0);
            if (fp & fp_mask_l_ls) == 0 {
                return idx;
            }

            let b1 = unsafe { *data.get_unchecked(idx + 1) };
            fp = self.update_fp_odd(fp, b1);
            if (fp & fp_mask_l) == 0 {
                return idx + 1;
            }

            i += 1;
        }

        max_cap
    }

    pub fn chunk_stream<R: Read>(&self, source: R) -> ChunkStream<'_, R> {
        ChunkStream::new(source, self)
    }
}

pub struct Chunk {
    pub offset: usize,
    pub length: usize,
    pub data: Vec<u8>,
}

pub struct ChunkStream<'a, R: Read> {
    chunker: &'a Chunker,
    source: R,
    buffer: Vec<u8>,
    global_offset: usize,
}

impl<'a, R: Read> ChunkStream<'a, R> {
    fn new(source: R, chunker: &'a Chunker) -> Self {
        Self {
            chunker,
            source,
            buffer: Vec::with_capacity(chunker.max_size),
            global_offset: 0,
        }
    }
}

impl<'a, R: Read> Iterator for ChunkStream<'a, R> {
    type Item = Chunk;

    fn next(&mut self) -> Option<Self::Item> {
        let max_size = self.chunker.max_size;
        let min_size = self.chunker.min_size;

        let mut eof = false;

        while self.buffer.len() < max_size {
            let cur_len = self.buffer.len();
            let needed = max_size - cur_len;

            let spare = self.buffer.spare_capacity_mut();
            let to_read = needed.min(spare.len());

            let buf =
                unsafe { std::slice::from_raw_parts_mut(spare.as_mut_ptr() as *mut u8, to_read) };

            match self.source.read(buf) {
                Ok(0) => {
                    eof = true;
                    break;
                }
                Ok(n) => {
                    unsafe {
                        self.buffer.set_len(cur_len + n);
                    }

                    if n < to_read {
                        eof = true;
                    }
                }
                Err(ref e) if e.kind() == std::io::ErrorKind::Interrupted => continue,
                Err(e) => {
                    eprintln!("Read error: {e}");
                    return None;
                }
            }
        }

        if eof && self.buffer.is_empty() {
            return None;
        }

        if self.buffer.len() >= min_size {
            let slice = &self.buffer[..self.buffer.len().min(max_size)];
            let cut = self.chunker.cut(slice);

            if cut > 0 {
                let data: Vec<u8> = self.buffer.drain(..cut).collect();
                let offset = self.global_offset;
                self.global_offset += cut;

                return Some(Chunk {
                    offset,
                    length: cut,
                    data,
                });
            }
        }

        if eof && !self.buffer.is_empty() {
            let length = self.buffer.len();
            let offset = self.global_offset;
            let data = std::mem::take(&mut self.buffer);

            self.global_offset += length;

            return Some(Chunk {
                offset,
                length,
                data,
            });
        }

        None
    }
}

#[cfg(test)]
mod tests {}
