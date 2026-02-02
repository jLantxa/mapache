//! This is an implementation of the [FastCDC](https://ieeexplore.ieee.org/document/9055082)
//! algorithm by Wen Xia et al published in 2020.
//! It implements the Content-Defined Chunking algorithm with all the five
//! optimization techniques described in the paper: Gear-based rolling hashing,
//! optimized hash judgment, sub-minimum chunk cut-point skipping, normalized
//! chunking and "rolling two bytes each time".
//!
//! The original paper suggests MD5 for the Gear lookup tables, although this
//! implementation uses BLAKE3 for the sole purpose of reusing an existing
//! dependency.
//!
//! The masks are randomly generated to distribute the 'one' bits evenly between
//! bits 0..48.

use std::io::Read;

use anyhow::{Result, anyhow};

use crate::lookup::{GEAR, GEAR_LS, MASKS};

mod lookup;

#[cfg(test)]
mod tests;

pub const TOTAL_MIN_SIZE: usize = 64; // 64 B
pub const TOTAL_MAX_SIZE: usize = 16 * 1024 * 1024; // 16 MiB
pub const MIN_NORMAL_SIZE: usize = 256; // 256 B
pub const MAX_NORMAL_SIZE: usize = 4 * 1024 * 1024; // 4 MiB

/// Chunk normalization level.
/// Higher normalization levels result in a narrower distribution of the chunk
/// sizes around the normal size.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum Normalization {
    /// No normalization.
    None,
    /// Level 1: low normalization.
    L1,
    /// Level 2: medium normalization.
    L2,
    /// Level 3: aggressive normalization.
    L3,
}

impl Normalization {
    const fn bits(&self) -> usize {
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
    normal_size: usize,
    max_size: usize,

    gear: &'static [u64; 256],
    gear_ls: &'static [u64; 256],

    mask_s: u64,
    mask_l: u64,
    mask_s_ls: u64,
    mask_l_ls: u64,
}

impl Chunker {
    /// Initialize a new Chunker with fixed parameters.
    pub const fn new(
        min_size: usize,
        normal_size: usize,
        max_size: usize,
        normalization: Normalization,
    ) -> Self {
        assert!(min_size <= normal_size);
        assert!(normal_size <= max_size);
        assert!(min_size >= TOTAL_MIN_SIZE);
        assert!(max_size <= TOTAL_MAX_SIZE);
        assert!(normal_size <= MAX_NORMAL_SIZE);

        let normal_bits = normal_size.ilog2() as usize;
        let norm_bits = normalization.bits();
        assert!(normal_bits + norm_bits <= MASKS.len());

        let mask_s = MASKS[normal_bits + norm_bits];
        let mask_l = MASKS[normal_bits - norm_bits];
        let mask_s_ls = mask_s << 1;
        let mask_l_ls = mask_l << 1;

        Self {
            min_size,
            normal_size,
            max_size,
            gear: &GEAR,
            gear_ls: &GEAR_LS,
            mask_s,
            mask_l,
            mask_s_ls,
            mask_l_ls,
        }
    }

    /// Find a cut point in a slice of bytes.
    pub fn cut(&self, data: &[u8]) -> usize {
        let len = data.len();
        if len <= self.min_size {
            return len;
        }

        let max = len.min(self.max_size);
        let center = self.normal_size.min(max);
        let mid = center & !1;
        let end = max & !1;

        let search_slice = &data[..end];

        let mut fp: u64 = 0;
        let mut n: usize = (self.min_size & !1).min(max);

        // Phase 1 (small masks) up to the normalization center
        while n < mid {
            let b0 = search_slice[n];
            fp = (fp << 2).wrapping_add(self.gear_ls[b0 as usize]);
            if fp & self.mask_s_ls == 0 {
                return n;
            }

            let b1 = search_slice[n + 1];
            fp = fp.wrapping_add(self.gear[b1 as usize]);
            if fp & self.mask_s == 0 {
                return n + 1;
            }

            n += 2;
        }

        // Phase 2 (large masks) up to the hard max
        while n < end {
            let b0 = search_slice[n];
            fp = (fp << 2).wrapping_add(self.gear_ls[b0 as usize]);
            if fp & self.mask_l_ls == 0 {
                return n;
            }

            let b1 = search_slice[n + 1];
            fp = fp.wrapping_add(self.gear[b1 as usize]);
            if fp & self.mask_l == 0 {
                return n + 1;
            }

            n += 2;
        }

        max
    }

    /// Returns a stream of the chunks of a Read trait object.
    pub fn stream<R: Read>(&self, source: R) -> ChunkStream<'_, R> {
        ChunkStream::new(source, self)
    }
}

pub struct Chunk {
    pub offset: usize,
    pub length: usize,
    pub data: Vec<u8>,
}

/// Chunk stream.
///
/// A chunk stream is an iterator over a Read object that produces a new
/// chunk every time the next() function is called.
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
    type Item = Result<Chunk>;

    /// Finds the next cut-point and returns a Chunk until the data is exhausted.
    fn next(&mut self) -> Option<Self::Item> {
        let max_size = self.chunker.max_size;
        let min_size = self.chunker.min_size;

        let mut eof = false;

        while self.buffer.len() < max_size {
            let cur_len = self.buffer.len();
            let needed = max_size - cur_len;

            let spare = self.buffer.spare_capacity_mut();
            let to_read = needed.min(spare.len());

            let buf = unsafe {
                // SAFETY: We bypass Rust's default rule of filling new memory with zeros.
                // Because we are immediately handing this memory to the OS to be filled
                // with data from the file, zeroing it first would be a waste of CPU cycles.
                std::slice::from_raw_parts_mut(spare.as_mut_ptr() as *mut u8, to_read)
            };

            match self.source.read(buf) {
                Ok(0) => {
                    eof = true;
                    break;
                }
                Ok(n) => {
                    unsafe {
                        // SAFETY: We now tell the Vector that 'n' bytes are no longer "junk"
                        // memory but are now valid data bytes received from the reader.
                        self.buffer.set_len(cur_len + n);
                    }

                    if n < to_read {
                        eof = true;
                    }
                }
                Err(ref e) if e.kind() == std::io::ErrorKind::Interrupted => {
                    continue;
                }
                Err(e) => {
                    return Some(Err(anyhow!("Read error: {e}")));
                }
            }
        }

        if eof && self.buffer.is_empty() {
            return None;
        }

        if self.buffer.len() >= min_size {
            let slice = &self.buffer[..self.buffer.len().min(max_size)];
            let cut_point = self.chunker.cut(slice);

            if cut_point > 0 {
                let data: Vec<u8> = self.buffer.drain(..cut_point).collect();
                let offset = self.global_offset;
                self.global_offset += cut_point;

                return Some(Ok(Chunk {
                    offset,
                    length: cut_point,
                    data,
                }));
            }
        }

        if eof && !self.buffer.is_empty() {
            let length = self.buffer.len();
            let offset = self.global_offset;
            let data = std::mem::take(&mut self.buffer);

            self.global_offset += length;

            return Some(Ok(Chunk {
                offset,
                length,
                data,
            }));
        }

        None
    }
}
