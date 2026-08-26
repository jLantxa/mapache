#![cfg_attr(
    test,
    allow(
        clippy::unwrap_used,
        clippy::unreachable,
        clippy::panic,
        clippy::panic_in_result_fn,
    )
)]

//! This is an implementation of the [FastCDC](https://ieeexplore.ieee.org/document/9055082)
//! algorithm by Wen Xia et al. published in 2020.
//! It implements the Content-Defined Chunking algorithm with all the five
//! optimization techniques described in the paper: Gear-based rolling hashing,
//! optimized hash judgment, sub-minimum chunk cut-point skipping, normalized
//! chunking and "rolling two bytes each time".
//!
//! The original paper suggests MD5 for the Gear lookup tables, although this
//! implementation uses a cryptographic hash for the sole purpose of reusing
//! an existing dependency.
//!
//! The masks are randomly generated to distribute the 'one' bits evenly between
//! bits 0..48.

use std::io::{self, Read};

use crate::lookup::{GEAR, GEAR_LS, MASKS};

mod lookup;

#[cfg(test)]
mod tests;

/// Errors that can occur during chunking.
#[derive(Debug)]
pub enum ChunkerError {
    /// An I/O error occurred while reading from the source.
    Io(io::Error),
}

impl std::fmt::Display for ChunkerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ChunkerError::Io(e) => write!(f, "I/O error: {e}"),
        }
    }
}

impl std::error::Error for ChunkerError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            ChunkerError::Io(e) => Some(e),
        }
    }
}

impl From<io::Error> for ChunkerError {
    fn from(e: io::Error) -> Self {
        ChunkerError::Io(e)
    }
}

/// A `Result` type alias using [`ChunkerError`].
pub type Result<T> = std::result::Result<T, ChunkerError>;

/// The absolute minimum chunk size in bytes (64 B).
pub const TOTAL_MIN_SIZE: usize = 64;
/// The absolute maximum chunk size in bytes (16 MiB).
pub const TOTAL_MAX_SIZE: usize = 16 * 1024 * 1024;
/// The smallest allowed normal (target) chunk size in bytes (256 B).
pub const MIN_NORMAL_SIZE: usize = 256;
/// The largest allowed normal (target) chunk size in bytes (4 MiB).
pub const MAX_NORMAL_SIZE: usize = 4 * 1024 * 1024;

/// Chunk normalization level.
///
/// Higher levels produce a narrower chunk size distribution around the normal
/// size, at the cost of making the cut-point condition stricter (fewer
/// positions pass the hash judgment). This can slightly reduce the
/// deduplication ratio because chunk boundaries are less likely to align with
/// natural content boundaries.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum Normalization {
    /// No normalization (widest distribution).
    None,
    /// Level 1: low normalization.
    L1,
    /// Level 2: medium normalization.
    L2,
    /// Level 3: aggressive normalization (narrowest distribution).
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

/// FastCDC chunker configured with size bounds and a normalization level.
///
/// Holds precomputed masks and references to the static Gear hash tables.
/// Construct via [`Chunker::new`], then call [`Chunker::cut`] on a buffer or
/// [`Chunker::stream`] on a [`Read`] source.
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
    /// Create a new `Chunker` with the given parameters.
    ///
    /// # Arguments
    ///
    /// * `min_size` — The minimum chunk size in bytes. Must be ≥ [`TOTAL_MIN_SIZE`].
    /// * `normal_size` — The target (normal) chunk size in bytes. Must be
    ///   between `min_size` and `max_size` (inclusive) and ≤ [`MAX_NORMAL_SIZE`].
    /// * `max_size` — The maximum chunk size in bytes. Must be ≥ `normal_size`
    ///   and ≤ [`TOTAL_MAX_SIZE`].
    /// * `normalization` — The normalization level (see [`Normalization`]).
    ///
    /// # Panics
    ///
    /// Panics if any of the following constraints are violated:
    /// - `min_size ≤ normal_size ≤ max_size`
    /// - `min_size ≥ TOTAL_MIN_SIZE` (64 B)
    /// - `max_size ≤ TOTAL_MAX_SIZE` (16 MiB)
    /// - `normal_size ≤ MAX_NORMAL_SIZE` (4 MiB)
    /// - `ilog2(normal_size) ± normalization.bits()` is within the mask table
    ///
    /// # Examples
    ///
    /// ```
    /// use mapache_chunker::{Chunker, Normalization};
    ///
    /// let chunker = Chunker::new(
    ///     512 * 1024,
    ///     1024 * 1024,
    ///     8 * 1024 * 1024,
    ///     Normalization::L2,
    /// );
    /// ```
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
        assert!(min_size.is_multiple_of(2), "min_size must be even");
        assert!(normal_size.is_multiple_of(2), "normal_size must be even");
        assert!(max_size.is_multiple_of(2), "max_size must be even");

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

    /// Find a cut point (chunk boundary) in a byte slice.
    ///
    /// Returns a tuple of `(hash, cut_point)` where `cut_point` is the index
    /// of the first byte past the cut, and `hash` is the gear fingerprint at
    /// that position. If no boundary is found within [`min_size`, `max_size`],
    /// returns `max_size` (or the slice length if shorter).
    ///
    /// ## Algorithm
    ///
    /// 1. **Sub-minimum skip** — If `len ≤ min_size`, return `len` immediately
    ///    (no hash checks below the minimum).
    /// 2. **Alignment** — If the start position is odd, a single byte is
    ///    consumed to align the loop to even indices.
    /// 3. **Phase 1** (small mask) — From `min_size` up to the normalization
    ///    center, use `mask_s` (more one-bits → more positions pass). Two bytes
    ///    are processed per iteration using both gear and gear_ls tables.
    /// 4. **Phase 2** (large mask) — From the center up to `max_size`, use
    ///    `mask_l` (fewer one-bits → fewer positions pass, guaranteeing
    ///    termination before or at `max_size`).
    /// 5. **Fallback** — Return `max` if no cut point was found.
    pub fn cut(&self, data: &[u8]) -> (u64, usize) {
        let len = data.len();
        if len <= self.min_size {
            return (0, len);
        }

        let max = len.min(self.max_size);
        let center = self.normal_size.min(max);
        let mid = center & !1;
        let end = max & !1;

        let search_slice = &data[..end];

        let mut fp: u64 = 0;
        let mut n: usize = self.min_size.min(max);

        // Phase 1 (small masks) up to the normalization center
        while n < mid {
            let b0 = search_slice[n];
            fp = (fp << 2).wrapping_add(self.gear_ls[b0 as usize]);
            if fp & self.mask_s_ls == 0 {
                return (fp, n);
            }

            let b1 = search_slice[n + 1];
            fp = fp.wrapping_add(self.gear[b1 as usize]);
            if fp & self.mask_s == 0 {
                return (fp, n + 1);
            }

            n += 2;
        }

        // Phase 2 (large masks) up to the hard max
        while n < end {
            let b0 = search_slice[n];
            fp = (fp << 2).wrapping_add(self.gear_ls[b0 as usize]);
            if fp & self.mask_l_ls == 0 {
                return (fp, n);
            }

            let b1 = search_slice[n + 1];
            fp = fp.wrapping_add(self.gear[b1 as usize]);
            if fp & self.mask_l == 0 {
                return (fp, n + 1);
            }

            n += 2;
        }

        // Fold any trailing odd byte into the hash so the fingerprint
        // reflects the full chunk (not just the even prefix).
        if max % 2 == 1 {
            fp = (fp << 1).wrapping_add(self.gear[data[max - 1] as usize]);
        }

        (fp, max)
    }

    /// Create a [`ChunkStream`] that yields chunks from a [`Read`] source.
    pub fn stream<R: Read>(&self, source: R) -> ChunkStream<'_, R> {
        ChunkStream::new(source, self, self.max_size)
    }
}

/// A single chunk produced by the chunker.
///
/// Contains the data together with its global offset and byte length.
pub struct Chunk {
    /// Global byte offset of this chunk in the original source stream.
    pub offset: usize,
    /// Number of bytes in this chunk.
    pub length: usize,
    /// The chunk payload.
    pub data: Vec<u8>,
}

/// Streaming iterator over chunks from a [`Read`] source.
///
/// Yields exactly-once reconstruction: concatenating all chunk data in order
/// reproduces the original source bytes. Each chunk (except possibly the last)
/// respects the chunker's min/max size constraints.
pub struct ChunkStream<'a, R: Read> {
    chunker: &'a Chunker,
    source: R,
    buffer: Vec<u8>,
    global_offset: usize,
}

impl<'a, R: Read> ChunkStream<'a, R> {
    /// Create a new chunk stream from a [`Read`] source.
    ///
    /// `initial_capacity` hints at the starting buffer size. The internal
    /// buffer grows as needed, up to `chunker.max_size`.
    pub fn new(source: R, chunker: &'a Chunker, initial_capacity: usize) -> Self {
        Self {
            chunker,
            source,
            buffer: Vec::with_capacity(initial_capacity.min(chunker.max_size)),
            global_offset: 0,
        }
    }
}

impl<'a, R: Read> Iterator for ChunkStream<'a, R> {
    type Item = Result<Chunk>;

    /// Read from the source until at least `min_size` bytes are buffered,
    /// then find a cut point and yield the chunk as a [`Chunk`].
    /// Flushes any remaining buffered data as the final chunk on EOF.
    fn next(&mut self) -> Option<Self::Item> {
        let max_size = self.chunker.max_size;
        let min_size = self.chunker.min_size;

        let mut eof = false;

        while self.buffer.len() < max_size {
            let cur_len = self.buffer.len();
            let needed = max_size - cur_len;

            if self.buffer.capacity() < max_size {
                self.buffer.reserve(needed);
            }

            let to_read = needed.min(self.buffer.capacity() - cur_len);

            let spare = self.buffer.spare_capacity_mut();
            let uninit = &mut spare[..to_read];
            // SAFETY: `u8` accepts any bit pattern; `read()` immediately
            // overwrites the uninitialized region before any read occurs.
            let buf =
                unsafe { std::slice::from_raw_parts_mut(uninit.as_mut_ptr() as *mut u8, to_read) };

            match self.source.read(buf) {
                Ok(0) => {
                    eof = true;
                    break;
                }
                // SAFETY: `cur_len + n` ≤ `to_read`, and we reserved enough
                // capacity above to fit `max_size` bytes. `n` is the number
                // of bytes actually written by `read()`, all within bounds.
                Ok(n) => unsafe {
                    self.buffer.set_len(cur_len + n);
                },
                Err(ref e) if e.kind() == std::io::ErrorKind::Interrupted => {
                    continue;
                }
                Err(e) => {
                    return Some(Err(ChunkerError::Io(e)));
                }
            }
        }

        if eof && self.buffer.is_empty() {
            return None;
        }

        if self.buffer.len() >= min_size {
            let slice = &self.buffer[..self.buffer.len().min(max_size)];
            let (_hash, cut_point) = self.chunker.cut(slice);

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
