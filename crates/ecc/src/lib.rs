#![forbid(unsafe_code)]

//! Reed-Solomon erasure coding over GF(2^8).
//!
//! Pure Rust implementation with no external runtime dependencies (except
//! `rayon` for parallel stripe processing). Uses the systematic Vandermonde
//! construction with reduction polynomial 0x11D, which is the standard
//! interchange format for GF(2^8) Reed-Solomon erasure coding.
//!
//! Wire-format compatible with `reed-solomon-erasure`, `klauspost/reedsolomon`
//! (Go), and Backblaze's Java implementation for individual codewords, with
//! built-in striping support for multi-megabyte payloads.
//!
//! The sidecar stores **only parity shards** — no data is duplicated on disk.
//! Stripes are encoded in parallel using rayon for multi-core acceleration.

mod galois;

pub use galois::Galois;

/// Reed-Solomon codec for a fixed data/parity split.
///
/// `K` is the number of data shards, `P` is the number of parity shards.
/// Any `K` of the `K + P` total shards can reconstruct the original data.
pub struct ReedSolomon {
    k: usize,
    p: usize,
    /// Flat P × K generator matrix stored as a single contiguous vector for cache locality.
    /// Coefficient at row `i` (0..p) and column `j` (0..k) is `generator[i * k + j]`.
    generator: Vec<u8>,
}

/// Errors that can occur during encoding or decoding.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Error {
    /// Not enough shards provided for reconstruction.
    TooFewShards { have: usize, need: usize },
    /// All shards must have the same length.
    InconsistentShardLength { a: usize, b: usize },
    /// The shard count is invalid.
    InvalidShardCount(usize),
    /// The shard length is invalid (0 bytes).
    InvalidShardLength,
    /// Matrix is singular and cannot be inverted.
    SingularMatrix,
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::TooFewShards { have, need } => {
                write!(f, "too few shards: have {have}, need {need}")
            }
            Error::InconsistentShardLength { a, b } => {
                write!(f, "inconsistent shard lengths: {a} vs {b}")
            }
            Error::InvalidShardCount(n) => write!(f, "invalid shard count: {n}"),
            Error::InvalidShardLength => write!(f, "shard length must be non-zero"),
            Error::SingularMatrix => write!(f, "singular matrix"),
        }
    }
}

impl std::error::Error for Error {}

/// Invert a k×k matrix over GF(2^8) using Gauss-Jordan elimination.
/// Returns None if singular.
fn invert_matrix(m: &[Vec<u8>]) -> Option<Vec<Vec<u8>>> {
    let n = m.len();
    debug_assert!(n > 0 && m.iter().all(|r| r.len() == n));

    // Build augmented matrix [m | I].
    let mut aug: Vec<Vec<u8>> = (0..n)
        .map(|i| {
            let mut row = Vec::with_capacity(2 * n);
            row.extend_from_slice(&m[i]);
            row.resize(2 * n, 0u8);
            row[n + i] = 1;
            row
        })
        .collect();

    for col in 0..n {
        // Find pivot.
        let pivot_row = (col..n).find(|&r| aug[r][col] != 0)?;
        aug.swap(col, pivot_row);

        let pivot = aug[col][col];
        let inv_pivot = Galois::inv(pivot);

        // Scale pivot row.
        for val in aug[col].iter_mut() {
            *val = Galois::mul(inv_pivot, *val);
        }

        // Eliminate column in all other rows without row cloning allocations.
        for row in 0..n {
            if row == col {
                continue;
            }
            let factor = aug[row][col];
            if factor == 0 {
                continue;
            }
            #[allow(clippy::needless_range_loop)]
            for c in 0..(2 * n) {
                let pivot_val = aug[col][c];
                aug[row][c] ^= Galois::mul(factor, pivot_val);
            }
        }
    }

    // Extract inverse from right half.
    Some((0..n).map(|i| aug[i][n..].to_vec()).collect())
}

impl ReedSolomon {
    #[inline]
    fn gen_row(&self, row: usize) -> &[u8] {
        let start = row * self.k;
        &self.generator[start..start + self.k]
    }

    /// Create a new codec with `k` data shards and `p` parity shards.
    ///
    /// Constructs a systematic Vandermonde generator matrix compatible with
    /// standard GF(2^8) Reed-Solomon implementations.
    pub fn new(k: usize, p: usize) -> Result<Self, Error> {
        if k == 0 || p == 0 || k + p > 256 {
            return Err(Error::InvalidShardCount(k + p));
        }

        // Build (k + p) × k Vandermonde matrix: V[i][j] = α^(i * j) where α = 0x02.
        let v: Vec<Vec<u8>> = (0..(k + p))
            .map(|i| (0..k).map(|j| Galois::exp((i * j) as u32)).collect())
            .collect();

        // Invert top k × k submatrix.
        let v_top: Vec<Vec<u8>> = v[..k].to_vec();
        let v_top_inv = invert_matrix(&v_top).ok_or(Error::SingularMatrix)?;

        // Multiply bottom p rows of V by V_top_inv: G = V_parity × V_top_inv.
        let mut generator = vec![0u8; p * k];
        for i in 0..p {
            for j in 0..k {
                let mut sum = 0u8;
                for m in 0..k {
                    sum ^= Galois::mul(v[k + i][m], v_top_inv[m][j]);
                }
                generator[i * k + j] = sum;
            }
        }

        Ok(Self { k, p, generator })
    }

    /// Number of data shards.
    pub fn data_shards(&self) -> usize {
        self.k
    }

    /// Number of parity shards.
    pub fn parity_shards(&self) -> usize {
        self.p
    }

    /// Total number of shards (data + parity).
    pub fn total_shards(&self) -> usize {
        self.k + self.p
    }

    /// Encode data shards into pre-allocated parity shard buffers in place (zero heap allocation).
    ///
    /// `data` must contain exactly `k` shards, each with the same non-zero length.
    /// `parity` must contain exactly `p` mutable shard slices of the same length.
    pub fn encode_into(&self, data: &[&[u8]], parity: &mut [&mut [u8]]) -> Result<(), Error> {
        if data.len() != self.k {
            return Err(Error::TooFewShards {
                have: data.len(),
                need: self.k,
            });
        }
        if parity.len() != self.p {
            return Err(Error::TooFewShards {
                have: parity.len(),
                need: self.p,
            });
        }

        let len = data[0].len();
        if len == 0 {
            return Err(Error::InvalidShardLength);
        }
        for shard in &data[1..] {
            if shard.len() != len {
                return Err(Error::InconsistentShardLength {
                    a: len,
                    b: shard.len(),
                });
            }
        }
        for shard in parity.iter_mut() {
            if shard.len() != len {
                return Err(Error::InconsistentShardLength {
                    a: len,
                    b: shard.len(),
                });
            }
            shard.fill(0);
        }

        for (i, parity_row) in parity.iter_mut().enumerate() {
            let row_coeffs = self.gen_row(i);
            for (j, data_shard) in data.iter().enumerate() {
                let coeff = row_coeffs[j];
                Galois::mul_add_scalar(parity_row, coeff, data_shard);
            }
        }

        Ok(())
    }

    /// Encode data shards into parity shards.
    ///
    /// `data` must contain exactly `k` shards, each with the same
    /// non-zero length. Returns `p` parity shards.
    pub fn encode(&self, data: &[&[u8]]) -> Result<Vec<Vec<u8>>, Error> {
        if data.len() != self.k {
            return Err(Error::TooFewShards {
                have: data.len(),
                need: self.k,
            });
        }
        let len = data[0].len();
        if len == 0 {
            return Err(Error::InvalidShardLength);
        }

        let mut parity = vec![vec![0u8; len]; self.p];
        let mut parity_refs: Vec<&mut [u8]> = parity.iter_mut().map(|v| v.as_mut_slice()).collect();
        self.encode_into(data, &mut parity_refs)?;
        Ok(parity)
    }

    /// Reconstruct missing data shards from available shards.
    ///
    /// `shards` is a slice of `Option<Vec<u8>>` with length `k + p`.
    /// Present shards are `Some(data)`, missing shards are `None`.
    /// At least `k` shards must be present.
    ///
    /// On success, all missing data shard entries (0..k) are filled.
    /// Missing parity shards are left as `None`.
    pub fn reconstruct(&self, shards: &mut [Option<Vec<u8>>]) -> Result<(), Error> {
        self.reconstruct_internal(shards, false)
    }

    /// Reconstruct missing data shards AND missing parity shards.
    ///
    /// At least `k` shards must be present. On success, all missing entries
    /// in `shards` (both data and parity) are reconstructed and populated.
    pub fn reconstruct_all(&self, shards: &mut [Option<Vec<u8>>]) -> Result<(), Error> {
        self.reconstruct_internal(shards, true)
    }

    fn reconstruct_internal(
        &self,
        shards: &mut [Option<Vec<u8>>],
        reconstruct_parity: bool,
    ) -> Result<(), Error> {
        let total = self.k + self.p;
        if shards.len() != total {
            return Err(Error::TooFewShards {
                have: shards.len(),
                need: total,
            });
        }

        // Count available shards and find missing data indices.
        let mut missing_data = Vec::new();
        let mut have_count = 0usize;
        for (i, shard) in shards.iter().enumerate().take(self.k) {
            if shard.is_some() {
                have_count += 1;
            } else {
                missing_data.push(i);
            }
        }
        for shard in shards.iter().skip(self.k).take(self.p) {
            if shard.is_some() {
                have_count += 1;
            }
        }

        if have_count < self.k {
            return Err(Error::TooFewShards {
                have: have_count,
                need: self.k,
            });
        }

        // Get shard length from any present shard.
        let len = shards
            .iter()
            .flatten()
            .next()
            .ok_or(Error::InvalidShardLength)?
            .len();
        if len == 0 {
            return Err(Error::InvalidShardLength);
        }

        if !missing_data.is_empty() {
            // Collect available shard indices (first k available).
            let mut available = Vec::with_capacity(self.k);
            for (i, shard) in shards.iter().enumerate() {
                if shard.is_some() {
                    available.push(i);
                }
                if available.len() == self.k {
                    break;
                }
            }

            // Build k×k decoding matrix.
            let mut dec = vec![vec![0u8; self.k]; self.k];
            for (r, &shard_idx) in available.iter().enumerate() {
                if shard_idx < self.k {
                    dec[r][shard_idx] = 1;
                } else {
                    let gen_row_idx = shard_idx - self.k;
                    dec[r].copy_from_slice(self.gen_row(gen_row_idx));
                }
            }

            // Invert the decoding matrix.
            let inv = invert_matrix(&dec).ok_or(Error::SingularMatrix)?;

            // Reconstruct each missing data shard.
            for &missing_idx in &missing_data {
                let mut result = vec![0u8; len];
                for (j, &shard_idx) in available.iter().enumerate() {
                    let coeff = inv[missing_idx][j];
                    if coeff == 0 {
                        continue;
                    }
                    let shard_data = shards[shard_idx]
                        .as_ref()
                        .expect("shard is present in `available`");
                    Galois::mul_add_scalar(&mut result, coeff, shard_data);
                }
                shards[missing_idx] = Some(result);
            }
        }

        if reconstruct_parity {
            // Reconstruct any missing parity shards from the complete data shards.
            let data_refs: Vec<&[u8]> = shards[..self.k]
                .iter()
                .map(|s| s.as_ref().expect("data shards now present").as_slice())
                .collect();
            let parity = self.encode(&data_refs)?;
            for (i, parity_shard) in parity.into_iter().enumerate() {
                if shards[self.k + i].is_none() {
                    shards[self.k + i] = Some(parity_shard);
                }
            }
        }

        Ok(())
    }
}

/// Shard size in bytes. Matches the OS page size and mapache block size.
pub const SHARD_SIZE: usize = 4096;

/// Magic header identifier for Mapache ECC sidecar files (`b"MECP"`).
///
/// Sidecars store **only parity shards** — no data is duplicated.
pub const MAGIC: [u8; 4] = *b"MECP";

/// Current format version of the ECC sidecar file.
pub const VERSION: u8 = 1;

/// Sidecar header size in bytes.
///
/// Format:
/// - `[0..4]`: `MAGIC` (`b"MECP"`)
/// - `[4]`: `VERSION` (`1`)
/// - `[5]`: `reserved` (`0`)
/// - `[6..8]`: `data_shards` (`u16 LE`) — K
/// - `[8..10]`: `parity_shards` (`u16 LE`) — P
/// - `[10..18]`: `original_len` (`u64 LE`)
/// - `[18..22]`: `stripe_count` (`u32 LE`)
pub const HEADER_SIZE: usize = 4 + 1 + 1 + 2 + 2 + 8 + 4; // 22 bytes

/// Description of a single stripe's layout within an ECC payload.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StripeLayout {
    pub data_shards: usize,
    pub parity_shards: usize,
    pub data_bytes: usize,
}

/// Compute stripe layouts for a given data length with fixed K and P.
///
/// All stripes use the same K and P. The last stripe may have fewer data bytes
/// but the same shard structure, so the ReedSolomon codec can be reused.
pub fn calculate_stripe_layouts(original_len: usize, k: usize, p: usize) -> Vec<StripeLayout> {
    if original_len == 0 || k == 0 || p == 0 {
        return Vec::new();
    }

    let total_data_shards = original_len.div_ceil(SHARD_SIZE);
    let stripe_count = total_data_shards.div_ceil(k);

    let mut layouts = Vec::with_capacity(stripe_count);
    let mut remaining_len = original_len;

    for _ in 0..stripe_count {
        let bytes_in_stripe = remaining_len.min(k * SHARD_SIZE);

        layouts.push(StripeLayout {
            data_shards: k,
            parity_shards: p,
            data_bytes: bytes_in_stripe,
        });

        remaining_len -= bytes_in_stripe;
    }

    layouts
}

/// Encode a blob into a parity-only sidecar payload.
///
/// Stores **only** the parity shards and header metadata. Does not duplicate
/// data on disk — the original data must be read separately for reconstruction.
/// Stripes are encoded in parallel using rayon.
pub fn ecc_encode(data: &[u8], k: usize, p: usize) -> Vec<u8> {
    if data.is_empty() || k == 0 || p == 0 {
        return Vec::new();
    }

    let layouts = calculate_stripe_layouts(data.len(), k, p);

    // All stripes share the same K and P. Build the codec once.
    let rs = std::sync::Arc::new(ReedSolomon::new(k, p).expect("valid k, p within stripe bounds"));

    let mut total_parity_size = 0usize;
    for s in &layouts {
        total_parity_size += s.parity_shards * SHARD_SIZE;
    }

    let mut out = Vec::with_capacity(HEADER_SIZE + total_parity_size);

    // Write header (22 bytes).
    out.extend_from_slice(&MAGIC);
    out.push(VERSION);
    out.push(0); // reserved
    out.extend_from_slice(&(k as u16).to_le_bytes());
    out.extend_from_slice(&(p as u16).to_le_bytes());
    out.extend_from_slice(&(data.len() as u64).to_le_bytes());
    out.extend_from_slice(&(layouts.len() as u32).to_le_bytes());

    // Pre-compute data offsets for each stripe (O(n) once).
    let offsets: Vec<usize> = layouts
        .iter()
        .scan(0, |acc, s| {
            let off = *acc;
            *acc += s.data_bytes;
            Some(off)
        })
        .collect();

    // Encode all stripes in parallel.
    use rayon::prelude::*;
    let stripe_parities: Vec<Vec<u8>> = layouts
        .par_iter()
        .enumerate()
        .map(|(idx, stripe)| {
            let data_offset = offsets[idx];

            // Allocate a single contiguous buffer for all data shards (one alloc).
            let shard_bytes = k * SHARD_SIZE;
            let mut shard_buf = vec![0u8; shard_bytes];
            let copy_len = stripe.data_bytes.min(data.len() - data_offset);
            shard_buf[..copy_len].copy_from_slice(&data[data_offset..data_offset + copy_len]);

            // Build shard references from the contiguous buffer.
            let data_refs: Vec<&[u8]> = shard_buf.chunks_exact(SHARD_SIZE).take(k).collect();

            // Encode directly into a single parity buffer.
            let parity_bytes = p * SHARD_SIZE;
            let mut parity_buf = vec![0u8; parity_bytes];
            {
                let mut parity_refs: Vec<&mut [u8]> =
                    parity_buf.chunks_exact_mut(SHARD_SIZE).take(p).collect();
                rs.encode_into(&data_refs, &mut parity_refs)
                    .expect("encode succeeds");
            }

            parity_buf
        })
        .collect();

    // Append parity in order.
    for chunk in &stripe_parities {
        out.extend_from_slice(chunk);
    }

    debug_assert_eq!(out.len(), HEADER_SIZE + total_parity_size);
    out
}

/// Decode (verify + repair) data using a parity-only sidecar.
///
/// Takes the original `data` and a parity-only `ecc_payload`. Verifies each
/// stripe's parity against the data. Returns a new `Vec<u8>` containing the
/// (potentially repaired) data.
pub fn ecc_decode(data: &[u8], ecc_payload: &[u8]) -> Result<Vec<u8>, EccDecodeError> {
    if ecc_payload.len() >= HEADER_SIZE && ecc_payload[0..4] == MAGIC {
        return ecc_decode_parity(data, ecc_payload);
    }

    Err(EccDecodeError::InvalidHeader)
}

fn ecc_decode_parity(data: &[u8], ecc_payload: &[u8]) -> Result<Vec<u8>, EccDecodeError> {
    let k = u16::from_le_bytes(ecc_payload[6..8].try_into().expect("slice length is 2")) as usize;
    let p = u16::from_le_bytes(ecc_payload[8..10].try_into().expect("slice length is 2")) as usize;
    let original_len =
        u64::from_le_bytes(ecc_payload[10..18].try_into().expect("slice length is 8")) as usize;
    let stripe_count =
        u32::from_le_bytes(ecc_payload[18..22].try_into().expect("slice length is 4")) as usize;

    if data.len() != original_len || k == 0 || p == 0 {
        return Err(EccDecodeError::InvalidHeader);
    }

    let layouts = calculate_stripe_layouts(original_len, k, p);
    if layouts.len() != stripe_count {
        return Err(EccDecodeError::InvalidHeader);
    }

    let mut expected_payload_size = HEADER_SIZE;
    for s in &layouts {
        expected_payload_size += s.parity_shards * SHARD_SIZE;
    }
    if ecc_payload.len() < expected_payload_size {
        return Err(EccDecodeError::PayloadTooShort);
    }

    let mut out = data.to_vec();
    let mut parity_offset = HEADER_SIZE;
    let mut data_offset = 0usize;

    // Pre-allocate shard buffers once — reused across all stripes.
    let max_k = layouts.iter().map(|s| s.data_shards).max().unwrap_or(0);
    let max_p = layouts.iter().map(|s| s.parity_shards).max().unwrap_or(0);
    let mut shard_bufs: Vec<Vec<u8>> = vec![vec![0u8; SHARD_SIZE]; max_k];
    let mut parity_bufs: Vec<Vec<u8>> = vec![vec![0u8; SHARD_SIZE]; max_p];
    let mut computed_parity: Vec<Vec<u8>> = vec![vec![0u8; SHARD_SIZE]; max_p];
    // Cache the codec — rebuild only when (k, p) changes.
    let mut cached_k = 0usize;
    let mut cached_p = 0usize;
    let mut cached_rs: Option<ReedSolomon> = None;

    for stripe in &layouts {
        let k = stripe.data_shards;
        let p = stripe.parity_shards;

        // Load data shards from the output buffer (may have been repaired).
        for (i, shard) in shard_bufs[..k].iter_mut().enumerate() {
            let start = data_offset + i * SHARD_SIZE;
            shard.fill(0);
            if start < out.len() {
                let end = (start + SHARD_SIZE).min(out.len());
                let len = end - start;
                shard[..len].copy_from_slice(&out[start..end]);
            }
        }

        // Load parity shards from the sidecar.
        for (i, shard) in parity_bufs[..p].iter_mut().enumerate() {
            let start = parity_offset + i * SHARD_SIZE;
            let end = start + SHARD_SIZE;
            shard.copy_from_slice(&ecc_payload[start..end]);
        }

        // Reuse or rebuild codec when (k, p) changes.
        if k != cached_k || p != cached_p {
            cached_rs = Some(ReedSolomon::new(k, p).map_err(|_| EccDecodeError::InvalidHeader)?);
            cached_k = k;
            cached_p = p;
        }
        let rs = cached_rs.as_ref().expect("codec always set before use");

        // Verify parity: compute expected parity from data and compare with stored.
        let data_refs: Vec<&[u8]> = shard_bufs[..k].iter().map(|s| s.as_slice()).collect();
        let mut computed_refs: Vec<&mut [u8]> = computed_parity[..p]
            .iter_mut()
            .map(|v| v.as_mut_slice())
            .collect();
        rs.encode_into(&data_refs, &mut computed_refs)
            .expect("encode succeeds");

        let parity_ok = computed_parity[..p]
            .iter()
            .zip(parity_bufs[..p].iter())
            .all(|(a, b)| a == b);

        if !parity_ok {
            // Parity mismatch — at least one data shard is corrupt.
            // Try marking each data shard as erased to locate and repair it.
            for erase_idx in 0..k {
                let mut shards: Vec<Option<Vec<u8>>> = Vec::with_capacity(k + p);
                for (i, shard) in shard_bufs[..k].iter().enumerate() {
                    if i == erase_idx {
                        shards.push(None);
                    } else {
                        shards.push(Some(shard.clone()));
                    }
                }
                for shard in &parity_bufs[..p] {
                    shards.push(Some(shard.clone()));
                }

                if rs.reconstruct(&mut shards).is_err() {
                    continue;
                }

                // Check if parity matches after reconstruction.
                let reconstructed: Vec<&[u8]> = shards[..k]
                    .iter()
                    .map(|s| s.as_ref().expect("reconstruct succeeded").as_slice())
                    .collect();
                let mut new_refs: Vec<&mut [u8]> = computed_parity[..p]
                    .iter_mut()
                    .map(|v| v.as_mut_slice())
                    .collect();
                rs.encode_into(&reconstructed, &mut new_refs)
                    .expect("encode succeeds");
                if computed_parity[..p]
                    .iter()
                    .zip(parity_bufs[..p].iter())
                    .all(|(a, b)| a == b)
                {
                    // Found the corrupt shard — write repaired data back.
                    let repaired_shard = shards[erase_idx].as_ref().expect("reconstruct succeeded");
                    let start = data_offset + erase_idx * SHARD_SIZE;
                    if start < out.len() {
                        let end = (start + SHARD_SIZE).min(out.len());
                        out[start..end].copy_from_slice(&repaired_shard[..end - start]);
                    }
                    break;
                }
            }
        }

        parity_offset += p * SHARD_SIZE;
        data_offset += stripe.data_bytes;
    }

    Ok(out)
}

/// Errors that can occur when decoding or repairing an ECC payload.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EccDecodeError {
    PayloadTooShort,
    InvalidHeader,
    ReconstructFailed(Error),
}

impl std::fmt::Display for EccDecodeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::PayloadTooShort => write!(f, "ECC payload too short"),
            Self::InvalidHeader => write!(f, "invalid ECC header"),
            Self::ReconstructFailed(err) => write!(f, "reconstruction failed: {err}"),
        }
    }
}

impl std::error::Error for EccDecodeError {}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn encode_into_test() {
        let rs = ReedSolomon::new(4, 2).unwrap();
        let d: Vec<Vec<u8>> = (0..4).map(|i| vec![i as u8 + 1; 128]).collect();
        let data_refs: Vec<&[u8]> = d.iter().map(|v| v.as_slice()).collect();

        let mut parity1 = vec![vec![0u8; 128]; 2];
        let mut parity_refs: Vec<&mut [u8]> =
            parity1.iter_mut().map(|v| v.as_mut_slice()).collect();
        rs.encode_into(&data_refs, &mut parity_refs).unwrap();

        let parity2 = rs.encode(&data_refs).unwrap();
        assert_eq!(parity1, parity2);
    }

    #[test]
    fn encode_decode_roundtrip() {
        let rs = ReedSolomon::new(4, 2).unwrap();
        let d: Vec<Vec<u8>> = (0..4).map(|i| vec![i as u8; 100]).collect();
        let data_refs: Vec<&[u8]> = d.iter().map(|v| v.as_slice()).collect();
        let parity = rs.encode(&data_refs).unwrap();
        assert_eq!(parity.len(), 2);

        let mut shards: Vec<Option<Vec<u8>>> = d
            .into_iter()
            .map(Some)
            .chain(parity.into_iter().map(Some))
            .collect();
        rs.reconstruct(&mut shards).unwrap();

        for (i, shard) in shards.iter().enumerate() {
            assert!(shard.is_some(), "shard {i} is None");
        }
    }

    #[test]
    fn reconstruct_one_missing_data() {
        let rs = ReedSolomon::new(4, 2).unwrap();
        let d: Vec<Vec<u8>> = vec![
            b"aaaa".to_vec(),
            b"bbbb".to_vec(),
            b"cccc".to_vec(),
            b"dddd".to_vec(),
        ];
        let data_refs: Vec<&[u8]> = d.iter().map(|v| v.as_slice()).collect();
        let parity = rs.encode(&data_refs).unwrap();

        let mut shards: Vec<Option<Vec<u8>>> = vec![
            Some(d[0].clone()),
            None,
            Some(d[2].clone()),
            Some(d[3].clone()),
            Some(parity[0].clone()),
            Some(parity[1].clone()),
        ];
        rs.reconstruct(&mut shards).unwrap();
        assert_eq!(shards[1].as_ref().unwrap().as_slice(), b"bbbb");
    }

    #[test]
    fn reconstruct_multiple_missing_data() {
        let rs = ReedSolomon::new(10, 4).unwrap();
        let original: Vec<Vec<u8>> = (0..10).map(|i| vec![i as u8; 4096]).collect();
        let data_refs: Vec<&[u8]> = original.iter().map(|v| v.as_slice()).collect();
        let parity = rs.encode(&data_refs).unwrap();

        let mut shards: Vec<Option<Vec<u8>>> = original
            .iter()
            .enumerate()
            .map(|(i, d)| {
                if i == 2 || i == 5 || i == 7 {
                    None
                } else {
                    Some(d.clone())
                }
            })
            .chain(
                parity
                    .into_iter()
                    .enumerate()
                    .map(|(i, p)| if i == 1 { None } else { Some(p) }),
            )
            .collect();

        rs.reconstruct(&mut shards).unwrap();

        for (i, orig) in original.iter().enumerate() {
            assert_eq!(
                shards[i].as_ref().unwrap().as_slice(),
                orig.as_slice(),
                "data shard {i} mismatch"
            );
        }
    }

    #[test]
    fn reconstruct_all_data_and_parity() {
        let rs = ReedSolomon::new(6, 3).unwrap();
        let original: Vec<Vec<u8>> = (0..6).map(|i| vec![(i * 10 + 1) as u8; 512]).collect();
        let data_refs: Vec<&[u8]> = original.iter().map(|v| v.as_slice()).collect();
        let original_parity = rs.encode(&data_refs).unwrap();

        let mut shards: Vec<Option<Vec<u8>>> = vec![
            Some(original[0].clone()),
            None, // missing data shard 1
            Some(original[2].clone()),
            None, // missing data shard 3
            Some(original[4].clone()),
            Some(original[5].clone()),
            Some(original_parity[0].clone()),
            None, // missing parity shard 1
            Some(original_parity[2].clone()),
        ];

        rs.reconstruct_all(&mut shards).unwrap();

        assert_eq!(shards[1].as_ref().unwrap(), &original[1]);
        assert_eq!(shards[3].as_ref().unwrap(), &original[3]);
        assert_eq!(shards[7].as_ref().unwrap(), &original_parity[1]);
    }

    #[test]
    fn reconstruct_large_shards() {
        let rs = ReedSolomon::new(10, 3).unwrap();
        let data: Vec<Vec<u8>> = (0..10).map(|i| vec![i as u8; 4096]).collect();
        let data_refs: Vec<&[u8]> = data.iter().map(|v| v.as_slice()).collect();
        let parity = rs.encode(&data_refs).unwrap();

        let mut shards: Vec<Option<Vec<u8>>> = data
            .into_iter()
            .map(Some)
            .chain(parity.into_iter().map(Some))
            .collect();
        shards[3] = None;
        rs.reconstruct(&mut shards).unwrap();
        assert_eq!(shards[3].as_ref().unwrap().len(), 4096);
        assert!(shards[3].as_ref().unwrap().iter().all(|&b| b == 3));
    }

    #[test]
    fn too_few_shards() {
        let rs = ReedSolomon::new(4, 2).unwrap();
        let mut shards: Vec<Option<Vec<u8>>> = vec![None; 6];
        assert!(matches!(
            rs.reconstruct(&mut shards),
            Err(Error::TooFewShards { .. })
        ));
    }

    #[test]
    fn ecc_encode_decode_roundtrip() {
        let data = vec![42u8; 12_000]; // ~3 shards
        let payload = ecc_encode(&data, 100, 50);
        assert!(!payload.is_empty());
        assert_eq!(&payload[0..4], &MAGIC);

        let decoded = ecc_decode(&data, &payload).unwrap();
        assert_eq!(decoded, data);
    }

    #[test]
    fn ecc_encode_empty() {
        let payload = ecc_encode(&[], 100, 50);
        assert!(payload.is_empty());
    }

    #[test]
    fn ecc_encode_zero_p() {
        let payload = ecc_encode(&[1, 2, 3], 100, 0);
        assert!(payload.is_empty());
    }

    #[test]
    fn ecc_decode_too_short() {
        let data = vec![0u8; 100];
        assert!(matches!(
            ecc_decode(&data, &[0; 5]),
            Err(EccDecodeError::InvalidHeader)
        ));
    }

    #[test]
    fn ecc_decode_invalid_header() {
        let data = vec![0u8; 100];
        let mut payload = vec![0u8; HEADER_SIZE + SHARD_SIZE + SHARD_SIZE];
        payload[0..4].copy_from_slice(&MAGIC);
        payload[4] = 99; // invalid version
        assert!(matches!(
            ecc_decode(&data, &payload),
            Err(EccDecodeError::InvalidHeader)
        ));
    }

    #[test]
    fn ecc_encode_decode_large_multi_mb_data() {
        // 16 MiB payload (standard Mapache pack size)
        let size = 16 * 1024 * 1024;
        let mut data = Vec::with_capacity(size);
        for i in 0..size {
            data.push(((i * 7 + 13) % 256) as u8);
        }

        let payload = ecc_encode(&data, 100, 20);
        assert!(!payload.is_empty());

        let decoded = ecc_decode(&data, &payload).unwrap();
        assert_eq!(decoded.len(), size);
        assert_eq!(decoded, data);
    }
}
