//! Reed-Solomon erasure coding over GF(2^8).
//!
//! Uses the systematic [Vandermonde] construction with reduction polynomial 0x11D,
//! which is the standard interchange format for GF(2^8) Reed-Solomon erasure coding.
//!
//! [Vandermonde]: https://en.wikipedia.org/wiki/Vandermonde_matrix

mod galois;

use galois::Galois;

/// Reed-Solomon codec for a fixed data/parity split.
///
/// `K` is the number of data shards, `P` is the number of parity shards.
/// Any `K` of the `K + P` total shards can reconstruct the original data.
pub(super) struct ReedSolomon {
    k: usize,
    p: usize,
    /// Flat P × K generator matrix stored as a single contiguous vector for cache locality.
    /// Coefficient at row `i` (0..p) and column `j` (0..k) is `generator[i * k + j]`.
    generator: Vec<u8>,
}

/// Errors that can occur during encoding or decoding.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum Error {
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
    /// Constructs a systematic [Vandermonde] generator matrix compatible with
    /// standard GF(2^8) Reed-Solomon implementations.
    ///
    /// [Vandermonde]: https://en.wikipedia.org/wiki/Vandermonde_matrix
    pub(super) fn new(k: usize, p: usize) -> Result<Self, Error> {
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

    /// Encode data shards into pre-allocated parity shard buffers in place (zero heap allocation).
    ///
    /// `data` must contain exactly `k` shards, each with the same non-zero length.
    /// `parity` must contain exactly `p` mutable shard slices of the same length.
    pub(super) fn encode_into(
        &self,
        data: &[&[u8]],
        parity: &mut [&mut [u8]],
    ) -> Result<(), Error> {
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

    /// Reconstruct missing data shards from available shards.
    ///
    /// `shards` is a slice of `Option<Vec<u8>>` with length `k + p`.
    /// Present shards are `Some(data)`, missing shards are `None`.
    /// At least `k` shards must be present.
    ///
    /// On success, all missing data shard entries (0..k) are filled.
    /// Missing parity shards are left as `None`.
    pub(super) fn reconstruct(&self, shards: &mut [Option<Vec<u8>>]) -> Result<(), Error> {
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

        Ok(())
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    fn encode(rs: &ReedSolomon, data: &[&[u8]]) -> Vec<Vec<u8>> {
        let len = data[0].len();
        let mut parity = vec![vec![0u8; len]; rs.p];
        let mut parity_refs: Vec<&mut [u8]> = parity.iter_mut().map(|v| v.as_mut_slice()).collect();
        rs.encode_into(data, &mut parity_refs).unwrap();
        parity
    }

    #[test]
    fn encode_into_test() {
        let rs = ReedSolomon::new(4, 2).unwrap();
        let d: Vec<Vec<u8>> = (0..4).map(|i| vec![i as u8 + 1; 128]).collect();
        let data_refs: Vec<&[u8]> = d.iter().map(|v| v.as_slice()).collect();

        let mut parity1 = vec![vec![0u8; 128]; 2];
        let mut parity_refs: Vec<&mut [u8]> =
            parity1.iter_mut().map(|v| v.as_mut_slice()).collect();
        rs.encode_into(&data_refs, &mut parity_refs).unwrap();

        let parity2 = encode(&rs, &data_refs);
        assert_eq!(parity1, parity2);
    }

    #[test]
    fn encode_decode_roundtrip() {
        let rs = ReedSolomon::new(4, 2).unwrap();
        let d: Vec<Vec<u8>> = (0..4).map(|i| vec![i as u8; 100]).collect();
        let data_refs: Vec<&[u8]> = d.iter().map(|v| v.as_slice()).collect();
        let parity = encode(&rs, &data_refs);
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
        let parity = encode(&rs, &data_refs);

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
        let parity = encode(&rs, &data_refs);

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
    fn reconstruct_large_shards() {
        let rs = ReedSolomon::new(10, 3).unwrap();
        let data: Vec<Vec<u8>> = (0..10).map(|i| vec![i as u8; 4096]).collect();
        let data_refs: Vec<&[u8]> = data.iter().map(|v| v.as_slice()).collect();
        let parity = encode(&rs, &data_refs);

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
}
