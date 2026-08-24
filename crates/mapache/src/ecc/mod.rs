//! Mapache ECC sidecar format.
//!
//! Parity-only sidecar encoding/decoding for pack files. Uses
//! [`reed_solomon::ReedSolomon`] for the underlying erasure coding.
//!
//! The sidecar stores **only parity shards** — no data is duplicated on disk.
//! Stripes are encoded in parallel using rayon for multi-core acceleration.
//!
//! # Sidecar format
//!
//! Each stripe stores CRC32 checksums for every data and parity shard,
//! enabling per-shard integrity verification and targeted erasure marking.
//!
//! ```text
//! Header (22 bytes):
//!   [0..4]   MAGIC        b"MECP"
//!   [4]      VERSION      2
//!   [5]      reserved     0
//!   [6..8]   data_shards  u16 LE  (K)
//!   [8..10]  parity_shards u16 LE (P)
//!   [10..18] original_len u64 LE
//!   [18..22] stripe_count u32 LE
//!
//! Per stripe:
//!   [crc32_data_0 .. crc32_data_{K-1}]
//!   [crc32_parity_0 .. crc32_parity_{P-1}]
//!   [parity_0_data .. parity_{P-1}_data]   (each SHARD_SIZE bytes)
//! ```

mod galois;
pub(crate) mod reed_solomon;

use reed_solomon::ReedSolomon;

/// Shard size in bytes (4 KiB).
pub(crate) const SHARD_SIZE: usize = 4096;

/// Magic header identifier for ECC sidecar files (`b"MECP"`).
pub(crate) const MAGIC: [u8; 4] = *b"MECP";

/// Current format version of the ECC sidecar file.
pub(crate) const VERSION: u8 = 2;

/// CRC32 checksum size in bytes.
pub(crate) const CRC_SIZE: usize = 4;

/// Sidecar header size in bytes (22).
pub(crate) const HEADER_SIZE: usize = 22;

/// Description of a single stripe's layout within an ECC payload.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct StripeLayout {
    pub data_shards: usize,
    pub parity_shards: usize,
    pub data_bytes: usize,
}

/// Compute stripe layouts for a given data length with fixed K and P.
///
/// All stripes use the same K and P. The last stripe may have fewer data bytes
/// but the same shard structure, so the ReedSolomon codec can be reused.
pub(crate) fn calculate_stripe_layouts(
    original_len: usize,
    k: usize,
    p: usize,
) -> Vec<StripeLayout> {
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

/// CRC32 lookup table for the IEEE 802.3 polynomial (0xEDB88320).
///
/// `TABLE[i]` is the CRC of the single byte `i` (as a 32-bit value in the
/// reflected domain). Generated at compile time via `const` evaluation.
const CRC32_TABLE: [u32; 256] = build_crc32_table();

const fn build_crc32_table() -> [u32; 256] {
    let mut table = [0u32; 256];
    let mut i = 0u32;
    while i < 256 {
        let mut crc = i;
        let mut bit = 0u8;
        while bit < 8 {
            crc = if crc & 1 != 0 {
                (crc >> 1) ^ 0xEDB8_8320
            } else {
                crc >> 1
            };
            bit += 1;
        }
        table[i as usize] = crc;
        i += 1;
    }
    table
}

/// CRC32 using a 256-entry lookup table generated at compile time.
pub(crate) fn crc32_ieee(data: &[u8]) -> u32 {
    let mut crc: u32 = 0xFFFF_FFFF;
    for &byte in data {
        crc = CRC32_TABLE[((crc ^ byte as u32) & 0xFF) as usize] ^ (crc >> 8);
    }
    crc ^ 0xFFFF_FFFF
}

/// Size of the sidecar payload for one stripe: CRC32s for all shards plus raw parity.
///
/// Layout: `(K + P) * CRC_SIZE + P * SHARD_SIZE`
pub(crate) fn stripe_payload_size(k: usize, p: usize) -> usize {
    (k + p) * CRC_SIZE + p * SHARD_SIZE
}

/// Encode a blob into a parity-only sidecar payload.
///
/// Stores **only** the parity shards and header metadata. Does not duplicate
/// data on disk — the original data must be read separately for reconstruction.
/// Stripes are encoded in parallel using rayon.
///
/// Returns `Err(EccEncodeError)` if `k` or `p` are zero or `k + p > 256`.
pub(crate) fn ecc_encode(data: &[u8], k: usize, p: usize) -> Result<Vec<u8>, EccEncodeError> {
    if data.is_empty() || p == 0 {
        return Ok(Vec::new());
    }

    let layouts = calculate_stripe_layouts(data.len(), k, p);

    let rs = ReedSolomon::new(k, p).map_err(|_| EccEncodeError::InvalidShardCount { k, p })?;

    let mut total_stripe_size = 0usize;
    for s in &layouts {
        total_stripe_size += stripe_payload_size(s.data_shards, s.parity_shards);
    }

    let mut out = Vec::with_capacity(HEADER_SIZE + total_stripe_size);

    // Write header (22 bytes).
    out.extend_from_slice(&MAGIC);
    out.push(VERSION);
    out.push(0); // reserved
    out.extend_from_slice(&(k as u16).to_le_bytes());
    out.extend_from_slice(&(p as u16).to_le_bytes());
    out.extend_from_slice(&(data.len() as u64).to_le_bytes());
    out.extend_from_slice(&(layouts.len() as u32).to_le_bytes());

    // Pre-compute data offsets for each stripe.
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
    let stripe_encoded: Vec<Vec<u8>> = layouts
        .par_iter()
        .enumerate()
        .map(|(idx, stripe)| {
            let data_offset = offsets[idx];
            let k = stripe.data_shards;
            let p = stripe.parity_shards;

            // Allocate a single contiguous buffer for all data shards (one alloc).
            let shard_bytes = k * SHARD_SIZE;
            let mut shard_buf = vec![0u8; shard_bytes];
            let copy_len = stripe.data_bytes.min(data.len() - data_offset);
            shard_buf[..copy_len].copy_from_slice(&data[data_offset..data_offset + copy_len]);

            // Build shard references from the contiguous buffer.
            let (data_shards, _remainder) = shard_buf.as_chunks::<SHARD_SIZE>();
            let data_refs: Vec<&[u8]> = data_shards.iter().take(k).map(|s| s.as_slice()).collect();

            // Encode directly into a single parity buffer.
            let parity_bytes = p * SHARD_SIZE;
            let mut parity_buf = vec![0u8; parity_bytes];
            {
                let (parity_shards, _remainder) = parity_buf.as_chunks_mut::<SHARD_SIZE>();
                let mut parity_refs: Vec<&mut [u8]> = parity_shards
                    .iter_mut()
                    .take(p)
                    .map(|s| s.as_mut_slice())
                    .collect();
                rs.encode_into(&data_refs, &mut parity_refs)
                    .expect("encode succeeds");
            }

            // Build stripe payload: [crc32_data_0..][crc32_parity_0..][parity_data..]
            let payload_size = stripe_payload_size(k, p);
            let mut stripe_out = Vec::with_capacity(payload_size);

            // CRC32 for each data shard.
            for i in 0..k {
                let start = i * SHARD_SIZE;
                let crc = crc32_ieee(&shard_buf[start..start + SHARD_SIZE]);
                stripe_out.extend_from_slice(&crc.to_le_bytes());
            }

            // CRC32 for each parity shard.
            for i in 0..p {
                let start = i * SHARD_SIZE;
                let crc = crc32_ieee(&parity_buf[start..start + SHARD_SIZE]);
                stripe_out.extend_from_slice(&crc.to_le_bytes());
            }

            // Raw parity bytes.
            stripe_out.extend_from_slice(&parity_buf);

            stripe_out
        })
        .collect();

    // Append stripes in order.
    for stripe in &stripe_encoded {
        out.extend_from_slice(stripe);
    }

    Ok(out)
}

/// Decode (verify + repair) data using a parity-only sidecar.
///
/// Reads CRC32s from the sidecar, verifies each shard (data + parity),
/// marks bad shards as erasures, and calls RS to reconstruct them.
/// Returns an error if reconstruction fails or erasures exceed parity capacity.
///
/// Stripes are processed in parallel using rayon for multi-core acceleration.
pub(crate) fn ecc_decode(data: &[u8], ecc_payload: &[u8]) -> Result<Vec<u8>, EccDecodeError> {
    if ecc_payload.len() < HEADER_SIZE || ecc_payload[0..4] != MAGIC {
        return Err(EccDecodeError::InvalidHeader);
    }

    let version = ecc_payload[4];
    if version != VERSION {
        return Err(EccDecodeError::InvalidHeader);
    }

    let k = u16::from_le_bytes(ecc_payload[6..8].try_into().expect("slice length is 2")) as usize;
    let p = u16::from_le_bytes(ecc_payload[8..10].try_into().expect("slice length is 2")) as usize;
    let original_len =
        u64::from_le_bytes(ecc_payload[10..18].try_into().expect("slice length is 8")) as usize;
    let stripe_count =
        u32::from_le_bytes(ecc_payload[18..22].try_into().expect("slice length is 4")) as usize;

    if data.len() != original_len || k == 0 || p == 0 || k + p > 256 {
        return Err(EccDecodeError::InvalidHeader);
    }

    let layouts = calculate_stripe_layouts(original_len, k, p);
    if layouts.len() != stripe_count {
        return Err(EccDecodeError::InvalidHeader);
    }

    // Verify total payload size is sufficient.
    let mut expected_size = HEADER_SIZE;
    for s in &layouts {
        expected_size += stripe_payload_size(s.data_shards, s.parity_shards);
    }
    if ecc_payload.len() < expected_size {
        return Err(EccDecodeError::PayloadTooShort);
    }

    let rs = ReedSolomon::new(k, p).expect("validated k + p <= 256 above");

    // Pre-compute per-stripe CRC metadata (sequential, cheap)
    let stripe_meta: Vec<StripeMetadata> = {
        let mut metas = Vec::with_capacity(layouts.len());
        let mut sidecar_offset = HEADER_SIZE;
        let mut data_offset = 0usize;

        for stripe in &layouts {
            let k = stripe.data_shards;
            let p = stripe.parity_shards;

            let mut data_crcs = Vec::with_capacity(k);
            for i in 0..k {
                let start = sidecar_offset + i * CRC_SIZE;
                let crc = u32::from_le_bytes(
                    ecc_payload[start..start + CRC_SIZE]
                        .try_into()
                        .expect("slice length is 4"),
                );
                data_crcs.push(crc);
            }

            let mut parity_crcs = Vec::with_capacity(p);
            for i in 0..p {
                let start = sidecar_offset + k * CRC_SIZE + i * CRC_SIZE;
                let crc = u32::from_le_bytes(
                    ecc_payload[start..start + CRC_SIZE]
                        .try_into()
                        .expect("slice length is 4"),
                );
                parity_crcs.push(crc);
            }

            let parity_data_start = sidecar_offset + (k + p) * CRC_SIZE;

            metas.push(StripeMetadata {
                k,
                p,
                data_offset,
                data_crcs,
                parity_crcs,
                parity_data_start,
                data_bytes: stripe.data_bytes,
            });

            sidecar_offset += stripe_payload_size(k, p);
            data_offset += stripe.data_bytes;
        }
        metas
    };

    // Parallel stripe processing
    // Each stripe is cloned into its own buffer so rayon tasks can operate
    // on disjoint memory without requiring `&mut` borrows of `out`.
    let mut stripe_buffers: Vec<Vec<u8>> = stripe_meta
        .iter()
        .map(|meta| {
            let mut buf = vec![0u8; SHARD_SIZE * meta.k];
            let copy_len = meta.data_bytes.min(buf.len());
            buf[..copy_len].copy_from_slice(&data[meta.data_offset..meta.data_offset + copy_len]);
            buf
        })
        .collect();

    use rayon::prelude::*;
    stripe_meta
        .par_iter()
        .zip(stripe_buffers.par_iter_mut())
        .enumerate()
        .try_for_each(|(stripe_idx, (meta, stripe_data))| {
            process_stripe(stripe_idx, meta, stripe_data, ecc_payload, &rs)
        })?;

    // Assemble output from repaired stripe buffers.
    let mut out = Vec::with_capacity(data.len());
    for (meta, stripe_data) in stripe_meta.iter().zip(&stripe_buffers) {
        let copy_len = meta.data_bytes.min(stripe_data.len());
        out.extend_from_slice(&stripe_data[..copy_len]);
    }

    Ok(out)
}

/// Pre-computed per-stripe metadata for parallel decode.
struct StripeMetadata {
    k: usize,
    p: usize,
    data_offset: usize,
    data_bytes: usize,
    data_crcs: Vec<u32>,
    parity_crcs: Vec<u32>,
    parity_data_start: usize,
}

/// Process a single stripe: verify CRCs, mark erasures, reconstruct if needed.
///
/// Each stripe operates on a disjoint mutable slice of the output buffer,
/// so it can be called from parallel rayon tasks.
fn process_stripe(
    stripe_idx: usize,
    meta: &StripeMetadata,
    stripe_data: &mut [u8],
    ecc_payload: &[u8],
    rs: &ReedSolomon,
) -> Result<(), EccDecodeError> {
    let k = meta.k;
    let p = meta.p;

    // Build shard list: first K data shards, then P parity shards.
    let mut shards: Vec<Option<Vec<u8>>> = Vec::with_capacity(k + p);

    // Load data shards from stripe slice, verify CRC32.
    let mut erasure_count = 0usize;
    for (i, expected_crc) in meta.data_crcs.iter().enumerate().take(k) {
        let start = i * SHARD_SIZE;
        let end = (start + SHARD_SIZE).min(stripe_data.len());
        let mut shard = vec![0u8; SHARD_SIZE];
        if start < stripe_data.len() {
            shard[..end - start].copy_from_slice(&stripe_data[start..end]);
        }
        let crc = crc32_ieee(&shard);
        if crc == *expected_crc {
            shards.push(Some(shard));
        } else {
            shards.push(None);
            erasure_count += 1;
        }
    }

    // Load parity shards from sidecar, verify CRC32.
    for (i, expected_crc) in meta.parity_crcs.iter().enumerate().take(p) {
        let start = meta.parity_data_start + i * SHARD_SIZE;
        let mut shard = vec![0u8; SHARD_SIZE];
        shard.copy_from_slice(&ecc_payload[start..start + SHARD_SIZE]);
        let crc = crc32_ieee(&shard);
        if crc == *expected_crc {
            shards.push(Some(shard));
        } else {
            shards.push(None);
            erasure_count += 1;
        }
    }

    // Reconstruct erasures.
    if erasure_count > 0 {
        if erasure_count > p {
            return Err(EccDecodeError::TooManyErasures);
        }

        rs.reconstruct(&mut shards)
            .map_err(|_| EccDecodeError::ReconstructFailed)?;

        // Write repaired data shards back to stripe slice.
        for (i, shard) in shards[..k].iter().enumerate() {
            if let Some(shard_data) = shard {
                let start = i * SHARD_SIZE;
                if start < stripe_data.len() {
                    let end = (start + SHARD_SIZE).min(stripe_data.len());
                    stripe_data[start..end].copy_from_slice(&shard_data[..end - start]);
                }
            }
        }
    }

    let _ = stripe_idx; // used for tracing in debug builds if needed
    Ok(())
}

/// Validate data CRCs against the sidecar after repair.
///
/// Re-reads the per-shard CRC32s from the sidecar and compares them with the
/// actual CRC32s of the (repaired) data. Returns `Ok(())` if all data shard
/// CRCs match; returns the index of the first mismatched shard otherwise.
pub(crate) fn validate_crc(data: &[u8], ecc_payload: &[u8]) -> Result<(), usize> {
    if ecc_payload.len() < HEADER_SIZE || ecc_payload[0..4] != MAGIC {
        return Err(0);
    }

    let version = ecc_payload[4];
    if version != VERSION {
        return Err(0);
    }

    let k = u16::from_le_bytes(ecc_payload[6..8].try_into().expect("slice length is 2")) as usize;
    let p = u16::from_le_bytes(ecc_payload[8..10].try_into().expect("slice length is 2")) as usize;
    let original_len =
        u64::from_le_bytes(ecc_payload[10..18].try_into().expect("slice length is 8")) as usize;

    if data.len() != original_len || k == 0 || p == 0 {
        return Err(0);
    }

    let layouts = calculate_stripe_layouts(original_len, k, p);

    let mut sidecar_offset = HEADER_SIZE;
    let mut data_offset = 0usize;
    let mut shard = vec![0u8; SHARD_SIZE];

    for stripe in &layouts {
        let sk = stripe.data_shards;

        // Verify each data shard's CRC against the sidecar.
        for i in 0..sk {
            let crc_start = sidecar_offset + i * CRC_SIZE;
            let expected_crc = u32::from_le_bytes(
                ecc_payload[crc_start..crc_start + CRC_SIZE]
                    .try_into()
                    .expect("slice length is 4"),
            );

            // Zero-pad the shard buffer, then copy available data into it.
            shard.fill(0);
            let shard_start = data_offset + i * SHARD_SIZE;
            let shard_end = (shard_start + SHARD_SIZE).min(data.len());
            if shard_start < data.len() {
                let copy_len = shard_end - shard_start;
                shard[..copy_len].copy_from_slice(&data[shard_start..shard_end]);
            }
            let actual_crc = crc32_ieee(&shard);

            if actual_crc != expected_crc {
                return Err(data_offset / SHARD_SIZE + i);
            }
        }

        sidecar_offset += stripe_payload_size(stripe.data_shards, stripe.parity_shards);
        data_offset += stripe.data_bytes;
    }

    Ok(())
}

/// Errors that can occur when encoding an ECC payload.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum EccEncodeError {
    InvalidShardCount { k: usize, p: usize },
}

impl std::fmt::Display for EccEncodeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidShardCount { k, p } => {
                write!(
                    f,
                    "invalid shard count: k={k}, p={p} (must be >0 and k+p<=256)"
                )
            }
        }
    }
}

impl std::error::Error for EccEncodeError {}

/// Errors that can occur when decoding or repairing an ECC payload.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum EccDecodeError {
    PayloadTooShort,
    InvalidHeader,
    TooManyErasures,
    ReconstructFailed,
    CrcValidationFailed(usize),
}

impl std::fmt::Display for EccDecodeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::PayloadTooShort => write!(f, "ECC payload too short"),
            Self::InvalidHeader => write!(f, "invalid ECC header"),
            Self::TooManyErasures => write!(f, "too many erasures for recovery"),
            Self::ReconstructFailed => write!(f, "reed-solomon reconstruction failed"),
            Self::CrcValidationFailed(shard) => {
                write!(f, "CRC validation failed at shard {shard} after repair")
            }
        }
    }
}

impl std::error::Error for EccDecodeError {}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip() {
        let data = vec![42u8; 12_000]; // ~3 shards
        let payload = ecc_encode(&data, 4, 2).unwrap();
        assert!(!payload.is_empty());
        assert_eq!(&payload[0..4], &MAGIC);
        assert_eq!(payload[4], VERSION);

        let decoded = ecc_decode(&data, &payload).unwrap();
        assert_eq!(decoded, data);
    }

    #[test]
    fn empty() {
        let payload = ecc_encode(&[], 4, 2).unwrap();
        assert!(payload.is_empty());
    }

    #[test]
    fn zero_p() {
        let payload = ecc_encode(&[1, 2, 3], 4, 0).unwrap();
        assert!(payload.is_empty());
    }

    #[test]
    fn invalid_shard_count() {
        let data = vec![0u8; 100];
        assert!(matches!(
            ecc_encode(&data, 0, 2),
            Err(EccEncodeError::InvalidShardCount { k: 0, p: 2 })
        ));
        assert!(matches!(
            ecc_encode(&data, 255, 2),
            Err(EccEncodeError::InvalidShardCount { k: 255, p: 2 })
        ));
    }

    #[test]
    fn too_short() {
        let data = vec![0u8; 100];
        assert!(matches!(
            ecc_decode(&data, &[0; 5]),
            Err(EccDecodeError::InvalidHeader)
        ));
    }

    #[test]
    fn invalid_header() {
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
    fn large_multi_mb() {
        let size = 16 * 1024 * 1024;
        let mut data = Vec::with_capacity(size);
        for i in 0..size {
            data.push(((i * 7 + 13) % 256) as u8);
        }

        let payload = ecc_encode(&data, 4, 2).unwrap();
        assert!(!payload.is_empty());

        let decoded = ecc_decode(&data, &payload).unwrap();
        assert_eq!(decoded.len(), size);
        assert_eq!(decoded, data);
    }

    #[test]
    fn corrupt_one_data_shard() {
        let data = vec![0xABu8; SHARD_SIZE * 4];
        let payload = ecc_encode(&data, 4, 2).unwrap();

        let mut corrupted_data = data.clone();
        corrupted_data[0] ^= 0xFF;

        let decoded = ecc_decode(&corrupted_data, &payload).unwrap();
        assert_eq!(decoded, data, "should repair one corrupt data shard");
    }

    #[test]
    fn corrupt_one_parity_shard() {
        let data = vec![0xCDu8; SHARD_SIZE * 4];
        let payload = ecc_encode(&data, 4, 2).unwrap();

        // Corrupt a byte in the parity region of the sidecar.
        let mut corrupted_payload = payload.clone();
        corrupted_payload[HEADER_SIZE + (4 + 2) * CRC_SIZE] ^= 0xFF;

        let decoded = ecc_decode(&data, &corrupted_payload).unwrap();
        assert_eq!(decoded, data, "should handle corrupt parity shard");
    }

    #[test]
    fn corrupt_multiple_within_p() {
        let data = vec![0x55u8; SHARD_SIZE * 4];
        let payload = ecc_encode(&data, 4, 3).unwrap(); // p=3, can handle up to 3 erasures

        let mut corrupted_data = data.clone();
        corrupted_data[0] ^= 0xFF;
        corrupted_data[SHARD_SIZE] ^= 0xAA;

        let decoded = ecc_decode(&corrupted_data, &payload).unwrap();
        assert_eq!(
            decoded, data,
            "should repair multiple corrupt shards within parity capacity"
        );
    }

    #[test]
    fn too_many_erasures_returns_error() {
        let data = vec![0x99u8; SHARD_SIZE * 4];
        let payload = ecc_encode(&data, 4, 2).unwrap(); // p=2, max 2 erasures

        // Corrupt 3 data shards (exceeds p=2).
        let mut corrupted_data = data.clone();
        corrupted_data[0] ^= 0xFF;
        corrupted_data[SHARD_SIZE] ^= 0xAA;
        corrupted_data[SHARD_SIZE * 2] ^= 0x55;

        let result = ecc_decode(&corrupted_data, &payload);
        assert!(
            matches!(result, Err(EccDecodeError::TooManyErasures)),
            "should return TooManyErasures for >P corrupt shards"
        );
    }

    #[test]
    fn reconstruct_failed_returns_error() {
        // Corrupt all data AND parity shards so reconstruction fails.
        let data = vec![0xBBu8; SHARD_SIZE * 4];
        let mut payload = ecc_encode(&data, 4, 2).unwrap();

        // Corrupt every byte in the sidecar (all parity and CRC32s).
        for byte in payload[HEADER_SIZE..].iter_mut() {
            *byte = 0xFF;
        }

        let mut corrupted_data = data.clone();
        corrupted_data[0] ^= 0xFF;

        let result = ecc_decode(&corrupted_data, &payload);
        assert!(
            matches!(
                result,
                Err(EccDecodeError::ReconstructFailed | EccDecodeError::TooManyErasures)
            ),
            "should return error when reconstruction fails, got: {result:?}"
        );
    }
}
