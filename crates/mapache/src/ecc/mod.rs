//! Mapache ECC sidecar format.
//!
//! Parity-only sidecar encoding/decoding for pack files. Uses
//! [`reed_solomon::ReedSolomon`] for the underlying erasure coding.
//!
//! The sidecar stores **only parity shards** — no data is duplicated on disk.
//! Stripes are encoded in parallel using rayon for multi-core acceleration.

mod reed_solomon;

use reed_solomon::ReedSolomon;

/// Shard size in bytes (4 KiB).
pub(crate) const SHARD_SIZE: usize = 4096;

/// Magic header identifier for ECC sidecar files (`b"MECP"`).
///
/// Sidecars store **only parity shards** — no data is duplicated.
pub(crate) const MAGIC: [u8; 4] = *b"MECP";

/// Current format version of the ECC sidecar file.
pub(crate) const VERSION: u8 = 1;

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
pub(crate) const HEADER_SIZE: usize = 4 + 1 + 1 + 2 + 2 + 8 + 4; // 22 bytes

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

/// Encode a blob into a parity-only sidecar payload.
///
/// Stores **only** the parity shards and header metadata. Does not duplicate
/// data on disk — the original data must be read separately for reconstruction.
/// Stripes are encoded in parallel using rayon.
pub(crate) fn ecc_encode(data: &[u8], k: usize, p: usize) -> Vec<u8> {
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
pub(crate) fn ecc_decode(data: &[u8], ecc_payload: &[u8]) -> Result<Vec<u8>, EccDecodeError> {
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
pub(crate) enum EccDecodeError {
    PayloadTooShort,
    InvalidHeader,
}

impl std::fmt::Display for EccDecodeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::PayloadTooShort => write!(f, "ECC payload too short"),
            Self::InvalidHeader => write!(f, "invalid ECC header"),
        }
    }
}

impl std::error::Error for EccDecodeError {}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

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
        // 16 MiB payload
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
