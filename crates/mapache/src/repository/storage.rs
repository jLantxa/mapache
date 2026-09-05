use std::sync::atomic::{AtomicBool, Ordering};

use aes_gcm_siv::aead::{AeadInOut, inout::InOutBuf};
use aes_gcm_siv::{Aes256GcmSiv, Key as AesKey, KeyInit, Nonce, aead::Aead};
use argon2::Argon2;
use parking_lot::Mutex;
use zeroize::Zeroizing;

use crate::{
    backend::WriteContents,
    common::error::{MapacheError, Result},
    common::{self, defaults},
};

const AES_GCM_NONCE_LEN: usize = 12;
const AES_GCM_TAG_LEN: usize = 16;

/// Secure storage is an abstraction for file IO that handles compression and encryption.
pub struct SecureStorage {
    compression_level: i32,
    cipher: Option<Aes256GcmSiv>,
    nonce_at_end: AtomicBool,
    compressor_pool: Mutex<Vec<EncodingContext>>,
}

impl Default for SecureStorage {
    fn default() -> Self {
        Self::new()
    }
}

impl SecureStorage {
    pub fn new() -> Self {
        Self {
            compression_level: defaults::DEFAULT_COMPRESSION.to_level(),
            cipher: None,
            nonce_at_end: AtomicBool::new(true),
            compressor_pool: Mutex::new(Vec::new()),
        }
    }

    /// Place the nonce at the end of the ciphertext (`[ct | tag | nonce]`) instead
    /// of the beginning (`[nonce | ct | tag]`). Eliminates a memmove on decrypt.
    /// Default: `true`.
    pub fn set_nonce_at_end(&self, nonce_at_end: bool) {
        self.nonce_at_end.store(nonce_at_end, Ordering::Relaxed);
    }

    pub(crate) fn nonce_at_end(&self) -> bool {
        self.nonce_at_end.load(Ordering::Relaxed)
    }

    /// Set compression level
    pub fn with_compression(mut self, level: i32) -> Self {
        self.compression_level = level;
        self
    }

    /// Set a 32-byte key and initialize the cipher (immutable afterward)
    pub fn with_key(mut self, key: &[u8]) -> Result<Self> {
        if key.len() != 32 {
            return Err(MapacheError::Internal(format!(
                "secure storage requires a 32-byte key, got {} bytes",
                key.len()
            )));
        }

        let aes_key = AesKey::<Aes256GcmSiv>::try_from(key)
            .map_err(|e| MapacheError::Internal(format!("invalid AES key: {e}")))?;
        self.cipher = Some(Aes256GcmSiv::new(&aes_key));
        Ok(self)
    }

    pub fn get_encoding_context(&self) -> Result<EncodingContext> {
        let mut compressor = zstd::bulk::Compressor::new(self.compression_level)
            .map_err(|e| MapacheError::Compression(format!("zstd init failed: {e}")))?;

        // Use a maximum back-reference window the size of the biggest chunk.
        const ZSTD_WINDOW_LOG: u32 = common::defaults::NORMAL_CHUNK_SIZE
            .saturating_sub(1)
            .next_power_of_two()
            .ilog2();

        compressor.set_parameter(zstd::zstd_safe::CParameter::WindowLog(ZSTD_WINDOW_LOG))?;
        compressor.set_parameter(zstd::zstd_safe::CParameter::ChecksumFlag(false))?;

        Ok(EncodingContext::new(compressor))
    }

    #[allow(clippy::uninit_vec)]
    fn transform_into(&self, ctx: Option<&mut EncodingContext>, data: &[u8]) -> Result<Vec<u8>> {
        self.transform_into_inner(ctx, data, self.nonce_at_end())
    }

    #[allow(clippy::uninit_vec)]
    fn transform_into_inner(
        &self,
        ctx: Option<&mut EncodingContext>,
        data: &[u8],
        nonce_at_end: bool,
    ) -> Result<Vec<u8>> {
        let bound = if ctx.is_some() {
            zstd::zstd_safe::compress_bound(data.len())
        } else {
            data.len()
        };

        let has_cipher = self.cipher.is_some();
        let overhead = if has_cipher {
            AES_GCM_TAG_LEN + AES_GCM_NONCE_LEN
        } else {
            0
        };
        let mut out = Vec::with_capacity(bound + overhead);

        if let Some(c) = ctx {
            unsafe {
                // SAFETY: u8 accepts any bit pattern. We set the length to `bound`
                // to obtain a mutable slice of the reserved capacity without zero-initializing.
                // This memory is immediately passed to the zstd compressor which
                // overwrites it. On error, the Vec is dropped.
                out.set_len(bound);
            }
            let n = c
                .compressor
                .compress_to_buffer(data, &mut out)
                .map_err(|e| MapacheError::Compression(format!("zstd failed: {e}")))?;
            out.truncate(n);
        } else {
            out.extend_from_slice(data);
        }

        if let Some(cipher) = &self.cipher {
            let nonce_bytes: [u8; AES_GCM_NONCE_LEN] = rand::random();
            let nonce = Nonce::try_from(&nonce_bytes[..]).expect("nonce length is always 12");
            // InOutBuf shares input/output memory — zero-copy in-place encryption.
            // (The nonce-at-start v1 branch below allocates an extra Vec.)
            let tag = cipher
                .encrypt_inout_detached(&nonce, b"", InOutBuf::from(&mut out[..]))
                .map_err(|_| MapacheError::Crypto("encryption failed".to_string()))?;

            if nonce_at_end {
                out.extend_from_slice(tag.as_slice());
                out.extend_from_slice(&nonce_bytes);
            } else {
                // TODO(v1-removal): Remove the v1 nonce-at-start branch.
                let mut prefixed =
                    Vec::with_capacity(AES_GCM_NONCE_LEN + out.len() + AES_GCM_TAG_LEN);
                prefixed.extend_from_slice(&nonce_bytes);
                prefixed.extend_from_slice(&out);
                prefixed.extend_from_slice(tag.as_slice());
                return Ok(prefixed);
            }
        }

        Ok(out)
    }

    /// Compresses data and returns an owned Vec.
    pub fn compress(&self, data: &[u8]) -> Result<Vec<u8>> {
        let mut ctx = self.take_encoding_context()?;
        let res = self.compress_managed(&mut ctx, data);
        self.return_encoding_context(ctx);
        res
    }

    /// Compress using a reusable context. Returns owned Vec.
    #[allow(clippy::uninit_vec)]
    pub(crate) fn compress_managed(
        &self,
        ctx: &mut EncodingContext,
        data: &[u8],
    ) -> Result<Vec<u8>> {
        let bound = zstd::zstd_safe::compress_bound(data.len());
        let mut out = Vec::with_capacity(bound);

        // SAFETY: u8 accepts any bit pattern. We set the length to `bound` to
        // obtain a mutable slice of the reserved capacity without zero-initializing.
        // This memory is immediately passed to the zstd compressor which
        // overwrites it. On error, the Vec is dropped.
        unsafe {
            out.set_len(bound);
        }

        let n = ctx
            .compressor
            .compress_to_buffer(data, &mut out)
            .map_err(|e| MapacheError::Compression(format!("zstd failed: {e}")))?;

        // SAFETY: `compress_to_buffer` successfully wrote `n` bytes.
        unsafe {
            out.set_len(n);
        }
        Ok(out)
    }

    pub fn decompress(&self, data: &[u8]) -> Result<Vec<u8>> {
        zstd::decode_all(data)
            .map_err(|e| MapacheError::Compression(format!("zstd decompression failed: {e}")))
    }

    /// Decompress `data` with an upper bound on the output size, producing at most
    /// `limit` bytes. Used for data that is decompressed before being authenticated
    /// (e.g. keyfiles) to prevent decompression bombs.
    pub fn decompress_with_limit(&self, data: &[u8], limit: usize) -> Result<Vec<u8>> {
        use std::io::Read;
        let decoder = zstd::stream::Decoder::new(data)
            .map_err(|e| MapacheError::Compression(format!("zstd decompression failed: {e}")))?;
        let mut out = Vec::with_capacity(limit.min(1024 * 1024));
        decoder
            .take(limit as u64 + 1)
            .read_to_end(&mut out)
            .map_err(|e| MapacheError::Compression(format!("zstd decompression failed: {e}")))?;
        if out.len() > limit {
            return Err(MapacheError::Compression(format!(
                "zstd decompressed size {} exceeds limit {}",
                out.len(),
                limit
            )));
        }
        Ok(out)
    }

    #[inline]
    pub fn encrypt(&self, data: &[u8]) -> Result<Vec<u8>> {
        self.transform_into(None, data)
    }

    /// Re-encrypt data from `old_nonce_at_end` position to `new_nonce_at_end` position.
    /// Used during migration to change the nonce position of existing encrypted data.
    pub fn re_encrypt(
        &self,
        data: &[u8],
        old_nonce_at_end: bool,
        new_nonce_at_end: bool,
    ) -> Result<Vec<u8>> {
        if self.cipher.is_none() {
            return Ok(data.to_vec());
        }
        let decrypted = match self.decrypt_inner(data, old_nonce_at_end)? {
            WriteContents::Owned(v) => v,
            WriteContents::Borrowed(b) => b.to_vec(),
        };
        self.transform_into_inner(None, &decrypted, new_nonce_at_end)
    }

    pub fn decrypt<'a>(&self, data: &'a [u8]) -> Result<WriteContents<'a>> {
        self.decrypt_inner(data, self.nonce_at_end())
    }

    pub(crate) fn decrypt_inner<'a>(
        &self,
        data: &'a [u8],
        nonce_at_end: bool,
    ) -> Result<WriteContents<'a>> {
        let Some(cipher) = &self.cipher else {
            return Ok(WriteContents::Borrowed(data));
        };

        if data.len() < AES_GCM_NONCE_LEN + AES_GCM_TAG_LEN {
            Err(MapacheError::Integrity("invalid ciphertext".to_string()))?;
        }

        let (nonce, ciphertext_and_tag) = Self::extract_nonce_and_ct(data, nonce_at_end)?;

        let decrypted = cipher
            .decrypt(&nonce, ciphertext_and_tag)
            .map_err(|_| MapacheError::Crypto("decryption failed".to_string()))?;
        Ok(WriteContents::Owned(decrypted))
    }

    /// Decrypts the given data, reusing the input allocation when possible.
    ///
    /// Tries the configured `nonce_at_end` position first. If that fails and
    /// the repo is in a transitional state (v1 data with v2 config), falls back
    /// to the other position. This fallback should be removed when v1 is
    /// deprecated.
    // TODO(v1-removal): Remove the fallback branch.
    pub fn decrypt_in_place(&self, mut data: Vec<u8>) -> Result<Vec<u8>> {
        let Some(cipher) = &self.cipher else {
            return Ok(data);
        };

        if data.len() < AES_GCM_NONCE_LEN + AES_GCM_TAG_LEN {
            Err(MapacheError::Integrity("invalid ciphertext".to_string()))?;
        }

        // Try the configured nonce position first.
        let primary = self.nonce_at_end();
        let primary_err = match Self::try_decrypt_in_place(cipher, &mut data, primary) {
            Ok(()) => return Ok(data),
            Err(e) => e,
        };

        // Fallback: try the other nonce position (needed for v1 data after migration).
        Self::try_decrypt_in_place(cipher, &mut data, !primary).map_err(|e| {
            MapacheError::Crypto(format!(
                "decryption failed with both nonce positions \
                 (primary: {primary_err}, fallback: {e})"
            ))
        })?;

        Ok(data)
    }

    /// Extracts the nonce and ciphertext+tag according to the nonce position layout.
    /// v2 uses nonce at end: `[ciphertext | tag | nonce]`
    /// Legacy v1 uses nonce at start: `[nonce | ciphertext | tag]`
    // TODO(v1-removal): Remove the v1 branch (nonce at start).
    fn extract_nonce_and_ct(data: &[u8], nonce_at_end: bool) -> Result<(Nonce, &[u8])> {
        if nonce_at_end {
            let nonce_start = data.len() - AES_GCM_NONCE_LEN;
            let nonce = Nonce::try_from(&data[nonce_start..])
                .map_err(|_| MapacheError::Crypto("invalid nonce".to_string()))?;
            Ok((nonce, &data[..nonce_start]))
        } else {
            let (nonce_bytes, ct_tag) = data.split_at(AES_GCM_NONCE_LEN);
            let nonce = Nonce::try_from(nonce_bytes)
                .map_err(|_| MapacheError::Crypto("invalid nonce".to_string()))?;
            Ok((nonce, ct_tag))
        }
    }

    /// Attempt to decrypt data in-place with the given nonce position.
    fn try_decrypt_in_place(
        cipher: &Aes256GcmSiv,
        data: &mut Vec<u8>,
        nonce_at_end: bool,
    ) -> Result<()> {
        let (nonce, ciphertext_and_tag) = Self::extract_nonce_and_ct(data, nonce_at_end)?;
        let plaintext = cipher
            .decrypt(&nonce, ciphertext_and_tag)
            .map_err(|_| MapacheError::Crypto("decryption failed".to_string()))?;
        *data = plaintext;
        Ok(())
    }

    pub fn encode(&self, data: &[u8]) -> Result<Vec<u8>> {
        let mut ctx = self.take_encoding_context()?;
        let res = self.encode_managed(&mut ctx, data);
        self.return_encoding_context(ctx);
        res
    }

    /// Encrypt with an EncodingContext
    #[inline]
    pub fn encode_managed(&self, ctx: &mut EncodingContext, data: &[u8]) -> Result<Vec<u8>> {
        self.transform_into(Some(ctx), data)
    }

    /// Encode (compress + encrypt) with an explicit nonce position.
    /// Used by migration to write metadata in the target format.
    pub fn encode_with_nonce_position(
        &self,
        ctx: &mut EncodingContext,
        data: &[u8],
        nonce_at_end: bool,
    ) -> Result<Vec<u8>> {
        self.transform_into_inner(Some(ctx), data, nonce_at_end)
    }

    pub fn take_encoding_context(&self) -> Result<EncodingContext> {
        if let Some(ctx) = self.compressor_pool.lock().pop() {
            return Ok(ctx);
        }
        self.get_encoding_context()
    }

    pub fn return_encoding_context(&self, ctx: EncodingContext) {
        let mut pool = self.compressor_pool.lock();
        if pool.len() < defaults::DEFAULT_COMPRESSOR_POOL_SIZE {
            pool.push(ctx);
        }
    }

    pub fn decode(&self, data: &[u8]) -> Result<Vec<u8>> {
        let decrypted = self.decrypt(data)?;
        self.decompress(&decrypted)
    }

    pub fn decode_owned(&self, data: Vec<u8>) -> Result<Vec<u8>> {
        let decrypted = self.decrypt_in_place(data)?;
        self.decompress(&decrypted)
    }

    /// Decodes a blob (decrypt, then optional decompress based on the
    /// per-blob compression marker stored in the blob descriptor).
    pub fn decode_blob(&self, data: &[u8], compressed: bool) -> Result<Vec<u8>> {
        let decrypted = self.decrypt(data)?;
        if compressed {
            self.decompress(&decrypted)
        } else {
            Ok(match decrypted {
                WriteContents::Owned(v) => v,
                WriteContents::Borrowed(b) => b.to_vec(),
            })
        }
    }

    /// Decodes an owned blob buffer (decrypt, then optional decompress).
    pub fn decode_blob_owned(&self, data: Vec<u8>, compressed: bool) -> Result<Vec<u8>> {
        if compressed {
            self.decode_owned(data)
        } else {
            self.decrypt_in_place(data)
        }
    }

    pub fn derive_key<const KEY_LEN: usize>(
        password: &str,
        salt: &[u8],
        params: argon2::Params,
    ) -> Result<Zeroizing<[u8; KEY_LEN]>> {
        let argon2 = Argon2::new(argon2::Algorithm::Argon2id, argon2::Version::V0x13, params);
        let mut key = [0u8; KEY_LEN];
        argon2
            .hash_password_into(password.as_bytes(), salt, &mut key)
            .map_err(|e| MapacheError::Crypto(format!("argon2 derive failed: {e}")))?;
        Ok(Zeroizing::new(key))
    }

    /// Generate a cryptographically strong salt using the OS random source.
    pub fn generate_salt<const LENGTH: usize>() -> [u8; LENGTH] {
        rand::random()
    }
}

pub struct EncodingContext {
    compressor: zstd::bulk::Compressor<'static>,
}

impl EncodingContext {
    fn new(compressor: zstd::bulk::Compressor<'static>) -> Self {
        Self { compressor }
    }
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;

    const TEST_KEY: [u8; 32] = *b"0123456789abcdef0123456789abcdef";

    const TEXT: &[u8; 431] = br#"
Lorem ipsum dolor sit amet, consectetur adipisici elit, sed eiusmod tempor incidunt
ut labore et dolore magna aliqua. Ut enim ad minim veniam, quis nostrud exercitation
ullamco laboris nisi ut aliquid ex ea commodi consequat. Quis aute iure reprehenderit in
voluptate velit esse cillum dolore eu fugiat nulla pariatur. Excepteur sint obcaecat
cupiditat non proident, sunt in culpa qui officia deserunt mollit anim id est laborum.
"#;

    #[rstest]
    #[case(0)]
    #[case(3)]
    #[case(10)]
    #[case(15)]
    #[case(22)]
    fn test_compression_and_decompression(#[case] level: i32) {
        let ss = SecureStorage::new().with_compression(level);

        let original_data = TEXT;
        let compressed_data = ss.compress(original_data).unwrap();
        let decompressed_data = ss.decompress(&compressed_data).unwrap();

        assert_eq!(*original_data, *decompressed_data);
    }

    #[test]
    fn test_decompress_with_limit_roundtrip() {
        let ss = SecureStorage::new()
            .with_compression(defaults::DEFAULT_COMPRESSION.to_level());

        let original_data = TEXT;
        let compressed_data = ss.compress(original_data).unwrap();

        let decompressed_data = ss.decompress_with_limit(&compressed_data, 1024).unwrap();
        assert_eq!(*original_data, *decompressed_data);

        // A limit smaller than the decompressed size must fail, not allocate.
        assert!(ss.decompress_with_limit(&compressed_data, 10).is_err());
    }

    #[test]
    fn test_decompress_with_limit_rejects_bomb() {
        let ss = SecureStorage::new()
            .with_compression(defaults::DEFAULT_COMPRESSION.to_level());

        // Highly compressible payload that expands beyond the limit.
        let bomb = vec![0u8; 8 * 1024 * 1024];
        let compressed_bomb = ss.compress(&bomb).unwrap();
        assert!(compressed_bomb.len() < 1024); // sanity: compresses well

        let res = ss.decompress_with_limit(&compressed_bomb, 1024);
        assert!(res.is_err());
    }

    #[test]
    fn test_generate_salt() {
        let salt = SecureStorage::generate_salt::<4>();
        assert_eq!(salt.len(), 4);

        let salt = SecureStorage::generate_salt::<8>();
        assert_eq!(salt.len(), 8);

        let salt = SecureStorage::generate_salt::<16>();
        assert_eq!(salt.len(), 16);

        let salt = SecureStorage::generate_salt::<32>();
        assert_eq!(salt.len(), 32);
    }

    #[test]
    fn test_derive_key() -> Result<()> {
        let password = "mapachito";
        let salt = b"0123456789abcdef0123456789abcdef";
        let key16a = SecureStorage::derive_key::<16>(password, salt, argon2::Params::default())?;
        let key16b = SecureStorage::derive_key::<16>(password, salt, argon2::Params::default())?;
        let key32a = SecureStorage::derive_key::<32>(password, salt, argon2::Params::default())?;
        let key32b = SecureStorage::derive_key::<32>(password, salt, argon2::Params::default())?;

        assert_eq!(key16a.len(), 16);
        assert_eq!(key16a, key16b);
        assert_eq!(key32a.len(), 32);
        assert_eq!(key32a, key32b);

        Ok(())
    }

    #[test]
    fn test_encode_decode_with_compression_and_key() -> Result<()> {
        let key = TEST_KEY;
        let ss = SecureStorage::new()
            .with_compression(defaults::DEFAULT_COMPRESSION.to_level())
            .with_key(&key)
            .unwrap();

        let ciphertext = ss.encode(TEXT)?;
        let decoded_plaintext = ss.decode(&ciphertext)?;

        assert_eq!(TEXT, decoded_plaintext.as_slice());
        Ok(())
    }

    #[test]
    fn test_encryption_decryption_with_key() -> Result<()> {
        // No compression: length checks are stable (nonce + tag overhead)
        let key = TEST_KEY;
        let ss = SecureStorage::new()
            .with_key(&key)
            .expect("valid 32-byte key");

        let original_data = TEXT.as_slice();
        let encrypted_data = ss.encrypt(original_data)?;
        let decrypted_data = ss.decrypt(&encrypted_data)?;

        assert!(encrypted_data.len() > original_data.len());
        assert_eq!(
            encrypted_data.len() - original_data.len(),
            AES_GCM_NONCE_LEN + AES_GCM_TAG_LEN
        );
        assert_eq!(original_data, &*decrypted_data);
        Ok(())
    }

    #[test]
    fn test_encryption_decryption_no_key() -> Result<()> {
        let ss = SecureStorage::new(); // no key, default compression
        let original_data = TEXT.as_slice();

        let encrypted_data = ss.encrypt(original_data)?;
        let decrypted_data = ss.decrypt(&encrypted_data)?;

        // No key means no encryption, so data should be unchanged
        assert_eq!(original_data, &*decrypted_data);

        Ok(())
    }

    #[test]
    fn test_encode_decode_no_key_with_compression() -> Result<()> {
        let ss = SecureStorage::new().with_compression(defaults::DEFAULT_COMPRESSION.to_level());

        let encoded_data = ss.encode(TEXT)?;
        let decoded_data = ss.decode(&encoded_data)?;

        // Data should be smaller than original due to compression, and not encrypted
        assert!(encoded_data.len() < TEXT.len());
        assert_eq!(TEXT.as_slice(), decoded_data.as_slice());

        Ok(())
    }

    #[test]
    fn test_decrypt_invalid_data_length() {
        let key = TEST_KEY;
        let ss = SecureStorage::new()
            .with_key(&key)
            .expect("valid 32-byte key");

        // Shorter than nonce length
        let too_short_data = [0u8; AES_GCM_NONCE_LEN - 1];

        let result = ss.decrypt(&too_short_data);
        assert!(result.is_err());
    }

    #[test]
    fn test_decrypt_tampered_data() -> Result<()> {
        let key = TEST_KEY;
        let ss = SecureStorage::new()
            .with_key(&key)
            .expect("valid 32-byte key");

        let original_data = TEXT.as_slice();
        let mut encrypted_data = ss.encrypt(original_data)?;

        // Tamper with one byte of the ciphertext (not in the nonce prefix)
        let tamper_index = AES_GCM_NONCE_LEN + (encrypted_data.len() - AES_GCM_NONCE_LEN) / 2;
        encrypted_data[tamper_index] = encrypted_data[tamper_index].wrapping_add(7);

        let result = ss.decrypt(&encrypted_data);
        assert!(result.is_err());
        Ok(())
    }

    #[test]
    fn test_encode_decode_managed() -> Result<()> {
        let key = TEST_KEY;
        let ss = SecureStorage::new()
            .with_compression(defaults::DEFAULT_COMPRESSION.to_level())
            .with_key(&key)
            .unwrap();

        let mut ectx = ss.get_encoding_context()?;

        let ciphertext = ss.encode_managed(&mut ectx, TEXT)?;
        let decoded_plaintext = ss.decode(&ciphertext)?;

        assert_eq!(TEXT, decoded_plaintext.as_slice());
        Ok(())
    }

    #[test]
    fn test_decrypt_in_place() -> Result<()> {
        let key = TEST_KEY;
        let ss = SecureStorage::new()
            .with_key(&key)
            .expect("valid 32-byte key");

        let encrypted_data = ss.encrypt(TEXT)?;
        let decrypted_data = ss.decrypt_in_place(encrypted_data)?;

        assert_eq!(TEXT.as_slice(), decrypted_data.as_slice());
        Ok(())
    }

    #[test]
    fn test_decode_owned() -> Result<()> {
        let key = TEST_KEY;
        let ss = SecureStorage::new()
            .with_compression(defaults::DEFAULT_COMPRESSION.to_level())
            .with_key(&key)
            .unwrap();

        let encoded_data = ss.encode(TEXT)?;
        let decoded_data = ss.decode_owned(encoded_data)?;

        assert_eq!(TEXT.as_slice(), decoded_data.as_slice());
        Ok(())
    }

    #[test]
    fn test_nonce_at_start() -> Result<()> {
        let key = TEST_KEY;

        // Encrypt with nonce at start.
        let ss_at_start = SecureStorage::new()
            .with_key(&key)
            .expect("valid 32-byte key");
        ss_at_start.set_nonce_at_end(false);
        let encrypted = ss_at_start.encrypt(TEXT)?;

        // Wire format: [nonce(12) | ct | tag(16)]
        assert!(encrypted.len() > AES_GCM_NONCE_LEN + AES_GCM_TAG_LEN);
        let nonce = &encrypted[..AES_GCM_NONCE_LEN];
        assert_ne!(nonce, &[0u8; AES_GCM_NONCE_LEN]);

        // Decrypt with nonce at end (default) — must NOT work.
        let ss_at_end = SecureStorage::new()
            .with_key(&key)
            .expect("valid 32-byte key");
        assert!(ss_at_end.decrypt(&encrypted).is_err());

        // Decrypt with nonce at start — must work.
        let decrypted = ss_at_start.decrypt(&encrypted)?;
        assert_eq!(TEXT.as_slice(), &*decrypted);
        Ok(())
    }
}
