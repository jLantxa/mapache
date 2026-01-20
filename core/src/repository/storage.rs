use std::io::Read;

use aes_gcm_siv::{AeadInPlace, Aes256GcmSiv, Key as AesKey, KeyInit, Nonce, aead::Aead};
use anyhow::{Result, anyhow, bail};
use argon2::Argon2;
use rand::{TryRngCore, rngs::OsRng};

use crate::mapache::{self, defaults::MIN_CHUNK_SIZE};

const AES_GCM_NONCE_LEN: usize = 12;
const AES_GCM_TAG_LEN: usize = 16;

/// Secure storage is an abstraction for file IO that handles compression and encryption.
pub struct SecureStorage {
    compression_level: i32,
    cipher: Option<Aes256GcmSiv>,
}

impl SecureStorage {
    pub fn build() -> Self {
        Self {
            // No compression by default (level -1).
            // This is not exactly true, as zstd has no setting to 'disable' compression, but this
            // compression level is so low and fast that the compressed slice is identical to the
            // input data (in all my tests).
            compression_level: -1,
            cipher: None,
        }
    }

    /// Set a 32-byte key and initialize the cipher (immutable afterward)
    pub fn with_key(mut self, key: &[u8]) -> Self {
        assert_eq!(key.len(), 32);

        let aes_key = AesKey::<Aes256GcmSiv>::from_slice(key);
        self.cipher = Some(Aes256GcmSiv::new(aes_key));
        self
    }

    /// Set compression level
    pub fn with_compression(mut self, level: i32) -> Self {
        self.compression_level = level;
        self
    }

    pub fn get_encoding_context(&self) -> Result<EncodingContext> {
        let mut compressor = zstd::bulk::Compressor::new(self.compression_level)
            .map_err(|e| anyhow!("zstd init failed: {e}"))?;

        // Use a maximum back-reference window the size of the biggest chunk.
        const ZSTD_WINDOW_LOG: u32 = mapache::defaults::MAX_CHUNK_SIZE.ilog2();

        compressor.set_parameter(zstd::zstd_safe::CParameter::WindowLog(ZSTD_WINDOW_LOG))?;
        compressor.set_parameter(zstd::zstd_safe::CParameter::ChecksumFlag(false))?;
        compressor.set_parameter(zstd::zstd_safe::CParameter::NbWorkers(0))?;

        Ok(EncodingContext::new(compressor))
    }

    pub fn compress(&self, data: &[u8]) -> Result<Vec<u8>> {
        let mut ctx = self.get_encoding_context()?;
        let compressed_slice = self.compress_managed(&mut ctx, data)?;
        Ok(compressed_slice.to_vec())
    }

    /// Compress with an EncodingContext
    pub fn compress_managed<'a>(
        &self,
        ctx: &'a mut EncodingContext,
        data: &[u8],
    ) -> Result<&'a [u8]> {
        ctx.compression_buf.clear();
        let bound = zstd::zstd_safe::compress_bound(data.len());
        if ctx.compression_buf.capacity() < bound {
            ctx.compression_buf.reserve(bound);
        }

        let n = ctx
            .compressor
            .compress_to_buffer(data, &mut ctx.compression_buf)
            .map_err(|e| anyhow!("zstd compress failed: {e}"))?;

        unsafe { ctx.compression_buf.set_len(n) };
        Ok(&ctx.compression_buf)
    }

    pub fn decompress(&self, data: &[u8]) -> Result<Vec<u8>> {
        let mut decoder = zstd::Decoder::new(data)?;
        let mut decompressed = Vec::with_capacity(data.len());
        decoder.read_to_end(&mut decompressed)?;
        Ok(decompressed)
    }

    fn encrypt_into<'a>(&self, out: &'a mut Vec<u8>, data: &[u8]) -> Result<&'a [u8]> {
        let Some(cipher) = &self.cipher else {
            out.clear();
            out.extend_from_slice(data);
            return Ok(out.as_slice());
        };

        out.clear();
        out.reserve(AES_GCM_NONCE_LEN + data.len() + AES_GCM_TAG_LEN);

        // nonce prefix
        let mut nonce_bytes = [0u8; AES_GCM_NONCE_LEN];
        OsRng
            .try_fill_bytes(&mut nonce_bytes)
            .map_err(|e| anyhow!("rng failed: {e}"))?;
        out.extend_from_slice(&nonce_bytes);

        // plaintext -> ciphertext in place
        out.extend_from_slice(data);

        let nonce = Nonce::from_slice(&nonce_bytes);
        let data_start = AES_GCM_NONCE_LEN;

        let tag = cipher
            .encrypt_in_place_detached(nonce, b"", &mut out[data_start..])
            .map_err(|_| anyhow!("encryption failed"))?;

        out.extend_from_slice(tag.as_slice());

        Ok(out.as_slice())
    }

    #[inline]
    pub fn encrypt(&self, data: &[u8]) -> Result<Vec<u8>> {
        let mut out = Vec::with_capacity(AES_GCM_NONCE_LEN + data.len() + AES_GCM_TAG_LEN);
        self.encrypt_into(&mut out, data)?;
        Ok(out)
    }

    pub fn decrypt(&self, data: &[u8]) -> Result<Vec<u8>> {
        let Some(cipher) = &self.cipher else {
            return Ok(data.to_vec());
        };
        if data.len() < AES_GCM_NONCE_LEN {
            bail!("invalid ciphertext");
        }
        let (nonce, ciphertext) = data.split_at(AES_GCM_NONCE_LEN);
        match cipher.decrypt(Nonce::from_slice(nonce), ciphertext) {
            Ok(pt) => Ok(pt),
            Err(_) => bail!("decryption failed"),
        }
    }

    /// Encrypt with an EncodingContext
    #[inline]
    pub fn encrypt_managed<'a>(
        &self,
        ctx: &'a mut EncodingContext,
        data: &'a [u8],
    ) -> Result<&'a [u8]> {
        self.encrypt_into(&mut ctx.encryption_buf, data)
    }

    /// Derive a key from a password and salt (Argon2id)
    pub fn derive_key<const KEY_LEN: usize>(
        password: &str,
        salt: &[u8],
        params: argon2::Params,
    ) -> Result<[u8; KEY_LEN]> {
        let argon2 = Argon2::new(argon2::Algorithm::Argon2id, argon2::Version::V0x13, params);
        let mut key = [0u8; KEY_LEN];
        argon2
            .hash_password_into(password.as_bytes(), salt, &mut key)
            .map_err(|e| anyhow!("argon2 derive failed: {e}"))?;
        Ok(key)
    }

    /// compress → encrypt
    pub fn encode(&self, data: &[u8]) -> Result<Vec<u8>> {
        let mut ctx = self.get_encoding_context()?;
        let encoded_slice = self.encode_managed(&mut ctx, data)?;
        Ok(encoded_slice.to_vec())
    }

    /// compress → encrypt with an EncodingContext
    pub fn encode_managed<'a>(
        &self,
        ctx: &'a mut EncodingContext,
        data: &[u8],
    ) -> Result<&'a [u8]> {
        self.compress_managed(ctx, data)?;

        let Some(cipher) = &self.cipher else {
            return Ok(&ctx.compression_buf);
        };

        ctx.encryption_buf.clear();
        let mut nonce_bytes = [0u8; AES_GCM_NONCE_LEN];
        OsRng.try_fill_bytes(&mut nonce_bytes)?;
        ctx.encryption_buf.extend_from_slice(&nonce_bytes);

        ctx.encryption_buf.extend_from_slice(&ctx.compression_buf);

        let nonce = Nonce::from_slice(&nonce_bytes);
        let data_start = AES_GCM_NONCE_LEN;

        let tag = cipher
            .encrypt_in_place_detached(nonce, b"", &mut ctx.encryption_buf[data_start..])
            .map_err(|_| anyhow!("encryption failed"))?;

        ctx.encryption_buf.extend_from_slice(tag.as_slice());

        Ok(&ctx.encryption_buf)
    }

    /// decrypt → decompress
    pub fn decode(&self, data: &[u8]) -> Result<Vec<u8>> {
        let decrypted = self.decrypt(data)?;
        self.decompress(&decrypted)
    }

    /// Generate a cryptographically random salt
    pub fn generate_salt<const LENGTH: usize>() -> [u8; LENGTH] {
        let mut salt = [0u8; LENGTH];
        OsRng.try_fill_bytes(&mut salt).expect("OS RNG failed");
        salt
    }
}

pub struct EncodingContext {
    compressor: zstd::bulk::Compressor<'static>,
    compression_buf: Vec<u8>,
    encryption_buf: Vec<u8>,
}

impl EncodingContext {
    fn new(compressor: zstd::bulk::Compressor<'static>) -> Self {
        Self {
            compressor,
            compression_buf: Vec::with_capacity(MIN_CHUNK_SIZE as usize),
            encryption_buf: Vec::with_capacity(MIN_CHUNK_SIZE as usize),
        }
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
        let ss = SecureStorage::build().with_compression(level);

        let original_data = TEXT;
        let compressed_data = ss.compress(original_data).unwrap();
        let decompressed_data = ss.decompress(&compressed_data).unwrap();

        assert_eq!(*original_data, *decompressed_data);
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
        let ss = SecureStorage::build()
            .with_compression(zstd::DEFAULT_COMPRESSION_LEVEL)
            .with_key(&key);

        let ciphertext = ss.encode(TEXT)?;
        let decoded_plaintext = ss.decode(&ciphertext)?;

        assert_eq!(TEXT, decoded_plaintext.as_slice());
        Ok(())
    }

    #[test]
    fn test_encryption_decryption_with_key() -> Result<()> {
        // No compression: length checks are stable (nonce + tag overhead)
        let key = TEST_KEY;
        let ss = SecureStorage::build().with_key(&key);

        let original_data = TEXT.as_slice();
        let encrypted_data = ss.encrypt(original_data)?;
        let decrypted_data = ss.decrypt(&encrypted_data)?;

        assert!(encrypted_data.len() > original_data.len());
        assert_eq!(
            encrypted_data.len() - original_data.len(),
            AES_GCM_NONCE_LEN + 16
        );
        assert_eq!(original_data, decrypted_data.as_slice());
        Ok(())
    }

    #[test]
    fn test_encryption_decryption_no_key() -> Result<()> {
        let ss = SecureStorage::build(); // no key, default compression
        let original_data = TEXT.as_slice();

        let encrypted_data = ss.encrypt(original_data)?;
        let decrypted_data = ss.decrypt(&encrypted_data)?;

        // No key means no encryption, so data should be unchanged
        assert_eq!(original_data, encrypted_data.as_slice());
        assert_eq!(original_data, decrypted_data.as_slice());

        Ok(())
    }

    #[test]
    fn test_encode_decode_with_key_no_compression() -> Result<()> {
        let key = TEST_KEY;
        let ss = SecureStorage::build().with_key(&key);

        let encoded_data = ss.encode(TEXT)?;
        let decoded_data = ss.decode(&encoded_data)?;

        assert!(encoded_data.len() >= TEXT.len());
        assert_eq!(TEXT.as_slice(), decoded_data.as_slice());
        Ok(())
    }

    #[test]
    fn test_encode_decode_no_key_with_compression() -> Result<()> {
        let ss = SecureStorage::build().with_compression(zstd::DEFAULT_COMPRESSION_LEVEL);

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
        let ss = SecureStorage::build().with_key(&key);

        // Shorter than nonce length
        let too_short_data = [0u8; AES_GCM_NONCE_LEN - 1];

        let result = ss.decrypt(&too_short_data);
        assert!(result.is_err());
    }

    #[test]
    fn test_decrypt_tampered_data() -> Result<()> {
        let key = TEST_KEY;
        let ss = SecureStorage::build().with_key(&key);

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
        let ss = SecureStorage::build()
            .with_compression(zstd::DEFAULT_COMPRESSION_LEVEL)
            .with_key(&key);

        let mut ectx = ss.get_encoding_context()?;

        let ciphertext = ss.encode_managed(&mut ectx, TEXT)?;
        let decoded_plaintext = ss.decode(&ciphertext)?;

        assert_eq!(TEXT, decoded_plaintext.as_slice());
        Ok(())
    }
}
