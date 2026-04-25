use aes_gcm_siv::{AeadInPlace, Aes256GcmSiv, Key as AesKey, KeyInit, Nonce, aead::Aead};
use anyhow::{Result, anyhow, bail};
use argon2::Argon2;
use zeroize::Zeroizing;

use crate::backend::WriteContents;
use crate::mapache::{self, defaults::DEFAULT_COMPRESSION};

use parking_lot::Mutex;

const AES_GCM_NONCE_LEN: usize = 12;
const AES_GCM_TAG_LEN: usize = 16;

/// Secure storage is an abstraction for file IO that handles compression and encryption.
pub struct SecureStorage {
    compression_level: i32,
    cipher: Option<Aes256GcmSiv>,
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
            compression_level: DEFAULT_COMPRESSION.to_level(),
            cipher: None,
            compressor_pool: Mutex::new(Vec::new()),
        }
    }

    /// Set compression level
    pub fn with_compression(mut self, level: i32) -> Self {
        self.compression_level = level;
        self
    }

    /// Set a 32-byte key and initialize the cipher (immutable afterward)
    pub fn with_key(mut self, key: &[u8]) -> Self {
        assert_eq!(key.len(), 32);

        let aes_key = AesKey::<Aes256GcmSiv>::from_slice(key);
        self.cipher = Some(Aes256GcmSiv::new(aes_key));
        self
    }

    pub fn get_encoding_context(&self) -> Result<EncodingContext> {
        let mut compressor = zstd::bulk::Compressor::new(self.compression_level)
            .map_err(|e| anyhow!("zstd init failed: {e}"))?;

        // Use a maximum back-reference window the size of the biggest chunk.
        const ZSTD_WINDOW_LOG: u32 = mapache::defaults::NORMAL_CHUNK_SIZE
            .saturating_sub(1)
            .next_power_of_two()
            .ilog2();

        compressor.set_parameter(zstd::zstd_safe::CParameter::WindowLog(ZSTD_WINDOW_LOG))?;
        compressor.set_parameter(zstd::zstd_safe::CParameter::ChecksumFlag(false))?;

        Ok(EncodingContext::new(compressor))
    }

    fn transform_into(&self, ctx: Option<&mut EncodingContext>, data: &[u8]) -> Result<Vec<u8>> {
        let bound = if ctx.is_some() {
            zstd::zstd_safe::compress_bound(data.len())
        } else {
            data.len()
        };

        let has_cipher = self.cipher.is_some();
        let prefix_len = if has_cipher { AES_GCM_NONCE_LEN } else { 0 };
        let suffix_len = if has_cipher { AES_GCM_TAG_LEN } else { 0 };

        let mut out = Vec::with_capacity(prefix_len + bound + suffix_len);

        let mut nonce_bytes = [0u8; AES_GCM_NONCE_LEN];
        if has_cipher {
            nonce_bytes = rand::random();
            out.extend_from_slice(&nonce_bytes);
        }

        let data_start = out.len();
        if let Some(c) = ctx {
            if out.capacity() < data_start + bound {
                out.reserve(data_start + bound - out.len());
            }

            unsafe {
                // SAFETY: Avoiding zero-initialization for performance. Safe because capacity is
                // reserved and zstd::compress_to_buffer initializes the written range.
                let slice = std::slice::from_raw_parts_mut(
                    out.as_mut_ptr().add(data_start) as *mut std::mem::MaybeUninit<u8>,
                    bound,
                );
                let dest = &mut *(slice as *mut [std::mem::MaybeUninit<u8>] as *mut [u8]);
                let n = c
                    .compressor
                    .compress_to_buffer(data, dest)
                    .map_err(|e| anyhow!("zstd failed: {e}"))?;
                out.set_len(data_start + n);
            }
        } else {
            out.extend_from_slice(data);
        }

        if let Some(cipher) = &self.cipher {
            let payload_mut = &mut out[data_start..];
            let tag = cipher
                .encrypt_in_place_detached(Nonce::from_slice(&nonce_bytes), b"", payload_mut)
                .map_err(|_| anyhow!("encryption failed"))?;
            out.extend_from_slice(tag.as_slice());
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
    pub fn compress_managed(&self, ctx: &mut EncodingContext, data: &[u8]) -> Result<Vec<u8>> {
        let temp_ss = Self {
            compression_level: self.compression_level,
            cipher: None,
            compressor_pool: Mutex::new(Vec::new()),
        };
        temp_ss.transform_into(Some(ctx), data)
    }

    pub fn decompress(&self, data: &[u8]) -> Result<Vec<u8>> {
        let mut decompressed = Vec::with_capacity(data.len() * 2);
        zstd::stream::copy_decode(data, &mut decompressed)?;
        Ok(decompressed)
    }

    /// Logic for encryption into a provided vector.
    fn encrypt_into(&self, data: &[u8]) -> Result<Vec<u8>> {
        let Some(cipher) = &self.cipher else {
            return Ok(data.to_vec());
        };

        let total = AES_GCM_NONCE_LEN + data.len() + AES_GCM_TAG_LEN;
        let mut out = Vec::with_capacity(total);

        let nonce_bytes: [u8; AES_GCM_NONCE_LEN] = rand::random();

        out.extend_from_slice(&nonce_bytes);
        out.extend_from_slice(data);

        let payload_mut = &mut out[AES_GCM_NONCE_LEN..];
        let nonce = Nonce::from_slice(&nonce_bytes);
        let tag = cipher
            .encrypt_in_place_detached(nonce, b"", payload_mut)
            .map_err(|_| anyhow!("encryption failed"))?;

        out.extend_from_slice(tag.as_slice());

        Ok(out)
    }

    #[inline]
    pub fn encrypt(&self, data: &[u8]) -> Result<Vec<u8>> {
        self.transform_into(None, data)
    }

    pub fn decrypt<'a>(&self, data: &'a [u8]) -> Result<WriteContents<'a>> {
        let Some(cipher) = &self.cipher else {
            return Ok(WriteContents::Borrowed(data));
        };

        if data.len() < AES_GCM_NONCE_LEN + AES_GCM_TAG_LEN {
            bail!("invalid ciphertext");
        }

        let (nonce, ciphertext_and_tag) = data.split_at(AES_GCM_NONCE_LEN);
        let decrypted = cipher
            .decrypt(Nonce::from_slice(nonce), ciphertext_and_tag)
            .map_err(|_| anyhow!("decryption failed"))?;
        Ok(WriteContents::Owned(decrypted))
    }

    /// Decrypts the given Vec in-place if possible.
    pub fn decrypt_in_place(&self, mut data: Vec<u8>) -> Result<Vec<u8>> {
        let Some(cipher) = &self.cipher else {
            return Ok(data);
        };

        if data.len() < AES_GCM_NONCE_LEN + AES_GCM_TAG_LEN {
            bail!("invalid ciphertext");
        }

        let nonce = Nonce::clone_from_slice(&data[..AES_GCM_NONCE_LEN]);
        let payload_len = data.len() - AES_GCM_NONCE_LEN - AES_GCM_TAG_LEN;

        // Move payload to the beginning
        data.copy_within(AES_GCM_NONCE_LEN..AES_GCM_NONCE_LEN + payload_len, 0);

        let tag_offset = AES_GCM_NONCE_LEN + payload_len;
        let tag = aes_gcm_siv::Tag::clone_from_slice(&data[tag_offset..]);

        data.truncate(payload_len);

        cipher
            .decrypt_in_place_detached(&nonce, b"", &mut data, &tag)
            .map_err(|_| anyhow!("decryption failed"))?;

        Ok(data)
    }

    /// Encrypt using a context (though buffers are no longer held).
    #[inline]
    pub fn encrypt_managed(&self, _ctx: &mut EncodingContext, data: &[u8]) -> Result<Vec<u8>> {
        self.encrypt_into(data)
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

    pub fn take_encoding_context(&self) -> Result<EncodingContext> {
        if let Some(ctx) = self.compressor_pool.lock().pop() {
            return Ok(ctx);
        }
        self.get_encoding_context()
    }

    pub fn return_encoding_context(&self, ctx: EncodingContext) {
        self.compressor_pool.lock().push(ctx);
    }

    pub fn decode(&self, data: &[u8]) -> Result<Vec<u8>> {
        let decrypted = self.decrypt(data)?;
        self.decompress(&decrypted)
    }

    pub fn decode_owned(&self, data: Vec<u8>) -> Result<Vec<u8>> {
        let decrypted = self.decrypt_in_place(data)?;
        self.decompress(&decrypted)
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
            .map_err(|e| anyhow!("argon2 derive failed: {e}"))?;
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
            .with_compression(DEFAULT_COMPRESSION.to_level())
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
        let ss = SecureStorage::new().with_key(&key);

        let original_data = TEXT.as_slice();
        let encrypted_data = ss.encrypt(original_data)?;
        let decrypted_data = ss.decrypt(&encrypted_data)?;

        assert!(encrypted_data.len() > original_data.len());
        assert_eq!(
            encrypted_data.len() - original_data.len(),
            AES_GCM_NONCE_LEN + 16
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
        assert_eq!(original_data, &*decrypted_data);

        Ok(())
    }

    #[test]
    fn test_encode_decode_no_key_with_compression() -> Result<()> {
        let ss = SecureStorage::new().with_compression(DEFAULT_COMPRESSION.to_level());

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
        let ss = SecureStorage::new().with_key(&key);

        // Shorter than nonce length
        let too_short_data = [0u8; AES_GCM_NONCE_LEN - 1];

        let result = ss.decrypt(&too_short_data);
        assert!(result.is_err());
    }

    #[test]
    fn test_decrypt_tampered_data() -> Result<()> {
        let key = TEST_KEY;
        let ss = SecureStorage::new().with_key(&key);

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
            .with_compression(DEFAULT_COMPRESSION.to_level())
            .with_key(&key);

        let mut ectx = ss.get_encoding_context()?;

        let ciphertext = ss.encode_managed(&mut ectx, TEXT)?;
        let decoded_plaintext = ss.decode(&ciphertext)?;

        assert_eq!(TEXT, decoded_plaintext.as_slice());
        Ok(())
    }

    #[test]
    fn test_decrypt_in_place() -> Result<()> {
        let key = TEST_KEY;
        let ss = SecureStorage::new().with_key(&key);

        let encrypted_data = ss.encrypt(TEXT)?;
        let decrypted_data = ss.decrypt_in_place(encrypted_data)?;

        assert_eq!(TEXT.as_slice(), decrypted_data.as_slice());
        Ok(())
    }

    #[test]
    fn test_decode_owned() -> Result<()> {
        let key = TEST_KEY;
        let ss = SecureStorage::new()
            .with_compression(DEFAULT_COMPRESSION.to_level())
            .with_key(&key);

        let encoded_data = ss.encode(TEXT)?;
        let decoded_data = ss.decode_owned(encoded_data)?;

        assert_eq!(TEXT.as_slice(), decoded_data.as_slice());
        Ok(())
    }
}
