use std::io::Read;

use aes_gcm_siv::{Aes256GcmSiv, Key as AesKey, KeyInit, Nonce, aead::Aead};
use anyhow::{Result, anyhow, bail};
use argon2::Argon2;
use rand::{TryRngCore, rngs::OsRng};
use zstd::{Decoder as ZstdDecoder, bulk::Compressor as ZstdCompressor, zstd_safe::CParameter};

use crate::mapache;

const AES_GCM_NONCE_LEN: usize = 12;
const ZSTD_WINDOW_LOG: u32 = mapache::defaults::NORMAL_CHUNK_SIZE.ilog2();

/// Secure storage is an abstraction for file IO that handles compression and encryption.
pub struct SecureStorage {
    compression_level: i32,
    cipher: Option<Aes256GcmSiv>,
}

impl SecureStorage {
    pub fn build() -> Self {
        Self {
            compression_level: -1, // No compression by default (level -1)
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

    /// compress → encrypt
    pub fn encode(&self, data: &[u8]) -> Result<Vec<u8>> {
        let compressed = self.compress(data)?;
        self.encrypt(&compressed)
    }

    /// decrypt → decompress
    pub fn decode(&self, data: &[u8]) -> Result<Vec<u8>> {
        let decrypted = self.decrypt(data)?;
        self.decompress(&decrypted)
    }

    pub fn compress(&self, data: &[u8]) -> Result<Vec<u8>> {
        let mut comp = ZstdCompressor::new(self.compression_level)
            .map_err(|e| anyhow!("zstd compressor init failed: {e}"))?;

        // Set parameters
        let _ = comp.set_parameter(CParameter::WindowLog(ZSTD_WINDOW_LOG));
        let _ = comp.set_parameter(CParameter::ChecksumFlag(false));

        let bound = zstd::zstd_safe::compress_bound(data.len());
        let mut out = Vec::with_capacity(bound);

        let n = comp
            .compress_to_buffer(data, &mut out)
            .map_err(|e| anyhow!("zstd compress failed: {e}"))?;

        unsafe { out.set_len(n) };
        Ok(out)
    }

    pub fn decompress(&self, data: &[u8]) -> Result<Vec<u8>> {
        let mut decoder = ZstdDecoder::new(data)?;
        decoder.window_log_max(ZSTD_WINDOW_LOG)?;

        let mut decompressed = Vec::with_capacity(data.len());
        decoder.read_to_end(&mut decompressed)?;
        Ok(decompressed)
    }

    pub fn encrypt(&self, data: &[u8]) -> Result<Vec<u8>> {
        let Some(cipher) = &self.cipher else {
            return Ok(data.to_vec());
        };

        let mut nonce = [0u8; AES_GCM_NONCE_LEN];
        OsRng
            .try_fill_bytes(&mut nonce)
            .map_err(|e| anyhow!("rng failed: {e}"))?;

        let mut out = Vec::with_capacity(AES_GCM_NONCE_LEN + data.len() + 16);
        out.extend_from_slice(&nonce);
        match cipher.encrypt(Nonce::from_slice(&nonce), data) {
            Ok(mut ct) => {
                out.append(&mut ct);
                Ok(out)
            }
            Err(_) => bail!("encryption failed"),
        }
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

    /// Generate a cryptographically random salt
    pub fn generate_salt<const LENGTH: usize>() -> [u8; LENGTH] {
        let mut salt = [0u8; LENGTH];
        OsRng.try_fill_bytes(&mut salt).expect("OS RNG failed");
        salt
    }
}

#[cfg(test)]
mod tests {
    use rstest::rstest;
    use zstd::DEFAULT_COMPRESSION_LEVEL;

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
            .with_compression(DEFAULT_COMPRESSION_LEVEL)
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
        let ss = SecureStorage::build().with_compression(DEFAULT_COMPRESSION_LEVEL);

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
}
