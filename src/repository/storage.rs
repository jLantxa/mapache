// mapache is an incremental backup tool
// Copyright (C) 2025  Javier Lancha Vázquez <javier.lancha@gmail.com>
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program.  If not, see <https://www.gnu.org/licenses/>.

use aes_gcm_siv::{Aes256GcmSiv, Key as AesKey, KeyInit, Nonce, aead::Aead};
use anyhow::{Result, bail};
use argon2::Argon2;
use rand::TryRngCore;
use rand::rngs::OsRng;
use secrecy::zeroize::Zeroize;
use secrecy::{ExposeSecret, SecretBox};
use std::io::{Read, Write};
use zstd::stream::read::Decoder as ZstdDecoder;
use zstd::stream::write::Encoder as ZstdEncoder;

use crate::global;

const AES_GCM_NONCE_LEN: usize = 12;
const ZSTD_WINDOW_LOG: u32 = global::defaults::AVG_CHUNK_SIZE.ilog2();

/// Secure storage is an abstraction for file IO that handles compression and encryption.
pub struct SecureStorage {
    key: Option<SecretBox<Vec<u8>>>,
    compression_level: i32,
}

impl SecureStorage {
    /// A new, default SecureStorage with no encryption and no compression
    pub fn build() -> Self {
        Self {
            key: Default::default(),
            compression_level: Default::default(),
        }
    }

    /// Builder method to set an encryption key
    pub fn with_key(mut self, key: Vec<u8>) -> Self {
        assert_eq!(key.len(), 32);
        self.key = Some(SecretBox::new(Box::new(key)));
        self
    }

    /// Builder method to set a compression level
    pub fn with_compression(mut self, level: i32) -> Self {
        self.compression_level = level;
        self
    }

    pub fn encode(&self, data: &[u8]) -> Result<Vec<u8>> {
        let mut processed_data = Self::compress(data, self.compression_level)?;
        processed_data = self.encrypt(&processed_data)?;
        Ok(processed_data)
    }

    pub fn decode(&self, data: &[u8]) -> Result<Vec<u8>> {
        let mut processed_data = self.decrypt(data)?;
        processed_data = Self::decompress(&processed_data)?;
        Ok(processed_data)
    }

    /// Compress a stream of bytes
    pub fn compress(data: &[u8], compression_level: i32) -> Result<Vec<u8>> {
        let mut compressed = Vec::with_capacity(data.len());
        let mut encoder = ZstdEncoder::new(&mut compressed, compression_level)?;

        encoder.set_parameter(zstd::zstd_safe::CParameter::WindowLog(ZSTD_WINDOW_LOG))?;
        encoder.set_parameter(zstd::zstd_safe::CParameter::ChecksumFlag(false))?;

        encoder.write_all(data)?;
        encoder.finish()?;
        Ok(compressed)
    }

    /// Decompress a stream of bytes
    pub fn decompress(data: &[u8]) -> Result<Vec<u8>> {
        let mut decoder = ZstdDecoder::new(data)?;
        decoder.window_log_max(ZSTD_WINDOW_LOG)?;

        let mut decompressed = Vec::with_capacity(data.len());
        decoder.read_to_end(&mut decompressed)?;
        Ok(decompressed)
    }

    /// Encrypt data using AES-GCM
    pub fn encrypt_with_key(key: &[u8], data: &[u8]) -> Result<Vec<u8>> {
        let key = AesKey::<Aes256GcmSiv>::from_slice(key);
        let cipher = Aes256GcmSiv::new(key);

        // Generate a random nonce for each encryption
        let mut nonce = vec![0u8; AES_GCM_NONCE_LEN];
        if let Err(e) = OsRng.try_fill_bytes(&mut nonce) {
            panic!("Error: {e}");
        }

        match cipher.encrypt(Nonce::from_slice(&nonce), data) {
            Ok(mut ciphertext) => {
                // Return nonce + ciphertext

                let mut out = Vec::with_capacity(AES_GCM_NONCE_LEN + ciphertext.len());
                out.append(&mut nonce);
                out.append(&mut ciphertext);
                Ok(out)
            }
            Err(_) => bail!("Encryption failed"),
        }
    }

    pub fn encrypt(&self, data: &[u8]) -> Result<Vec<u8>> {
        match &self.key {
            Some(key_secret) => Self::encrypt_with_key(key_secret.expose_secret(), data),
            None => Ok(data.to_vec()),
        }
    }

    /// Decrypt data using AES-GCM
    pub fn decrypt_with_key(key: &[u8], data: &[u8]) -> Result<Vec<u8>> {
        let key = AesKey::<Aes256GcmSiv>::from_slice(key);
        let cipher = Aes256GcmSiv::new(key);

        if data.len() < AES_GCM_NONCE_LEN {
            bail!("Decryption failed: invalid data");
        }

        // Extract the nonce from the first 12 bytes of the data
        let (nonce, ciphertext) = data.split_at(AES_GCM_NONCE_LEN);
        let nonce = Nonce::from_slice(nonce);

        match cipher.decrypt(nonce, ciphertext) {
            Ok(plaintext) => Ok(plaintext),
            Err(_) => bail!("Decryption failed"),
        }
    }

    pub fn decrypt(&self, data: &[u8]) -> Result<Vec<u8>> {
        match &self.key {
            Some(key_secret) => Self::decrypt_with_key(key_secret.expose_secret(), data),
            None => Ok(data.to_vec()),
        }
    }

    /// Derive a key from a password and a salt
    pub fn derive_key(password: &str, salt: &[u8]) -> [u8; 32] {
        let mut output_key_material = [0u8; 32];
        let _ = Argon2::default().hash_password_into(
            password.as_bytes(),
            salt,
            &mut output_key_material,
        );

        output_key_material
    }

    /// Generate a random salt of a given length
    pub fn generate_salt<const LENGTH: usize>() -> [u8; LENGTH] {
        let mut salt = [0u8; LENGTH];
        if let Err(e) = OsRng.try_fill_bytes(&mut salt) {
            panic!("Error: {e}");
        }
        salt
    }
}

impl Drop for SecureStorage {
    fn drop(&mut self) {
        // Zeroize the key on drop
        self.key.zeroize();
    }
}

#[cfg(test)]
mod tests {
    use zstd::DEFAULT_COMPRESSION_LEVEL;

    use crate::{repository::keys::generate_new_master_key, ui};

    use super::*;

    const TEXT: &[u8; 431] = br#"
Lorem ipsum dolor sit amet, consectetur adipisici elit, sed eiusmod tempor incidunt
ut labore et dolore magna aliqua. Ut enim ad minim veniam, quis nostrud exercitation
ullamco laboris nisi ut aliquid ex ea commodi consequat. Quis aute iure reprehenderit in
voluptate velit esse cillum dolore eu fugiat nulla pariatur. Excepteur sint obcaecat
cupiditat non proident, sunt in culpa qui officia deserunt mollit anim id est laborum.
"#;

    #[test]
    fn test_compression_and_decompression() {
        let original_data = TEXT;

        let compression_levels = [0, 10, 22];

        for &compression_level in &compression_levels {
            let compressed_data =
                SecureStorage::compress(original_data, compression_level).unwrap();
            let decompressed_data = SecureStorage::decompress(&compressed_data).unwrap();

            assert_eq!(*original_data, *decompressed_data);

            let ratio = original_data.len() as f64 / compressed_data.len() as f64;
            ui::cli::log!(
                "Compression level {}: Ratio = {:.2}",
                compression_level,
                ratio
            );
        }
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
    fn test_deterministic_encryption() -> Result<()> {
        let key = generate_new_master_key();
        let secure_storage = SecureStorage::build()
            .with_compression(DEFAULT_COMPRESSION_LEVEL)
            .with_key(key);
        let ciphertext = secure_storage.encode(TEXT)?;
        let decoded_plaintext = secure_storage.decode(&ciphertext)?;

        assert_eq!(TEXT, decoded_plaintext.as_slice());

        Ok(())
    }
}
