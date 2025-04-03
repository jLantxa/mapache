/*
 * [backup] is an incremental backup tool
 * Copyright (C) 2025  Javier Lancha Vázquez <javier.lancha@gmail.com>
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
*/

use aes_gcm::aead::Aead;
use aes_gcm::{Aes256Gcm, Key, KeyInit, Nonce};
use anyhow::{Context, Result, bail};
use argon2::Argon2;
use rand::{Rng, RngCore};
use secrecy::zeroize::Zeroize;
use secrecy::{ExposeSecret, SecretBox};
use serde::Serialize;
use serde::de::DeserializeOwned;
use std::io::{Read, Write};
use std::path::Path;
use zstd::stream::read::Decoder as ZstdDecoder;
use zstd::stream::write::Encoder as ZstdEncoder;

const SALT_LENGTH: usize = 16;

/// Secure storage is an abstraction for file IO that handles compression and encryption.
pub struct SecureStorage {
    password: SecretBox<Vec<u8>>,
}

impl SecureStorage {
    /// Create a new instance of SecureStorage with a password
    pub fn new(password: String) -> Self {
        SecureStorage {
            password: SecretBox::new(Box::new(password.as_bytes().to_vec())),
        }
    }

    /// Load a file previously saved with SecureStorage
    pub fn load_from_file(&self, path: &Path) -> Result<Vec<u8>> {
        let data = std::fs::read(path)?;
        let decrypted_data = self.decrypt(&data)?;
        Self::decompress(&decrypted_data)
    }

    /// Save data to a file with SecureStorage
    pub fn save_to_file(&self, data: &[u8], path: &Path, compression_level: i32) -> Result<usize> {
        let compressed_data = Self::compress(data, compression_level)?;
        let encrypted_data = self.encrypt(&compressed_data)?;
        std::fs::write(path, &encrypted_data)?;
        Ok(encrypted_data.len())
    }

    /// Serialize a JSON metadata file.
    pub fn load_json<T: DeserializeOwned>(&self, path: &Path) -> Result<T> {
        let data = self
            .load_from_file(path)
            .with_context(|| "Could not deserialize metadata")?;
        let text = String::from_utf8(data)?;
        serde_json::from_str(&text).with_context(|| "Could not load metadata")
    }

    /// Serialize a JSON metadata file.
    pub fn save_json<T: Serialize>(
        &self,
        metadata: &T,
        path: &Path,
        compression_level: i32,
    ) -> Result<()> {
        let serialized_txt =
            serde_json::to_string(metadata).with_context(|| "Could not serialize metadata")?;
        let data = serialized_txt.as_bytes().to_vec();
        self.save_to_file(&data, path, compression_level)
            .with_context(|| "Could not save metadata")?;

        Ok(())
    }

    /// Compress a stream of bytes
    fn compress(data: &[u8], compression_level: i32) -> Result<Vec<u8>> {
        let mut compressed = Vec::new();
        let mut encoder = ZstdEncoder::new(&mut compressed, compression_level)?;
        encoder.write_all(data)?;
        encoder.finish()?;
        Ok(compressed)
    }

    /// Decompress a stream of bytes
    fn decompress(data: &[u8]) -> Result<Vec<u8>> {
        let mut decompressed = Vec::new();
        let mut decoder = ZstdDecoder::new(data)?;
        decoder.read_to_end(&mut decompressed)?;
        Ok(decompressed)
    }

    /// Encrypt data using AES-GCM
    fn encrypt(&self, data: &[u8]) -> Result<Vec<u8>> {
        let salt = Self::generate_salt::<SALT_LENGTH>();
        let key = self.derive_key(&salt);

        let key = Key::<Aes256Gcm>::from_slice(&key);
        let cipher = Aes256Gcm::new(&key);

        // Generate a random nonce for each encryption
        let mut nonce = [0u8; 12];
        rand::rng().fill(&mut nonce);
        let nonce = Nonce::from_slice(&nonce);

        match cipher.encrypt(nonce, data) {
            Ok(encrypted_data) => {
                // Return salt + nonce + encrypted data as the result
                // The salt must be stored together with the data.
                Ok([salt.as_slice(), nonce.as_slice(), &encrypted_data].concat())
            }
            Err(_) => bail!("Encryption failed"),
        }
    }

    /// Decrypt data using AES-GCM
    fn decrypt(&self, data: &[u8]) -> Result<Vec<u8>> {
        // Recover the salt to derive the key
        let salt = &data[0..SALT_LENGTH];

        let data = &data[SALT_LENGTH..];
        let key = self.derive_key(salt);

        let key = Key::<Aes256Gcm>::from_slice(&key);
        let cipher = Aes256Gcm::new(key);

        // Extract the nonce from the first 12 bytes of the data
        let (nonce, ciphertext) = data.split_at(12);
        let nonce = Nonce::from_slice(nonce);

        match cipher.decrypt(nonce, ciphertext) {
            Ok(decrypted_data) => Ok(decrypted_data),
            Err(_) => bail!("Decryption failed"),
        }
    }

    fn derive_key(&self, salt: &[u8]) -> [u8; 32] {
        let mut output_key_material = [0u8; 32];
        let _ = Argon2::default().hash_password_into(
            self.password.expose_secret(),
            salt,
            &mut output_key_material,
        );

        output_key_material
    }

    fn generate_salt<const LENGTH: usize>() -> [u8; LENGTH] {
        let mut rng = rand::rng();
        let mut salt = [0u8; LENGTH];
        rng.fill_bytes(&mut salt);
        salt
    }
}

impl Drop for SecureStorage {
    fn drop(&mut self) {
        self.password.zeroize();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_compression_and_decompression() {
        let original_data = br#"
             Lorem ipsum dolor sit amet, consectetur adipisici elit, sed eiusmod tempor incidunt
             ut labore et dolore magna aliqua. Ut enim ad minim veniam, quis nostrud exercitation
             ullamco laboris nisi ut aliquid ex ea commodi consequat. Quis aute iure reprehenderit in
             voluptate velit esse cillum dolore eu fugiat nulla pariatur. Excepteur sint obcaecat
             cupiditat non proident, sunt in culpa qui officia deserunt mollit anim id est laborum.
             "#;

        let compression_levels = [0, 10, 22];

        for &compression_level in &compression_levels {
            let compressed_data =
                SecureStorage::compress(original_data, compression_level).unwrap();
            let decompressed_data = SecureStorage::decompress(&compressed_data).unwrap();

            assert_eq!(*original_data, *decompressed_data);

            let ratio = original_data.len() as f64 / compressed_data.len() as f64;
            println!(
                "Compression level {}: Ratio = {:.2}",
                compression_level, ratio
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
}
