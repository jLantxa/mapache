// mapache is a secure, de-duplicating, incremental backup tool.
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

use std::{
    path::{Path, PathBuf},
    sync::Arc,
};

use anyhow::{Context, Result, bail};
use argon2;
use base64::Engine;
use chrono::{DateTime, Local};
use rand::{TryRngCore, rngs::OsRng};
use serde::{Deserialize, Serialize};
use zstd::DEFAULT_COMPRESSION_LEVEL;

use crate::{
    backend::StorageBackend,
    global::{self, ID},
    repository::{
        repo::{Auth, KEYS_DIR},
        storage::SecureStorage,
    },
    ui,
};

mod argon2_defaults {

    pub(crate) const fn default_m() -> u32 {
        argon2::Params::DEFAULT_M_COST
    }

    pub(crate) const fn default_t() -> u32 {
        argon2::Params::DEFAULT_T_COST
    }

    pub(crate) const fn default_p() -> u32 {
        argon2::Params::DEFAULT_P_COST
    }
}

/// A metadata structure that contains information about a repository key
#[derive(Debug, Serialize, Deserialize)]
pub struct KeyFile {
    pub created: DateTime<Local>,
    pub username: String,

    #[serde(default = "argon2_defaults::default_m")]
    pub m: u32, // Argon2 memory size
    #[serde(default = "argon2_defaults::default_t")]
    pub t: u32, // Argon2 iterations
    #[serde(default = "argon2_defaults::default_p")]
    pub p: u32, // Argon2 parallelism

    pub salt: String,
    pub encrypted_key: String,
}

impl KeyFile {
    pub fn argon2_params(&self) -> argon2::Params {
        argon2::ParamsBuilder::new()
            .m_cost(self.m)
            .t_cost(self.t)
            .p_cost(self.p)
            .build()
            .expect("Parameters should be valid")
    }
}

pub struct KeyManager {
    backend: Arc<dyn StorageBackend>,
}

impl KeyManager {
    pub fn new(backend: Arc<dyn StorageBackend>) -> Self {
        Self { backend }
    }

    pub fn generate_new_master_key() -> Vec<u8> {
        let mut new_random_key = vec![0u8; 32];
        if let Err(e) = OsRng.try_fill_bytes(&mut new_random_key) {
            panic!("Error: {e}");
        }

        new_random_key
    }

    pub fn decode_master_key(password: &str, keyfile: &KeyFile) -> Result<Vec<u8>> {
        // Decode salt and key from base64
        let salt = base64::engine::general_purpose::STANDARD.decode(keyfile.salt.clone())?;
        let encrypted_key =
            base64::engine::general_purpose::STANDARD.decode(keyfile.encrypted_key.clone())?;

        let intermediate_key =
            SecureStorage::derive_key::<32>(password, &salt, keyfile.argon2_params())?;
        SecureStorage::decrypt_with_key(&intermediate_key, &encrypted_key)
            .with_context(|| "Could not retrieve master key from this keyfile")
    }

    /// Generates a new KeyFile for the master key with a new password
    pub fn generate_key_file(auth: &Auth, master_key: Vec<u8>) -> Result<KeyFile> {
        let create_time = Local::now();

        let argon2_params = argon2::Params::default();

        const SALT_LENGTH: usize = 32;
        let salt = SecureStorage::generate_salt::<SALT_LENGTH>();
        let intermediate_key =
            SecureStorage::derive_key::<32>(&auth.password, &salt, argon2_params.clone())?;

        let encrypted_key = SecureStorage::encrypt_with_key(&intermediate_key, &master_key)?;

        let key_file = KeyFile {
            created: create_time,
            username: auth.username.clone(),
            m: argon2_params.m_cost(),
            t: argon2_params.t_cost(),
            p: argon2_params.p_cost(),
            encrypted_key: base64::engine::general_purpose::STANDARD.encode(encrypted_key),
            salt: base64::engine::general_purpose::STANDARD.encode(salt),
        };

        Ok(key_file)
    }

    /// Retrieve the master key from all available keys in a folder
    pub fn retrieve_master_key(
        &self,
        auth: &Auth,
        keyfile_path: Option<&PathBuf>,
    ) -> Result<(Option<ID>, Vec<u8>)> {
        match keyfile_path {
            Some(path) => {
                let keyfile = std::fs::read(path)?;
                let keyfile = SecureStorage::decompress(&keyfile)?;
                let keyfile: KeyFile = serde_json::from_slice(&keyfile)
                    .with_context(|| format!("KeyFile at {path:?} is invalid"))?;

                Ok((None, Self::decode_master_key(&auth.password, &keyfile)?))
            }
            None => {
                let keyfile_streamer = KeyFileStreamer::new(self.backend.clone())?;
                for (id, keyfile) in keyfile_streamer.flatten() {
                    // Discard this key if it belongs to a different user
                    if keyfile.username != auth.username {
                        continue;
                    }

                    if let Ok(master_key) = Self::decode_master_key(&auth.password, &keyfile) {
                        return Ok((Some(id), master_key));
                    }
                }

                Err(anyhow::anyhow!(
                    "No valid KeyFile found for the provided password in the keys directory."
                ))
            }
        }
    }

    /// Load a keyfile with a given ID
    pub fn load_keyfile_with_id(&self, id: &ID) -> Result<Option<KeyFile>> {
        let path = PathBuf::from(KEYS_DIR).join(id.to_hex());

        if !self.backend.exists(&path) {
            return Ok(None);
        }

        let keyfile = self.backend.read(&path)?;
        let keyfile: KeyFile = serde_json::from_slice(&keyfile)?;
        Ok(Some(keyfile))
    }

    /// Load a keyfile with a given username
    pub fn load_keyfile_with_username(&self, username: &str) -> Result<Option<(ID, KeyFile)>> {
        let mut keyfile_streamer = KeyFileStreamer::new(self.backend.clone())?;

        // Find the first keyfile that matches the username
        let first_match = keyfile_streamer
            .by_ref()
            .flatten()
            .find(|(_id, keyfile)| keyfile.username == username);

        // If a first match was found, check for a second one
        if let Some(keyfile) = first_match {
            let has_second_match = keyfile_streamer
                .flatten()
                .any(|(_id, keyfile)| keyfile.username == username);

            // If a second match exists, return an error
            if has_second_match {
                bail!("More than one Keyfile found for username {username}");
            }

            Ok(Some(keyfile))
        } else {
            Ok(None)
        }
    }

    /// Delete a keyfile with a given ID
    pub fn delete_keyfile_with_id(&self, id: &ID) -> Result<()> {
        let path = PathBuf::from(KEYS_DIR).join(id.to_hex());
        self.backend.remove_file(&path)
    }

    /// Delete keyfiles with a given username
    pub fn delete_keyfile_with_username(&self, username: &str) -> Result<()> {
        let keyfile_streamer = KeyFileStreamer::new(self.backend.clone())?;

        for (id, keyfile) in keyfile_streamer.flatten() {
            if keyfile.username == username {
                self.delete_keyfile_with_id(&id)?;
            }
        }

        Ok(())
    }

    /// Save a KeyFile
    pub fn save_keyfile(&self, keyfile: &KeyFile) -> Result<ID> {
        let keyfile_json = serde_json::to_string(keyfile)?;
        let keyfile_json =
            SecureStorage::compress(keyfile_json.as_bytes(), DEFAULT_COMPRESSION_LEVEL)?;
        let id = ID::from_content(&keyfile_json);
        let path = PathBuf::from(KEYS_DIR).join(id.to_hex());
        self.backend.write(&path, &keyfile_json)?;

        Ok(id)
    }

    /// Find a KeyFile ID using a prefix
    pub fn find_id_with_prefix(&self, prefix: &str) -> Result<(ID, PathBuf)> {
        if prefix.len() > 2 * global::ID_LENGTH {
            // A hex string has 2 characters per byte.
            bail!(
                "Invalid prefix length. The prefix must not be longer than the ID ({} chars)",
                2 * global::ID_LENGTH
            );
        } else if prefix.is_empty() {
            bail!("Prefix cannot be empty");
        }

        let files = self.backend.read_dir(Path::new(KEYS_DIR))?;
        let mut matches = Vec::new();

        for file_path in files {
            let filename = file_path
                .file_name()
                .expect("File should have a name")
                .to_string_lossy()
                .into_owned();

            if !filename.starts_with(prefix) {
                continue;
            }

            if matches.is_empty() {
                matches.push((filename, file_path));
            } else {
                bail!("Prefix {} is ambiguous", prefix);
            }
        }

        if matches.is_empty() {
            bail!("Keyfile with prefix {} doesn't exist", prefix);
        }

        let (filename, filepath) = matches.pop().unwrap();
        let id = ID::from_hex(&filename)?;

        Ok((id, filepath))
    }
}

/// A KeyFile streamer.
///
/// This streamer loads KeyFile on demand.
pub struct KeyFileStreamer {
    backend: Arc<dyn StorageBackend>,
    entries: Vec<PathBuf>,
}

impl KeyFileStreamer {
    pub fn new(backend: Arc<dyn StorageBackend>) -> Result<Self> {
        let entries = backend.read_dir(Path::new(KEYS_DIR))?;
        Ok(Self { backend, entries })
    }
}

impl Iterator for KeyFileStreamer {
    type Item = Result<(ID, KeyFile)>;

    fn next(&mut self) -> Option<Self::Item> {
        while let Some(path) = self.entries.pop() {
            if !self.backend.is_file(&path) {
                ui::cli::warning!(
                    "Extraneous item '{}' in keys directory is not a file",
                    path.display()
                );
                continue;
            }

            let keyfile_data = match self.backend.read(&path) {
                Ok(data) => data,
                Err(e) => return Some(Err(e)),
            };

            let keyfile_decompressed = match SecureStorage::decompress(&keyfile_data) {
                Ok(data) => data,
                Err(e) => return Some(Err(e)),
            };

            let keyfile: KeyFile = match serde_json::from_slice(keyfile_decompressed.as_slice()) {
                Ok(kf) => kf,
                Err(e) => {
                    ui::cli::warning!("Failed to parse keyfile at {}: {}", path.display(), e);
                    continue;
                }
            };

            let id = match ID::from_hex(
                path.file_name()
                    .expect("File should have a name")
                    .to_string_lossy()
                    .as_ref(),
            ) {
                Err(e) => return Some(Err(e)),
                Ok(id) => id,
            };
            return Some(Ok((id, keyfile)));
        }

        None
    }
}
