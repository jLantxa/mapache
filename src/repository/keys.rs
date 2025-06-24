// [backup] is an incremental backup tool
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

use aes_gcm::aead::{OsRng, rand_core::RngCore};
use anyhow::{Context, Result};
use base64::Engine;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::{
    backend::StorageBackend,
    repository::{KEYS_DIR, storage::SecureStorage},
    ui,
};

/// A metadata structure that contains information about a repository key
#[derive(Serialize, Deserialize)]
pub struct KeyFile {
    pub created: DateTime<Utc>,
    pub encrypted_key: String,
    pub salt: String,
}

pub fn generate_new_master_key() -> Vec<u8> {
    let mut new_random_key = vec![0u8; 32];
    OsRng.fill_bytes(&mut new_random_key);
    new_random_key
}

/// Generates a new KeyFile for the master key with a new password
pub fn generate_key_file(password: &str, master_key: Vec<u8>) -> Result<KeyFile> {
    let create_time = Utc::now();

    const SALT_LENGTH: usize = 32;
    let salt = SecureStorage::generate_salt::<SALT_LENGTH>();
    let intermediate_key = SecureStorage::derive_key(password, &salt);

    let encrypted_key = SecureStorage::encrypt_with_key(&intermediate_key, &master_key)?;

    let key_file = KeyFile {
        created: create_time,
        encrypted_key: base64::engine::general_purpose::STANDARD.encode(encrypted_key),
        salt: base64::engine::general_purpose::STANDARD.encode(salt),
    };

    Ok(key_file)
}

/// Retrieve the master key from all available keys in a folder
pub fn retrieve_master_key(
    password: &str,
    keyfile_path: Option<&PathBuf>,
    backend: Arc<dyn StorageBackend>,
) -> Result<Vec<u8>> {
    match keyfile_path {
        Some(path) => {
            let file = std::fs::File::open(&path)
                .with_context(|| format!("Could not open KeyFile at {:?}", path))?;
            let keyfile: KeyFile = serde_json::from_reader(file)
                .with_context(|| format!("KeyFile at {:?} is invalid", path))?;

            decode_master_key(&password, keyfile)
        }
        None => {
            let keys_path = Path::new(KEYS_DIR);
            let entries = backend.read_dir(&keys_path)?;

            for path in entries {
                // The keys directory should only contain files. We can ignore anything
                // that is not a file, but show a warning anyway.
                if !backend.is_file(&path) {
                    ui::cli::warning!(
                        "Extraneous item \'{}\' in keys directory is not a file",
                        path.display()
                    );
                    continue;
                }

                // Load keyfile
                let keyfile_str = backend.read(&path)?;
                let keyfile: KeyFile = match serde_json::from_slice(keyfile_str.as_slice()) {
                    Ok(kf) => kf,
                    Err(e) => {
                        ui::cli::warning!("Failed to parse keyfile at {}: {}", path.display(), e);
                        continue;
                    }
                };

                if let Ok(master_key) = decode_master_key(&password, keyfile) {
                    return Ok(master_key);
                }
            }

            Err(anyhow::anyhow!(
                "No valid KeyFile found for the provided password in the keys directory."
            )
            .into())
        }
    }
}

fn decode_master_key(password: &str, keyfile: KeyFile) -> Result<Vec<u8>> {
    // Decode salt and key from base64
    let salt = base64::engine::general_purpose::STANDARD.decode(keyfile.salt)?;
    let encrypted_key = base64::engine::general_purpose::STANDARD.decode(keyfile.encrypted_key)?;

    let intermediate_key = SecureStorage::derive_key(&password, &salt);
    SecureStorage::decrypt_with_key(&intermediate_key, &encrypted_key)
        .with_context(|| "Could not retrieve master key from this keyfile")
}
