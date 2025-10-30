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

use crate::{
    backend::{Handle, StorageBackend},
    mapache::{self, ContentIdType, ID},
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
        let ss = SecureStorage::build()
            .with_compression(zstd::DEFAULT_COMPRESSION_LEVEL)
            .with_key(&intermediate_key);

        ss.decrypt(&encrypted_key)
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

        let ss = SecureStorage::build()
            .with_compression(zstd::DEFAULT_COMPRESSION_LEVEL)
            .with_key(&intermediate_key);

        let encrypted_key = ss.encrypt(&master_key)?;

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
                let ss = SecureStorage::build().with_compression(zstd::DEFAULT_COMPRESSION_LEVEL);

                let keyfile = std::fs::read(path)?;
                let keyfile = ss.decompress(&keyfile)?;
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

        if !self.backend.path_exists(&path) {
            return Ok(None);
        }

        let ss = SecureStorage::build().with_compression(zstd::DEFAULT_COMPRESSION_LEVEL);

        let handle = Handle::new_with_hint(&path, ContentIdType::Key, true);
        let keyfile = self.backend.read(&handle, 0, 0)?;
        let keyfile = ss.decompress(&keyfile)?;
        let keyfile: KeyFile = serde_json::from_slice(&keyfile)?;

        Ok(Some(keyfile))
    }

    /// Load a keyfile with a given username
    pub fn load_keyfile_with_username(&self, username: &str) -> Result<Option<(ID, KeyFile)>> {
        let keyfile_streamer = KeyFileStreamer::new(self.backend.clone())?;

        // Collect all valid keyfiles for the given username in a single pass.
        // Flattens the Result<T> iterator, silently skipping corrupt files.
        let matches: Vec<(ID, KeyFile)> = keyfile_streamer
            .flatten()
            .filter(|(_id, keyfile)| keyfile.username == username)
            .collect();

        match matches.len() {
            0 => Ok(None),
            1 => Ok(Some(matches.into_iter().next().unwrap())),
            _ => bail!("More than one Keyfile found for username {username}"),
        }
    }

    /// Delete a keyfile with a given ID
    pub fn delete_keyfile_with_id(&self, id: &ID) -> Result<()> {
        let path = PathBuf::from(KEYS_DIR).join(id.to_hex());
        self.backend.remove(&path)
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
        let ss = SecureStorage::build().with_compression(zstd::DEFAULT_COMPRESSION_LEVEL);

        let keyfile_json = serde_json::to_string(keyfile)?;
        let keyfile_json = ss.compress(keyfile_json.as_bytes())?;
        let id = ID::from_content(&keyfile_json);
        let path = PathBuf::from(KEYS_DIR).join(id.to_hex());
        self.backend.write(
            &Handle::new_with_hint(&path, ContentIdType::Key, true),
            &keyfile_json,
        )?;

        Ok(id)
    }

    /// Find a KeyFile ID using a prefix
    pub fn find_id_with_prefix(&self, prefix: &str) -> Result<(ID, PathBuf)> {
        if prefix.len() > 2 * mapache::ID_LENGTH {
            // A hex string has 2 characters per byte.
            bail!(
                "Invalid prefix length. The prefix must not be longer than the ID ({} chars)",
                2 * mapache::ID_LENGTH
            );
        } else if prefix.is_empty() {
            bail!("Prefix cannot be empty");
        }

        let files = self.backend.list_dir(Path::new(KEYS_DIR))?;
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
                bail!("Prefix {prefix} is ambiguous");
            }
        }

        if matches.is_empty() {
            bail!("Keyfile with prefix {prefix} doesn't exist");
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
        let entries = backend.list_dir(Path::new(KEYS_DIR))?;
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

            let id = match ID::from_hex(
                path.file_name()
                    .expect("File should have a name")
                    .to_string_lossy()
                    .as_ref(),
            ) {
                Err(_) => continue, // Ignore key files with invalid ID names
                Ok(id) => id,
            };

            let ss = SecureStorage::build().with_compression(zstd::DEFAULT_COMPRESSION_LEVEL);

            let keyfile_data = match self.backend.read(
                &Handle::new_with_hint(&path, ContentIdType::Key, true),
                0,
                0,
            ) {
                Ok(data) => data,
                Err(e) => return Some(Err(e)),
            };

            let keyfile_decompressed = match ss.decompress(&keyfile_data) {
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

            return Some(Ok((id, keyfile)));
        }

        None
    }
}

#[cfg(test)]
mod tests {
    use tempfile::tempdir;

    use crate::backend::localfs::LocalFS;

    use super::*;

    #[test]
    fn test_key_lifecycle_and_decode_success() -> Result<()> {
        let original_master_key = KeyManager::generate_new_master_key();

        let auth = Auth {
            username: "test_user".to_string(),
            password: "correct_password".to_string(),
        };
        let keyfile = KeyManager::generate_key_file(&auth, original_master_key.clone())?;

        let decoded_master_key = KeyManager::decode_master_key(&auth.password, &keyfile)?;

        assert_eq!(
            original_master_key, decoded_master_key,
            "Decrypted key must match the original key."
        );

        Ok(())
    }

    #[test]
    fn test_key_decoding_failure_with_wrong_password() -> Result<()> {
        let original_master_key = KeyManager::generate_new_master_key();

        let auth = Auth {
            username: "test_user".to_string(),
            password: "correct_password".to_string(),
        };
        let keyfile = KeyManager::generate_key_file(&auth, original_master_key)?;

        let wrong_password = "wrong_password";
        let result = KeyManager::decode_master_key(wrong_password, &keyfile);

        assert!(
            result.is_err(),
            "Decoding must fail with the wrong password."
        );
        let error_message = result.unwrap_err().to_string();
        assert!(
            error_message.contains("Could not retrieve master key from this keyfile"),
            "Error message should indicate decoding failure. Got: {}",
            error_message
        );

        Ok(())
    }

    #[test]
    fn test_keyfile_save_and_load_by_id() -> Result<()> {
        let tmp_dir = tempdir()?;
        let tmp_path = tmp_dir.path();
        let backend = Arc::new(LocalFS::new(tmp_path.to_path_buf()));
        backend.create()?;
        backend.create_dir(Path::new(KEYS_DIR))?;
        let key_manager = KeyManager::new(backend.clone());

        let original_key = KeyManager::generate_new_master_key();
        let auth = Auth {
            username: "loader".to_string(),
            password: "test_password".to_string(),
        };
        let keyfile_to_save = KeyManager::generate_key_file(&auth, original_key.clone())?;

        let id = key_manager.save_keyfile(&keyfile_to_save)?;
        let loaded_keyfile = key_manager.load_keyfile_with_id(&id)?;

        assert!(
            loaded_keyfile.is_some(),
            "Keyfile should be successfully loaded by ID."
        );
        let loaded_keyfile = loaded_keyfile.unwrap();

        let decoded_key = KeyManager::decode_master_key(&auth.password, &loaded_keyfile)?;
        assert_eq!(
            original_key, decoded_key,
            "Loaded and decoded key must match the original."
        );
        assert_eq!(
            loaded_keyfile.username, auth.username,
            "Username must match."
        );

        let fake_id = ID::from_content("non_existent_key".as_bytes());
        let missing_keyfile = key_manager.load_keyfile_with_id(&fake_id)?;
        assert!(
            missing_keyfile.is_none(),
            "Loading a non-existent ID should return None."
        );

        Ok(())
    }

    #[test]
    fn test_retrieve_master_key_from_keys_dir_success() -> Result<()> {
        let tmp_dir = tempdir()?;
        let tmp_path = tmp_dir.path();
        let backend = Arc::new(LocalFS::new(tmp_path.to_path_buf()));
        backend.create()?;
        backend.create_dir(Path::new(KEYS_DIR))?;
        let key_manager = KeyManager::new(backend.clone());
        let username = "key_finder".to_string();
        let password = "retrieve_password".to_string();
        let auth = Auth {
            username: username.clone(),
            password: password.clone(),
        };
        let original_key = KeyManager::generate_new_master_key();

        // Save a dummy key for another user to ensure it's skipped
        let dummy_auth = Auth {
            username: "other_user".to_string(),
            password: "dummy_pass".to_string(),
        };
        let dummy_keyfile =
            KeyManager::generate_key_file(&dummy_auth, KeyManager::generate_new_master_key())?;
        key_manager.save_keyfile(&dummy_keyfile)?;

        // Save the correct key
        let keyfile = KeyManager::generate_key_file(&auth, original_key.clone())?;
        let saved_id = key_manager.save_keyfile(&keyfile)?;

        let (found_id, decoded_key) = key_manager.retrieve_master_key(&auth, None)?;

        assert!(
            found_id.is_some(),
            "An ID should be returned when key is found."
        );
        assert_eq!(
            found_id.unwrap(),
            saved_id,
            "The correct key ID should be returned."
        );
        assert_eq!(
            original_key, decoded_key,
            "Retrieved key must match the original key."
        );

        Ok(())
    }

    #[test]
    fn test_retrieve_master_key_failure_no_match() -> Result<()> {
        let tmp_dir = tempdir()?;
        let tmp_path = tmp_dir.path();
        let backend = Arc::new(LocalFS::new(tmp_path.to_path_buf()));
        backend.create()?;
        backend.create_dir(Path::new(KEYS_DIR))?;
        let key_manager = KeyManager::new(backend.clone());
        // Save a key for a different user
        let other_auth = Auth {
            username: "other_user".to_string(),
            password: "other_pass".to_string(),
        };
        let keyfile_other =
            KeyManager::generate_key_file(&other_auth, KeyManager::generate_new_master_key())?;
        key_manager.save_keyfile(&keyfile_other)?;

        // Attempt retrieval with credentials that don't match any keyfile (wrong user and password)
        let auth = Auth {
            username: "missing_user".to_string(),
            password: "wrong_password".to_string(),
        };
        let result = key_manager.retrieve_master_key(&auth, None);

        assert!(
            result.is_err(),
            "Retrieval must fail when no keyfile matches the username and password."
        );
        let error_message = result.unwrap_err().to_string();
        assert!(
            error_message.contains(
                "No valid KeyFile found for the provided password in the keys directory."
            ),
            "Error message should indicate no matching key was found. Got: {}",
            error_message
        );

        Ok(())
    }

    #[test]
    fn test_delete_keyfile_with_username_and_load_not_found() -> Result<()> {
        let tmp_dir = tempdir()?;
        let tmp_path = tmp_dir.path();
        let backend = Arc::new(LocalFS::new(tmp_path.to_path_buf()));
        backend.create()?;
        backend.create_dir(Path::new(KEYS_DIR))?;
        let key_manager = KeyManager::new(backend.clone());

        let username = "to_be_deleted".to_string();

        let auth_a = Auth {
            username: username.clone(),
            password: "password_a".to_string(),
        };
        let auth_b = Auth {
            username: username.clone(),
            password: "password_b".to_string(),
        };

        // Save two keyfiles for the same user
        let keyfile_a =
            KeyManager::generate_key_file(&auth_a, KeyManager::generate_new_master_key())?;
        let keyfile_b =
            KeyManager::generate_key_file(&auth_b, KeyManager::generate_new_master_key())?;
        key_manager.save_keyfile(&keyfile_a)?;
        key_manager.save_keyfile(&keyfile_b)?;

        // Save a keyfile for another user which should remain
        let other_username = "safe_user";
        let other_auth = Auth {
            username: other_username.to_string(),
            password: "safe_pass".to_string(),
        };
        let keyfile_other =
            KeyManager::generate_key_file(&other_auth, KeyManager::generate_new_master_key())?;
        let other_id = key_manager.save_keyfile(&keyfile_other)?;

        key_manager.delete_keyfile_with_username(&username)?;

        let result = key_manager.load_keyfile_with_username(&username);
        assert!(
            result.is_ok(),
            "load_keyfile_with_username should succeed with 0 matches (Ok(None))."
        );
        assert!(
            result.unwrap().is_none(),
            "All keyfiles for the user should be deleted."
        );

        let remaining_keyfile = key_manager.load_keyfile_with_username(other_username)?;
        assert!(
            remaining_keyfile.is_some(),
            "Other user's keyfile should still exist."
        );
        assert_eq!(
            remaining_keyfile.unwrap().0,
            other_id,
            "The correct key ID should remain."
        );

        Ok(())
    }

    #[test]
    fn test_find_id_with_prefix_success_and_failures() -> Result<()> {
        let tmp_dir = tempdir()?;
        let tmp_path = tmp_dir.path();
        let backend = Arc::new(LocalFS::new(tmp_path.to_path_buf()));
        backend.create()?;
        backend.create_dir(Path::new(KEYS_DIR))?;
        let key_manager = KeyManager::new(backend.clone());

        let original_key = KeyManager::generate_new_master_key();
        let auth = Auth {
            username: "prefix_test".to_string(),
            password: "p".to_string(),
        };
        let keyfile = KeyManager::generate_key_file(&auth, original_key)?;
        let id_a = key_manager.save_keyfile(&keyfile)?;

        let prefix_full = id_a.to_hex();
        let (found_id, _) = key_manager.find_id_with_prefix(&prefix_full)?;
        assert_eq!(found_id, id_a, "Finding with the full ID should succeed.");

        let prefix_short = &prefix_full[..4];
        let (found_id, _) = key_manager.find_id_with_prefix(prefix_short)?;
        assert_eq!(
            found_id, id_a,
            "Finding with a short, unambiguous prefix should succeed."
        );

        assert!(
            key_manager.find_id_with_prefix("").is_err(),
            "Empty prefix must fail."
        );

        let too_long_prefix = "a".repeat(2 * mapache::ID_LENGTH + 1);
        assert!(
            key_manager.find_id_with_prefix(&too_long_prefix).is_err(),
            "Prefix longer than ID length must fail."
        );

        let non_existent_prefix = "ffffffffff";
        assert!(
            key_manager
                .find_id_with_prefix(non_existent_prefix)
                .is_err(),
            "Non-existent prefix must fail."
        );

        // Create a second ID that starts with the same short prefix as the first
        let mut path_b = PathBuf::from(KEYS_DIR).join(prefix_short);
        path_b.push("000000000000000000000000000000000000000000000000000000000000"); // Fake file to create ambiguity

        let ambiguous_filename = format!(
            "{}111111111111111111111111111111111111111111111111111111111111",
            prefix_short
        );
        let ambiguous_path = PathBuf::from(KEYS_DIR).join(&ambiguous_filename);
        backend.write(
            &Handle::new_with_hint(&ambiguous_path, ContentIdType::Key, true),
            b"dummy content",
        )?; // Write arbitrary content

        let result = key_manager.find_id_with_prefix(prefix_short);
        assert!(result.is_err(), "Ambiguous prefix must fail.");
        let error_message = result.unwrap_err().to_string();
        assert!(
            error_message.contains("is ambiguous"),
            "Error message should indicate ambiguity. Got: {}",
            error_message
        );

        Ok(())
    }

    #[test]
    fn test_duplicate_username_keyfiles() -> Result<()> {
        let tmp_dir = tempdir()?;
        let tmp_path = tmp_dir.path();
        let backend = Arc::new(LocalFS::new(tmp_path.to_path_buf()));
        backend.create()?;
        backend.create_dir(Path::new(KEYS_DIR))?;
        let key_manager = KeyManager::new(backend.clone());

        let username = "mapachito".to_string();
        let master_key = KeyManager::generate_new_master_key();

        let auth_a = Auth {
            username: username.clone(),
            password: "password_one".to_string(),
        };
        let keyfile_a = KeyManager::generate_key_file(&auth_a, master_key.clone())?;
        let id_a = key_manager.save_keyfile(&keyfile_a)?; // Save the first keyfile

        let auth_b = Auth {
            username: username.clone(),
            password: "password_two".to_string(),
        };
        let keyfile_b = KeyManager::generate_key_file(&auth_b, master_key)?;
        let id_b = key_manager.save_keyfile(&keyfile_b)?; // Save the second keyfile

        assert_ne!(
            id_a, id_b,
            "The IDs must be different since salts/passwords are different"
        );

        let result = key_manager.load_keyfile_with_username(&username);

        assert!(
            result.is_err(),
            "load_keyfile_with_username should fail when multiple keyfiles exist for one user"
        );

        let error_message = result.unwrap_err().to_string();
        assert!(
            error_message.contains(&format!(
                "More than one Keyfile found for username {username}"
            )),
            "Error message should indicate ambiguity. Got: {}",
            error_message
        );

        key_manager.delete_keyfile_with_id(&id_b)?;
        let result = key_manager.load_keyfile_with_username(&username)?;
        assert!(
            result.is_some(),
            "Loading should succeed after deleting the duplicate."
        );
        assert_eq!(
            result.unwrap().0,
            id_a,
            "The correct ID should be returned."
        );

        Ok(())
    }
}
