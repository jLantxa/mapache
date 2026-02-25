use std::{
    path::{Path, PathBuf},
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use anyhow::{Context as _, Result, bail};
use argon2;
use base64::Engine;
use chrono::{DateTime, Local};
use futures::{Stream, StreamExt, future::BoxFuture};
use serde::{Deserialize, Serialize};

use crate::{
    backend::{Handle, StorageBackend, WriteContents},
    mapache::{self, ContentIdType, ID, defaults::DEFAULT_COMPRESSION},
    repository::{
        repo::{Auth, KEYS_DIR},
        storage::SecureStorage,
    },
    ui,
};

/// Error types for KeyManager operations
#[derive(Debug)]
pub enum KeyManagerError {
    /// No keyfiles found (repository may not be initialized)
    NoKeyfilesFound,
    /// Keyfiles exist but none match the provided password/username
    NoMatchingKeyfile,
    /// A keyfile is corrupted or invalid
    InvalidKeyfile(String),
    /// Other errors (I/O, decompression, etc.)
    Other(anyhow::Error),
}

impl KeyManagerError {
    /// Check if this is a wrong password error (retryable)
    pub fn is_wrong_password(&self) -> bool {
        matches!(self, KeyManagerError::NoMatchingKeyfile)
    }

    /// Check if this is a fatal error (not retryable)
    pub fn is_fatal(&self) -> bool {
        matches!(
            self,
            KeyManagerError::NoKeyfilesFound | KeyManagerError::InvalidKeyfile(_)
        )
    }
}

impl std::fmt::Display for KeyManagerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            KeyManagerError::NoKeyfilesFound => {
                write!(
                    f,
                    "No keyfiles found. This repository may not be properly initialized."
                )
            }
            KeyManagerError::NoMatchingKeyfile => {
                write!(
                    f,
                    "No valid KeyFile found for the provided password in the keys directory."
                )
            }
            KeyManagerError::InvalidKeyfile(msg) => {
                write!(f, "Invalid keyfile: {}", msg)
            }
            KeyManagerError::Other(err) => write!(f, "{}", err),
        }
    }
}

impl std::error::Error for KeyManagerError {}

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

    /// Argon2 memory size
    #[serde(default = "argon2_defaults::default_m")]
    pub m: u32,

    /// Argon2 iterations
    #[serde(default = "argon2_defaults::default_t")]
    pub t: u32,

    /// Argon2 parallelism
    #[serde(default = "argon2_defaults::default_p")]
    pub p: u32,

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

/// A KeyFile stream loading files on demand asynchronously.
pub struct KeyFileStream {
    backend: Arc<dyn StorageBackend>,
    entries: Vec<PathBuf>,
    loading_future: Option<BoxFuture<'static, Result<(ID, KeyFile)>>>,
}

impl KeyFileStream {
    pub async fn new(backend: Arc<dyn StorageBackend>) -> Result<Self> {
        let entries = backend.list_dir(Path::new(KEYS_DIR)).await?;
        Ok(Self {
            backend,
            entries,
            loading_future: None,
        })
    }
}

impl Stream for KeyFileStream {
    type Item = Result<(ID, KeyFile)>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        // Start a new future if none is currently active
        if self.loading_future.is_none()
            && let Some(path) = self.entries.pop()
        {
            let backend = self.backend.clone();

            self.loading_future = Some(Box::pin(async move {
                if !backend.is_file(&path).await {
                    bail!("Not a file");
                }

                let id_str = path.file_name().context("No filename")?.to_string_lossy();
                let id = ID::from_hex(&id_str)?;

                let ss = SecureStorage::new().with_compression(DEFAULT_COMPRESSION.to_level());
                let handle = Handle::new_with_hint(&path, ContentIdType::Key, true);

                let keyfile_data = backend.read(&handle, 0, 0).await?;
                let decompressed = ss.decompress(&keyfile_data)?;
                let kf: KeyFile = serde_json::from_slice(&decompressed)?;

                Ok((id, kf))
            }));
        }

        // Poll the existing future
        if let Some(mut fut) = self.loading_future.take() {
            match fut.as_mut().poll(cx) {
                Poll::Ready(Ok(res)) => Poll::Ready(Some(Ok(res))),
                Poll::Ready(Err(e)) => {
                    // Log error for parse failures like the original, but allow stream to continue
                    if e.to_string().contains("serde_json") {
                        ui::cli::warning!("Failed to parse keyfile: {}", e);
                        self.loading_future = None;
                        return self.poll_next(cx);
                    }
                    Poll::Ready(Some(Err(e)))
                }
                Poll::Pending => {
                    self.loading_future = Some(fut);
                    Poll::Pending
                }
            }
        } else {
            Poll::Ready(None)
        }
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
        rand::random::<[u8; 32]>().to_vec()
    }

    pub fn decode_master_key(password: &str, keyfile: &KeyFile) -> Result<Vec<u8>> {
        let salt = base64::engine::general_purpose::STANDARD.decode(&keyfile.salt)?;
        let encrypted_key =
            base64::engine::general_purpose::STANDARD.decode(&keyfile.encrypted_key)?;

        let intermediate_key =
            SecureStorage::derive_key::<32>(password, &salt, keyfile.argon2_params())?;
        let ss = SecureStorage::new()
            .with_compression(DEFAULT_COMPRESSION.to_level())
            .with_key(&intermediate_key);

        ss.decrypt(&encrypted_key)
            .map(|c| c.into_owned())
            .context("Could not retrieve master key from this keyfile")
    }

    /// Generates a new KeyFile for the master key with a new password
    pub fn generate_key_file(auth: &Auth, master_key: Vec<u8>) -> Result<KeyFile> {
        let create_time = Local::now();
        let argon2_params = argon2::Params::default();

        const SALT_LENGTH: usize = 32;
        let salt = SecureStorage::generate_salt::<SALT_LENGTH>();
        let intermediate_key =
            SecureStorage::derive_key::<32>(&auth.password, &salt, argon2_params.clone())?;

        let ss = SecureStorage::new()
            .with_compression(DEFAULT_COMPRESSION.to_level())
            .with_key(&intermediate_key);

        let encrypted_key = ss.encrypt(&master_key)?;

        Ok(KeyFile {
            created: create_time,
            username: auth.username.clone(),
            m: argon2_params.m_cost(),
            t: argon2_params.t_cost(),
            p: argon2_params.p_cost(),
            encrypted_key: base64::engine::general_purpose::STANDARD.encode(encrypted_key),
            salt: base64::engine::general_purpose::STANDARD.encode(salt),
        })
    }

    /// Retrieve the master key from all available keys in a folder
    pub async fn retrieve_master_key(
        &self,
        auth: &Auth,
        keyfile_path: Option<&PathBuf>,
    ) -> Result<(Option<ID>, Vec<u8>)> {
        self.retrieve_master_key_internal(auth, keyfile_path)
            .await
            .map_err(anyhow::Error::new)
    }

    async fn retrieve_master_key_internal(
        &self,
        auth: &Auth,
        keyfile_path: Option<&PathBuf>,
    ) -> std::result::Result<(Option<ID>, Vec<u8>), KeyManagerError> {
        match keyfile_path {
            Some(path) => {
                let ss = SecureStorage::new();
                let handle = Handle::new_with_hint(path, ContentIdType::Key, true);
                let keyfile_data = self
                    .backend
                    .read(&handle, 0, 0)
                    .await
                    .map_err(KeyManagerError::Other)?;
                let keyfile_bytes = ss
                    .decompress(&keyfile_data)
                    .map_err(KeyManagerError::Other)?;
                let keyfile: KeyFile = serde_json::from_slice(&keyfile_bytes).map_err(|e| {
                    KeyManagerError::InvalidKeyfile(format!("KeyFile at {path:?} is invalid: {e}"))
                })?;

                Self::decode_master_key(&auth.password, &keyfile)
                    .map(|key| (None, key))
                    .map_err(KeyManagerError::Other)
            }
            None => {
                let mut keyfile_stream = KeyFileStream::new(self.backend.clone())
                    .await
                    .map_err(|_| KeyManagerError::NoKeyfilesFound)?;

                let mut found_any_keyfiles = false;
                while let Some(res) = keyfile_stream.next().await {
                    let (id, keyfile) = match res {
                        Ok(val) => val,
                        Err(_) => continue,
                    };
                    found_any_keyfiles = true;
                    if keyfile.username == auth.username
                        && let Ok(master_key) = Self::decode_master_key(&auth.password, &keyfile)
                    {
                        return Ok((Some(id), master_key));
                    }
                }

                if !found_any_keyfiles {
                    Err(KeyManagerError::NoKeyfilesFound)
                } else {
                    Err(KeyManagerError::NoMatchingKeyfile)
                }
            }
        }
    }

    /// Check if any keyfiles exist in the repository
    pub async fn keyfiles_exist(&self) -> Result<()> {
        let mut stream = KeyFileStream::new(self.backend.clone()).await?;
        if stream.next().await.is_some() {
            Ok(())
        } else {
            bail!("No keyfiles found. This repository may not be properly initialized.")
        }
    }

    /// Load a keyfile with a given ID
    pub async fn load_keyfile_with_id(&self, id: &ID) -> Result<Option<KeyFile>> {
        let path = PathBuf::from(KEYS_DIR).join(id.to_hex());
        if !self.backend.path_exists(&path).await {
            return Ok(None);
        }

        let ss = SecureStorage::new();
        let handle = Handle::new_with_hint(&path, ContentIdType::Key, true);
        let keyfile_data = self.backend.read(&handle, 0, 0).await?;
        let keyfile_bytes = ss.decompress(&keyfile_data)?;
        let keyfile: KeyFile = serde_json::from_slice(&keyfile_bytes)?;

        Ok(Some(keyfile))
    }

    /// Load a keyfile with a given username
    pub async fn load_keyfile_with_username(
        &self,
        username: &str,
    ) -> Result<Option<(ID, KeyFile)>> {
        let stream = KeyFileStream::new(self.backend.clone()).await?;

        // Filter stream for matching username
        let mut matches = stream
            .filter_map(|res| async move {
                match res {
                    Ok((id, kf)) if kf.username == username => Some((id, kf)),
                    _ => None,
                }
            })
            .collect::<Vec<_>>()
            .await;

        match matches.len() {
            0 => Ok(None),
            1 => Ok(Some(matches.pop().unwrap())),
            _ => bail!("More than one Keyfile found for username {username}"),
        }
    }

    /// Delete a keyfile with a given ID
    pub async fn delete_keyfile_with_id(&self, id: &ID) -> Result<()> {
        let path = PathBuf::from(KEYS_DIR).join(id.to_hex());
        self.backend.remove(&path).await
    }

    /// Delete keyfiles with a given username
    pub async fn delete_keyfile_with_username(&self, username: &str) -> Result<()> {
        let stream = KeyFileStream::new(self.backend.clone()).await?;

        // Collect IDs of keyfiles belonging to the username
        let ids_to_delete: Vec<ID> = stream
            .filter_map(|res| async move {
                match res {
                    Ok((id, kf)) if kf.username == username => Some(id),
                    _ => None,
                }
            })
            .collect()
            .await;

        for id in ids_to_delete {
            self.delete_keyfile_with_id(&id).await?;
        }
        Ok(())
    }

    /// Save a KeyFile
    pub async fn save_keyfile(&self, keyfile: &KeyFile) -> Result<ID> {
        let ss = SecureStorage::new().with_compression(DEFAULT_COMPRESSION.to_level());
        let keyfile_json = serde_json::to_string(keyfile)?;
        let compressed_json = ss.compress(keyfile_json.as_bytes())?;
        let id = ID::from_content(&compressed_json);
        let path = PathBuf::from(KEYS_DIR).join(id.to_hex());

        self.backend
            .write(
                &Handle::new_with_hint(&path, ContentIdType::Key, true),
                WriteContents::Owned(compressed_json),
            )
            .await?;

        Ok(id)
    }

    /// Find a KeyFile ID using a prefix
    pub async fn find_id_with_prefix(&self, prefix: &str) -> Result<(ID, PathBuf)> {
        if prefix.len() > 2 * mapache::ID_LENGTH {
            // A hex string has 2 characters per byte.
            bail!(
                "Invalid prefix length. The prefix must not be longer than the ID ({} chars)",
                2 * mapache::ID_LENGTH
            );
        } else if prefix.is_empty() {
            bail!("Prefix cannot be empty");
        }

        let files = self.backend.list_dir(Path::new(KEYS_DIR)).await?;
        let mut matches = Vec::new();

        for file_path in files {
            let filename = file_path
                .file_name()
                .context("File should have a name")?
                .to_string_lossy();
            if filename.starts_with(prefix) {
                matches.push((filename.into_owned(), file_path));
            }
        }

        if matches.is_empty() {
            bail!("Keyfile with prefix {prefix} doesn't exist");
        }
        if matches.len() > 1 {
            bail!("Prefix {prefix} is ambiguous");
        }

        let (filename, filepath) = matches.pop().unwrap();
        let id = ID::from_hex(&filename)?;
        Ok((id, filepath))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_master_key_generation() {
        let k1 = KeyManager::generate_new_master_key();
        let k2 = KeyManager::generate_new_master_key();
        assert_eq!(k1.len(), 32);
        assert_ne!(k1, k2);
    }

    #[test]
    fn test_key_file_roundtrip() -> Result<()> {
        let auth = Auth {
            username: "test_user".to_string(),
            password: "test_password".to_string(),
        };
        let master_key = KeyManager::generate_new_master_key();

        let key_file = KeyManager::generate_key_file(&auth, master_key.clone())?;
        assert_eq!(key_file.username, "test_user");

        let decoded_master_key = KeyManager::decode_master_key(&auth.password, &key_file)?;
        assert_eq!(master_key, decoded_master_key);

        // Test with wrong password
        let wrong_password_res = KeyManager::decode_master_key("wrong_password", &key_file);
        assert!(wrong_password_res.is_err());

        Ok(())
    }

    #[test]
    fn test_argon2_params() {
        let kf = KeyFile {
            created: Local::now(),
            username: "test".to_string(),
            m: 1024,
            t: 1,
            p: 1,
            salt: "".to_string(),
            encrypted_key: "".to_string(),
        };

        let params = kf.argon2_params();
        assert_eq!(params.m_cost(), 1024);
        assert_eq!(params.t_cost(), 1);
        assert_eq!(params.p_cost(), 1);
    }
}
