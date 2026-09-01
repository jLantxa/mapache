use std::{
    path::{Path, PathBuf},
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use argon2;
use chrono::{DateTime, Local};
use futures::{Stream, StreamExt, future::BoxFuture};
use serde::{Deserialize, Deserializer, Serialize};
use zeroize::Zeroizing;

use crate::{
    backend::{Handle, StorageBackend, WriteContents},
    common::{
        self, ContentIdType, ID,
        defaults::DEFAULT_COMPRESSION,
        error::{MapacheError, Result},
        kdf,
    },
    repository::{
        repo::{Auth, KEYS_DIR},
        storage::SecureStorage,
    },
    ui, utils,
};

/// Supported KDF algorithms and their parameters.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "algorithm")]
pub enum KdfConfig {
    #[serde(rename = "argon2id")]
    Argon2id(Argon2Params),
}

impl std::fmt::Display for KdfConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            KdfConfig::Argon2id(_) => write!(f, "Argon2id"),
        }
    }
}

/// Argon2id KDF parameters.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Argon2Params {
    pub m: u32,
    pub t: u32,
    pub p: u32,
}

/// A metadata structure that contains information about a repository key
#[derive(Debug, Serialize)]
pub struct KeyFile {
    pub created: DateTime<Local>,
    pub username: String,

    /// KDF algorithm and parameters
    pub kdf: KdfConfig,

    pub salt: String,
    pub encrypted_key: String,
}

// TODO(v1-removal): Remove this custom Deserialize impl and use #[derive(Deserialize)].
// The default deserialization handles the v2 format (kdf object) correctly.
impl<'de> Deserialize<'de> for KeyFile {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct RawKeyFile {
            created: DateTime<Local>,
            username: String,
            kdf: Option<KdfConfig>,
            m: Option<u32>,
            t: Option<u32>,
            p: Option<u32>,
            salt: String,
            encrypted_key: String,
        }

        let raw = RawKeyFile::deserialize(deserializer)?;

        // v2: kdf field present
        if let Some(kdf) = raw.kdf {
            return Ok(KeyFile {
                created: raw.created,
                username: raw.username,
                kdf,
                salt: raw.salt,
                encrypted_key: raw.encrypted_key,
            });
        }

        // v1: flat m/t/p fields, assume Argon2id
        let m = raw
            .m
            .ok_or_else(|| serde::de::Error::missing_field("kdf or m"))?;
        let t = raw
            .t
            .ok_or_else(|| serde::de::Error::missing_field("kdf or t"))?;
        let p = raw
            .p
            .ok_or_else(|| serde::de::Error::missing_field("kdf or p"))?;

        Ok(KeyFile {
            created: raw.created,
            username: raw.username,
            kdf: KdfConfig::Argon2id(Argon2Params { m, t, p }),
            salt: raw.salt,
            encrypted_key: raw.encrypted_key,
        })
    }
}

impl KeyFile {
    pub fn argon2_params(&self) -> Result<argon2::Params> {
        match &self.kdf {
            KdfConfig::Argon2id(params) => argon2::ParamsBuilder::new()
                .m_cost(params.m)
                .t_cost(params.t)
                .p_cost(params.p)
                .build()
                .map_err(|e| MapacheError::Crypto(format!("invalid Argon2 parameters: {e}"))),
        }
    }
}

#[derive(Debug)]
pub enum KeyManagerError {
    /// No keyfiles found (repository may not be initialized)
    NoKeyfilesFound,
    /// Keyfiles exist but none match the provided password/username
    NoMatchingKeyfile,
    /// A keyfile is corrupted or invalid
    InvalidKeyfile(String),
    /// Other errors (I/O, decompression, etc.)
    Other(MapacheError),
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
                    "no keyfiles found. This repository may not be properly initialized."
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

/// A KeyFile stream loading files on demand asynchronously.
pub struct KeyFileStream {
    backend: Arc<dyn StorageBackend>,
    entries: Vec<PathBuf>,
    loading_future: Option<BoxFuture<'static, Result<(ID, KeyFile)>>>,
}

impl KeyFileStream {
    pub async fn new(backend: Arc<dyn StorageBackend>) -> Result<Self> {
        let entries = backend
            .list_dir(Path::new(KEYS_DIR))
            .await?
            .into_iter()
            .map(|n| n.into_path())
            .collect();
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
        loop {
            if self.loading_future.is_none()
                && let Some(path) = self.entries.pop()
            {
                let backend = self.backend.clone();

                self.loading_future = Some(Box::pin(async move {
                    if !backend.is_file(&path).await {
                        Err(MapacheError::Repo("not a file".to_string()))?;
                    }

                    let id_str = path
                        .file_name()
                        .ok_or_else(|| MapacheError::Format("no filename".to_string()))?
                        .to_string_lossy();
                    let id = ID::from_hex(&id_str)?;

                    let ss = SecureStorage::new().with_compression(DEFAULT_COMPRESSION.to_level());
                    let handle = Handle::new_with_hint(&path, ContentIdType::Key, true);

                    let keyfile_data = backend.read(&handle, 0, 0).await?;
                    let decompressed = ss.decompress(&keyfile_data)?;
                    let kf: KeyFile = serde_json::from_slice(&decompressed)?;

                    Ok((id, kf))
                }));
            }

            if let Some(mut fut) = self.loading_future.take() {
                match fut.as_mut().poll(cx) {
                    Poll::Ready(Ok(res)) => return Poll::Ready(Some(Ok(res))),
                    Poll::Ready(Err(e)) => {
                        if matches!(e, MapacheError::Serialization(_)) {
                            ui::cli::warning!("Failed to parse keyfile: {}", e);
                            self.loading_future = None;
                            continue;
                        }
                        return Poll::Ready(Some(Err(e)));
                    }
                    Poll::Pending => {
                        self.loading_future = Some(fut);
                        return Poll::Pending;
                    }
                }
            } else {
                return Poll::Ready(None);
            }
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

    pub fn generate_new_master_key() -> Zeroizing<Vec<u8>> {
        Zeroizing::new(rand::random::<[u8; 32]>().to_vec())
    }

    pub fn decode_master_key(password: &str, keyfile: &KeyFile) -> Result<Zeroizing<Vec<u8>>> {
        tracing::debug!(target: "keys", "Decoding master key for user: {}", keyfile.username);
        let salt = utils::base64::decode(&keyfile.salt)?;
        let encrypted_key = utils::base64::decode(&keyfile.encrypted_key)?;

        let intermediate_key =
            SecureStorage::derive_key::<32>(password, &salt, keyfile.argon2_params()?)?;
        let ss = SecureStorage::new()
            .with_compression(DEFAULT_COMPRESSION.to_level())
            .with_key(&*intermediate_key)?;

        // TODO(v1-removal): Try the default nonce position (v2: nonce at end). If that fails,
        // fall back to the other position (v1: nonce at start).
        let primary_err = match ss.decrypt(&encrypted_key) {
            Ok(c) => return Ok(Zeroizing::new(c.into_owned())),
            Err(e) => e,
        };
        ss.set_nonce_at_end(!ss.nonce_at_end());
        ss.decrypt(&encrypted_key)
            .map(|c| Zeroizing::new(c.into_owned()))
            .map_err(|e| {
                MapacheError::Crypto(format!(
                    "could not retrieve master key from this keyfile \
                     (primary: {primary_err}, fallback: {e})"
                ))
            })
    }

    /// Generates a new KeyFile for the master key with a new password.
    ///
    /// `repo_version` controls the nonce position used to encrypt the master key
    /// inside the keyfile: v1 uses nonce-at-start, v2+ uses nonce-at-end. This
    /// ensures old v1 binaries (which only try nonce-at-start) can still open
    /// keyfiles produced by the current code.
    // TODO(v1-removal): Remove repo_version parameter, always use nonce-at-end.
    pub fn generate_key_file(
        auth: &Auth,
        master_key: &[u8],
        repo_version: u32,
        calibrate_kdf: bool,
    ) -> Result<KeyFile> {
        tracing::info!(target: "keys", "Generating new key file for user: {}", auth.username);
        let create_time = Local::now();
        let argon2_params = if calibrate_kdf {
            kdf::calibrate_params(kdf::CALIBRATE_TARGET, None)
        } else {
            kdf::default_params()
        };

        const SALT_LENGTH: usize = 32;
        let salt = SecureStorage::generate_salt::<SALT_LENGTH>();
        let intermediate_key =
            SecureStorage::derive_key::<32>(&auth.password, &salt, argon2_params.clone())?;

        let ss = SecureStorage::new()
            .with_compression(DEFAULT_COMPRESSION.to_level())
            .with_key(&*intermediate_key)?;
        // TODO(v1-removal): v1 binaries only try nonce-at-start; use that position for v1 repos
        // so they remain backward-compatible.
        ss.set_nonce_at_end(repo_version >= 2);

        let encrypted_key = ss.encrypt(master_key)?;

        let m = argon2_params.m_cost();
        let t = argon2_params.t_cost();
        let p = argon2_params.p_cost();

        Ok(KeyFile {
            created: create_time,
            username: auth.username.clone(),
            kdf: KdfConfig::Argon2id(Argon2Params { m, t, p }),
            encrypted_key: utils::base64::encode(&encrypted_key),
            salt: utils::base64::encode(&salt),
        })
    }

    /// Retrieve the master key from all available keys in a folder
    pub async fn retrieve_master_key(
        &self,
        auth: &Auth,
        keyfile_path: Option<&PathBuf>,
    ) -> Result<(Option<ID>, Zeroizing<Vec<u8>>)> {
        tracing::debug!(target: "keys", "Retrieving master key (path={:?})", keyfile_path);
        self.retrieve_master_key_internal(auth, keyfile_path)
            .await
            .map_err(|e| match e {
                KeyManagerError::NoMatchingKeyfile => {
                    MapacheError::Auth("Incorrect username or password".to_string())
                }
                KeyManagerError::Other(inner) => inner,
                other => MapacheError::Repo(format!("{other}")),
            })
    }

    async fn retrieve_master_key_internal(
        &self,
        auth: &Auth,
        keyfile_path: Option<&PathBuf>,
    ) -> std::result::Result<(Option<ID>, Zeroizing<Vec<u8>>), KeyManagerError> {
        match keyfile_path {
            Some(path) => {
                tracing::debug!(target: "keys", "Reading key file from local path: {:?}", path);

                let keyfile_data =
                    std::fs::read(path).map_err(|e| KeyManagerError::Other(MapacheError::Io(e)))?;

                let ss = SecureStorage::new();
                let keyfile_bytes = ss
                    .decompress(&keyfile_data)
                    .map_err(KeyManagerError::Other)?;
                let keyfile: KeyFile = serde_json::from_slice(&keyfile_bytes).map_err(|e| {
                    KeyManagerError::InvalidKeyfile(format!("keyFile at {path:?} is invalid: {e}"))
                })?;

                if keyfile.username != auth.username {
                    return Err(KeyManagerError::NoMatchingKeyfile);
                }

                Self::decode_master_key(&auth.password, &keyfile)
                    .map(|key| (None, key))
                    .map_err(KeyManagerError::Other)
            }
            None => {
                tracing::debug!(target: "keys", "Searching for matching key file in repository");
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
                        tracing::info!(target: "keys", "Master key retrieved using key file {}", id.to_short_hex(8));
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
            1 => Ok(Some(matches.swap_remove(0))),
            _ => Err(MapacheError::Integrity(format!(
                "more than one Keyfile found for username {username}"
            )))?,
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

    /// Load a keyfile as raw compressed bytes (without decompressing)
    pub async fn load_raw_keyfile(&self, id: &ID) -> Result<Vec<u8>> {
        let path = PathBuf::from(KEYS_DIR).join(id.to_hex());
        tracing::debug!(target: "keys", "Loading raw key file {}", id.to_short_hex(8));
        let handle = Handle::new_with_hint(&path, ContentIdType::Key, true);
        let data = self.backend.read(&handle, 0, 0).await?;
        Ok(data)
    }

    /// Save a KeyFile
    pub async fn save_keyfile(&self, keyfile: &KeyFile) -> Result<ID> {
        let ss = SecureStorage::new().with_compression(DEFAULT_COMPRESSION.to_level());
        let keyfile_json = serde_json::to_string(keyfile)?;
        let compressed_json = ss.compress(keyfile_json.as_bytes())?;
        let id = ID::from_content(&compressed_json);
        let path = PathBuf::from(KEYS_DIR).join(id.to_hex());

        tracing::info!(target: "keys", "Saving new key file {} for user {}", id.to_short_hex(8), keyfile.username);
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
        if prefix.len() > 2 * common::ID_LENGTH {
            // A hex string has 2 characters per byte.
            return Err(MapacheError::Format(format!(
                "invalid prefix length. The prefix must not be longer than the ID ({} chars)",
                2 * common::ID_LENGTH
            )));
        } else if prefix.is_empty() {
            Err(MapacheError::Format("prefix cannot be empty".to_string()))?;
        }

        let entries = self.backend.list_dir(Path::new(KEYS_DIR)).await?;
        let mut matches = Vec::new();

        for node in entries {
            let file_path = node.into_path();
            let filename = file_path
                .file_name()
                .ok_or_else(|| MapacheError::Format("file should have a name".to_string()))?
                .to_string_lossy();
            if filename.starts_with(prefix) {
                matches.push((filename.into_owned(), file_path));
            }
        }

        if matches.is_empty() {
            return Err(MapacheError::NotFound(format!(
                "keyfile with prefix {prefix} doesn't exist"
            )));
        }
        if matches.len() > 1 {
            return Err(MapacheError::Format(format!(
                "prefix {prefix} is ambiguous"
            )));
        }

        let (filename, filepath) = matches
            .pop()
            .ok_or_else(|| MapacheError::Integrity("keyfile match set is empty".to_string()))?;
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
            password: Zeroizing::new("test_password".to_string()),
        };
        let master_key = KeyManager::generate_new_master_key();

        let key_file = KeyManager::generate_key_file(&auth, &master_key, 2, false)?;
        assert_eq!(key_file.username, "test_user");

        let decoded_master_key = KeyManager::decode_master_key(&auth.password, &key_file)?;
        assert_eq!(*master_key, *decoded_master_key);

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
            kdf: KdfConfig::Argon2id(Argon2Params {
                m: 1024,
                t: 1,
                p: 1,
            }),
            salt: "".to_string(),
            encrypted_key: "".to_string(),
        };

        let params = kf.argon2_params().unwrap();
        assert_eq!(params.m_cost(), 1024);
        assert_eq!(params.t_cost(), 1);
        assert_eq!(params.p_cost(), 1);
    }
}
