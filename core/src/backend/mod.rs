pub mod cache;
pub mod dry;
pub mod localfs;
pub mod s3;
pub mod sftp;

use std::{
    path::{Path, PathBuf},
    str::FromStr,
    sync::Arc,
    time::SystemTime,
};

use crate::{backend::sftp::SftpBackend, mapache::ContentIdType};
use anyhow::{Result, anyhow, bail};
use async_trait::async_trait;
use dry::DryBackend;
use localfs::LocalFS;
use s3::S3Backend;

use crate::{ui, utils::url::Url};

/// Configuration for retry logic.
pub struct RetryOptions {
    pub max_attempts: u32,
    pub base_delay: std::time::Duration,
    pub request_timeout: std::time::Duration,
}

/// A generic retry wrapper with exponential backoff and per-request timeouts.
pub async fn retry<T, F, Fut>(name: &str, opts: &RetryOptions, mut op: F) -> Result<T>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<T>>,
{
    let mut attempts = 0;
    loop {
        let attempt_res = tokio::time::timeout(opts.request_timeout, op()).await;

        match attempt_res {
            Ok(Ok(val)) => return Ok(val),
            Ok(Err(e)) if attempts < opts.max_attempts => {
                attempts += 1;
                let wait = opts.base_delay * (2_u32.pow(attempts - 1));
                ui::cli::warning!(
                    "{} operation failed: {}. Retrying in {}ms...",
                    name,
                    e,
                    wait.as_millis()
                );
                tokio::time::sleep(wait).await;
            }
            Err(_) if attempts < opts.max_attempts => {
                attempts += 1;
                let wait = opts.base_delay * (2_u32.pow(attempts - 1));
                ui::cli::warning!(
                    "{} operation timed out. Retrying in {}ms...",
                    name,
                    wait.as_millis()
                );
                tokio::time::sleep(wait).await;
            }
            Ok(Err(e)) => {
                return Err(e.context(format!("{} operation failed after multiple retries", name)));
            }
            Err(_) => bail!("{} operation timed out after multiple retries", name),
        }
    }
}

/// Represents the attributes (metadata) of a file or directory node.
/// This information is typically retrieved via an `lstat` call on the backend.
#[derive(Debug)]
pub struct NodeAttr {
    pub size: Option<u64>,
    pub uid: Option<u32>,
    pub gid: Option<u32>,
    pub perm: Option<u32>,
    pub atime: Option<SystemTime>,
    pub mtime: Option<SystemTime>,
}

/// A handle used for file operations (read/write) on a `StorageBackend`.
///
/// It combines the path of the resource with an optional hint that provides
/// extra context about the data being stored, allowing the backend to apply
/// optimization or specific storage logic.
#[derive(Debug)]
pub struct Handle<'a> {
    pub path: &'a Path,
    pub hint: Option<StorageHint>,
}

impl<'a> Handle<'a> {
    /// Creates a new Handle without a StorageHint.
    pub fn new(path: &'a Path) -> Self {
        Self { path, hint: None }
    }

    /// Creates a new Handle with a StorageHint, indicating the nature of the data.
    pub fn new_with_hint(path: &'a Path, file_type: ContentIdType, is_metadata: bool) -> Self {
        Self {
            path,
            hint: Some(StorageHint {
                file_type,
                is_metadata,
            }),
        }
    }
}

/// Provides additional context to the `StorageBackend` about the data
/// associated with a `Handle` during read or write operations.
///
/// This hint is typically used to optimize the storage strategy for different
/// types of backup data (pure data vs. metadata files).
#[derive(Debug)]
pub struct StorageHint {
    /// The type of content ID (e.g., data chunk, index, or configuration file).
    pub file_type: ContentIdType,

    /// Indicates if the content being accessed is backup metadata (e.g., index files)
    /// rather than raw backed-up data. This can influence caching or redundancy decisions.
    pub is_metadata: bool,
}

/// Alias for the data buffer passed to the backend during write operations.
/// Using Cow allows backends to take ownership of the data without cloning.
pub type WriteContents<'a> = std::borrow::Cow<'a, [u8]>;

/// Abstraction of a storage backend.
///
/// A backend is a filesystem interface that can represent local storage,
/// remote servers (SFTP), or cloud buckets (S3).
#[async_trait]
pub trait StorageBackend: Send + Sync {
    /// Initializes the repository structure on the backend.
    async fn create(&self) -> Result<()>;

    /// Returns true if the given path exists on the backend.
    async fn path_exists(&self, path: &Path) -> bool;

    /// Reads data from a file.
    ///
    /// * If `length` is 0, reads until EOF.
    /// * If `offset` is negative, reads relative to the end of the file.
    async fn read(&self, handle: &Handle, offset: isize, length: usize) -> Result<Vec<u8>>;

    /// Writes the provided buffer to a file, creating it if it doesn't exist.
    async fn write(&self, handle: &Handle, contents: WriteContents<'_>) -> Result<()>;

    /// Renames (moves) a file. Atomicity depends on the specific implementation.
    async fn rename(&self, from: &Path, to: &Path) -> Result<()>;

    /// Lists the immediate contents of a directory.
    async fn list_dir(&self, path: &Path) -> Result<Vec<PathBuf>>;

    /// Recursively creates a directory path.
    async fn create_dir(&self, path: &Path) -> Result<()>;

    /// Deletes a file or directory (recursively).
    async fn remove(&self, file_path: &Path) -> Result<()>;

    /// Returns true if the path points to a file.
    async fn is_file(&self, path: &Path) -> bool;

    /// Returns true if the path points to a directory.
    async fn is_dir(&self, path: &Path) -> bool;

    /// Returns metadata for the path without following symbolic links.
    async fn lstat(&self, path: &Path) -> Result<NodeAttr>;
}

/// Configuration used to initialize a backend.
pub struct BackendOptions {
    pub repo_path: String,
    pub ssh_privatekey: Option<PathBuf>,
    /// If true, wraps the chosen backend in a `DryBackend` to prevent actual writes.
    pub dry_backend: bool,
}

/// Factory function to initialize a backend based on a URL string.
///
/// This will trigger interactive CLI prompts if environment variables
/// for credentials (like S3 keys or SFTP passwords) are missing.
pub async fn new_backend_with_prompt(opts: BackendOptions) -> Result<Arc<dyn StorageBackend>> {
    let backend_url = BackendUrl::from(&opts.repo_path)?;

    let backend: Arc<dyn StorageBackend> = match backend_url {
        BackendUrl::Local(repo_path) => Arc::new(LocalFS::new(repo_path)),
        BackendUrl::Sftp(username, host, port, repo_path) => {
            const MAX_PASSWORD_RETRIES: usize = 3;
            let mut password_try_count = 0;
            loop {
                let auth_method = match &opts.ssh_privatekey {
                    Some(pk) => sftp::AuthMethod::PubKey {
                        private_key: pk.clone(),
                        passphrase: None,
                    },
                    None => {
                        let prompt = format!("{username}@{host}'s password");
                        sftp::AuthMethod::Password(ui::cli::request_password(&prompt))
                    }
                };

                match SftpBackend::new(
                    repo_path.clone(),
                    username.clone(),
                    host.clone(),
                    port,
                    auth_method,
                )
                .await
                {
                    Ok(backend) => break Arc::new(backend),
                    Err(e)
                        if opts.ssh_privatekey.is_none()
                            && password_try_count < MAX_PASSWORD_RETRIES - 1
                            && e.chain().any(|err| err.is::<sftp::SftpError>()) =>
                    {
                        password_try_count += 1;
                        ui::cli::log!("Incorrect password. Try again.");
                    }
                    Err(e) => return Err(e),
                }
            }
        }
        BackendUrl::S3(bucket, prefix) => {
            let endpoint = std::env::var("AWS_ENDPOINT_URL").unwrap_or_else(|_| {
                ui::cli::request_input("S3 Endpoint (leave empty for AWS)")
                    .unwrap_or("amazonaws.com".to_string())
            });

            let region = std::env::var("AWS_DEFAULT_REGION").unwrap_or_else(|_| {
                ui::cli::request_input("S3 Region").unwrap_or("us-east-1".to_string())
            });

            let access_key = std::env::var("AWS_ACCESS_KEY_ID").unwrap_or_else(|_| {
                ui::cli::request_input("AWS Access Key ID").unwrap_or_default()
            });

            let secret_key = std::env::var("AWS_SECRET_ACCESS_KEY")
                .unwrap_or_else(|_| ui::cli::request_password("AWS Secret Access Key"));

            Arc::new(S3Backend::new(
                region, bucket, prefix, endpoint, access_key, secret_key,
            )?)
        }
    };

    let backend = match opts.dry_backend {
        true => Arc::new(DryBackend::new(backend.clone())),
        false => backend,
    };

    Ok(backend)
}

/// Identifies the protocol and connection parameters for a backend.
#[derive(Debug, Clone, PartialEq)]
pub enum BackendUrl {
    Local(PathBuf),
    Sftp(String, String, u16, PathBuf), // (user, host, port, path)
    S3(String, PathBuf),                // (bucket, prefix)
}

impl BackendUrl {
    /// Parses a raw string (e.g., "s3://my-bucket/prefix") into a `BackendUrl`.
    pub fn from(url_str: &str) -> Result<Self> {
        if !url_str.contains("://") {
            return Ok(BackendUrl::Local(PathBuf::from(url_str)));
        }

        let parsed_url = Url::from_str(url_str)?;

        match parsed_url.scheme.as_ref() {
            "file" => {
                let path_str: &str = &parsed_url.path.join("/");
                let path_buf = PathBuf::from(path_str);
                Ok(BackendUrl::Local(path_buf))
            }
            "sftp" => {
                let user = parsed_url.username.to_string();

                let host = parsed_url
                    .host
                    .ok_or_else(|| anyhow!("SFTP URL '{url_str}' requires a host"))?
                    .to_string();

                let port = parsed_url.port.unwrap_or(22);

                let path_str: &str = &parsed_url.path.join("/");
                let path_buf = PathBuf::from(path_str);

                Ok(BackendUrl::Sftp(user, host, port, path_buf))
            }
            "s3" => {
                let bucket = parsed_url
                    .host
                    .ok_or_else(|| anyhow!("S3 URL '{url_str}' requires a bucket name (host)"))?
                    .to_string();

                let path_str: &str = &parsed_url.path.join("/");

                // Strip leading slash if present to make it a clean prefix
                let path_str = path_str.strip_prefix('/').unwrap_or(path_str);
                let path_buf = PathBuf::from(path_str);

                Ok(BackendUrl::S3(bucket, path_buf))
            }
            _ => {
                bail!(
                    "Unsupported URL scheme: '{}' for URL '{}'",
                    parsed_url.scheme.as_str(),
                    url_str
                );
            }
        }
    }
}

/// Represents a generic filesystem node within a backend.
#[derive(Debug, Clone, PartialEq)]
pub enum BackendNode {
    File(PathBuf),
    Dir(PathBuf),
}

impl BackendNode {
    /// Returns the inner path regardless of node type.
    pub fn path(&self) -> &Path {
        match self {
            BackendNode::File(path) => path,
            BackendNode::Dir(path) => path,
        }
    }
}

/// Helper function to perform a recursive crawl of a backend directory.
pub async fn read_backend_dir(
    backend: &dyn StorageBackend,
    path: &Path,
) -> Result<Vec<BackendNode>> {
    let mut nodes = Vec::new();
    let mut stack: Vec<PathBuf> = vec![path.to_path_buf()];

    while let Some(current) = stack.pop() {
        let entries = backend.list_dir(&current).await?;

        for sub_path in entries {
            if backend.is_file(&sub_path).await {
                nodes.push(BackendNode::File(sub_path.to_path_buf()));
            } else if backend.is_dir(&sub_path).await {
                nodes.push(BackendNode::Dir(sub_path.to_path_buf()));
                stack.push(sub_path.to_path_buf());
            }
        }
    }

    Ok(nodes)
}

/// Modifies a Unix mode bitmask to toggle readonly status while
/// maintaining necessary traverse/list bits for directories.
pub fn set_readonly_mode(mode: u32, readonly: bool, is_dir: bool) -> u32 {
    let base = mode & !0o777; // Preserve special bits (setuid, etc.)
    if is_dir {
        // Directories need Execute bits to be listable/traversable
        if readonly {
            base | 0o500 // r-x------
        } else {
            base | 0o700 // rwx------
        }
    } else {
        // Standard files
        if readonly {
            base | 0o400 // r---------
        } else {
            base | 0o600 // rw-------
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_local_path() {
        assert_eq!(
            BackendUrl::from("/home/target").unwrap(),
            BackendUrl::Local(PathBuf::from("/home/target"))
        );
        assert_eq!(
            BackendUrl::from("base/dir").unwrap(),
            BackendUrl::Local(PathBuf::from("base/dir"))
        );
        assert_eq!(
            BackendUrl::from("dir").unwrap(),
            BackendUrl::Local(PathBuf::from("dir"))
        );
        assert_eq!(
            BackendUrl::from("dir/").unwrap(),
            BackendUrl::Local(PathBuf::from("dir/"))
        );
        assert_eq!(
            BackendUrl::from("./dir").unwrap(),
            BackendUrl::Local(PathBuf::from("./dir"))
        );
        assert_eq!(
            BackendUrl::from("./dir/").unwrap(),
            BackendUrl::Local(PathBuf::from("./dir/"))
        );
        assert_eq!(
            BackendUrl::from(".").unwrap(),
            BackendUrl::Local(PathBuf::from("."))
        );
    }

    #[test]
    fn test_local_path_with_file_scheme() {
        assert_eq!(
            BackendUrl::from("file:///home/target").unwrap(),
            BackendUrl::Local(PathBuf::from("/home/target"))
        );
        assert_eq!(
            BackendUrl::from("file://base/dir").unwrap(),
            BackendUrl::Local(PathBuf::from("base/dir"))
        );
        assert_eq!(
            BackendUrl::from("file://dir").unwrap(),
            BackendUrl::Local(PathBuf::from("dir"))
        );
        assert_eq!(
            BackendUrl::from("file://dir/").unwrap(),
            BackendUrl::Local(PathBuf::from("dir/"))
        );
        assert_eq!(
            BackendUrl::from("file://./dir").unwrap(),
            BackendUrl::Local(PathBuf::from("dir"))
        );
        assert_eq!(
            BackendUrl::from("file://./dir/a/..").unwrap(),
            BackendUrl::Local(PathBuf::from("dir"))
        );
        assert_eq!(
            BackendUrl::from("file://./dir/").unwrap(),
            BackendUrl::Local(PathBuf::from("dir"))
        );
        assert_eq!(
            BackendUrl::from("file://.").unwrap(),
            BackendUrl::Local(PathBuf::from(""))
        );
    }

    #[test]
    fn test_sftp_path() -> Result<()> {
        let user = String::from("user");
        let host = String::from("host");

        assert_eq!(
            BackendUrl::from("sftp://user@host:22//home/target")?,
            BackendUrl::Sftp(
                user.clone(),
                host.clone(),
                22,
                PathBuf::from("/home/target")
            )
        );
        assert_eq!(
            BackendUrl::from("sftp://user@host:22/base/dir")?,
            BackendUrl::Sftp(user.clone(), host.clone(), 22, PathBuf::from("base/dir"))
        );
        assert_eq!(
            BackendUrl::from("sftp://user@host:22/dir")?,
            BackendUrl::Sftp(user.clone(), host.clone(), 22, PathBuf::from("dir"))
        );
        assert_eq!(
            BackendUrl::from("sftp://user@host:22/dir/")?,
            BackendUrl::Sftp(user.clone(), host.clone(), 22, PathBuf::from("dir/"))
        );
        assert_eq!(
            BackendUrl::from("sftp://user@host:22/./dir")?,
            BackendUrl::Sftp(user.clone(), host.clone(), 22, PathBuf::from("dir"))
        );
        assert_eq!(
            BackendUrl::from("sftp://user@host:22/./dir/")?,
            BackendUrl::Sftp(user.clone(), host.clone(), 22, PathBuf::from("dir"))
        );
        assert_eq!(
            BackendUrl::from("sftp://user@host:22/")?,
            BackendUrl::Sftp(user.clone(), host.clone(), 22, PathBuf::from(""))
        );
        assert_eq!(
            BackendUrl::from("sftp://user@host:22")?,
            BackendUrl::Sftp(user.clone(), host.clone(), 22, PathBuf::from(""))
        );
        assert_eq!(
            BackendUrl::from("sftp://user@host:22//")?,
            BackendUrl::Sftp(user.clone(), host.clone(), 22, PathBuf::from("/"))
        );

        Ok(())
    }

    #[test]
    fn test_set_readonly_mode() {
        // --- FILE TESTS ---
        // Start with loose permissions: -rw-r--r-- (0o644)
        let initial_file = 0o100644;
        let result = set_readonly_mode(initial_file, true, false);
        // Should be strictly -r-------- (0o400)
        assert_eq!(result, 0o100400);

        // Toggle back to writable
        let result = set_readonly_mode(0o100400, false, false);
        // Should be -rw------- (0o600)
        assert_eq!(result, 0o100600);

        // --- DIRECTORY TESTS ---
        // Start with a Directory (0o040000) and loose perms (0o755)
        let initial_dir = 0o040755;

        // Set Directory to Read-Only
        let result = set_readonly_mode(initial_dir, true, true);
        // Should be dr-x------ (0o500) - Execute bit is required to list/enter!
        assert_eq!(result, 0o040500);
        assert!(result & 0o040000 != 0, "Must remain a directory bitmask");

        // Set Directory back to Writable
        let result = set_readonly_mode(result, false, true);
        // Should be drwx------ (0o700)
        assert_eq!(result, 0o040700);

        // --- SANITY CHECK ---
        // Ensure high-order bits (like SUID/SGID) are stripped or handled
        // if that is your helper's intent (current logic strips them)
        let messy_file = 0o100777;
        let result = set_readonly_mode(messy_file, true, false);
        assert_eq!(result, 0o100400);
    }
}
