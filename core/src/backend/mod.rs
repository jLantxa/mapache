pub mod cache;
pub mod dry;
pub mod limiter;
pub mod localfs;
pub mod mock;
pub mod s3;
pub mod sftp;

use std::{
    path::{Path, PathBuf},
    sync::Arc,
    time::SystemTime,
};

use anyhow::{Context, Result, anyhow, bail};
use async_trait::async_trait;
use dry::DryBackend;
use limiter::ThrottledBackend;
use localfs::LocalFS;
use percent_encoding::percent_decode_str;
use s3::S3Backend;
use url::Url;
use zeroize::Zeroizing;

use crate::{backend::sftp::SftpBackend, mapache::ContentIdType, ui};

/// Configuration for retry logic.
#[derive(Debug, Clone)]
pub struct RetryOptions {
    pub max_attempts: u32,
    pub base_delay: std::time::Duration,
    pub request_timeout: std::time::Duration,
}

impl Default for RetryOptions {
    fn default() -> Self {
        Self {
            max_attempts: 5,
            base_delay: std::time::Duration::from_millis(200),
            request_timeout: std::time::Duration::from_secs(30),
        }
    }
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
            Ok(Err(_)) if attempts < opts.max_attempts => {
                attempts += 1;
                let wait = opts.base_delay * (2_u32.pow(attempts - 1));
                tokio::time::sleep(wait).await;
            }
            Err(_) if attempts < opts.max_attempts => {
                attempts += 1;
                let wait = opts.base_delay * (2_u32.pow(attempts - 1));
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
#[derive(Debug, Clone)]
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
#[derive(Debug, Clone, Copy)]
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
    async fn list_dir(&self, path: &Path) -> Result<Vec<BackendNode>>;

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

    /// Returns true if the backend is a dry-run backend (ignoring writes).
    fn is_dry_run(&self) -> bool {
        false
    }

    /// Recursively lists all files under a directory.
    ///
    /// Default implementation iterates 256 fanout subdirectories for object paths.
    /// Backends like S3 override this with a single prefix-list call.
    async fn list_dir_recursive(&self, path: &Path) -> Result<Vec<BackendNode>> {
        use futures::stream::{self, StreamExt, TryStreamExt};

        let entries = stream::iter(self.list_dir(path).await?)
            .map(|node| {
                let backend = self;
                async move {
                    match node {
                        BackendNode::Dir(dir_path) => {
                            let sub = backend.list_dir_recursive(&dir_path).await?;
                            Ok::<Vec<BackendNode>, anyhow::Error>(sub)
                        }
                        BackendNode::File(_, _) => Ok(vec![node]),
                    }
                }
            })
            .buffer_unordered(8)
            .try_collect::<Vec<_>>()
            .await?
            .into_iter()
            .flatten()
            .collect();

        Ok(entries)
    }
}

/// Configuration used to initialize a backend.
pub struct BackendOptions {
    pub repo_path: String,
    pub ssh_privatekey: Option<PathBuf>,
    pub ssh_known_hosts: Option<PathBuf>,
    /// If true, wraps the chosen backend in a `DryBackend` to prevent actual writes.
    pub dry_backend: bool,
    pub limit_upload: Option<u64>,
    pub limit_download: Option<u64>,
}

/// Factory function to initialize a backend based on a URL string.
///
/// This will trigger interactive CLI prompts if environment variables
/// for credentials (like S3 keys or SFTP passwords) are missing.
pub async fn new_backend_with_prompt(opts: BackendOptions) -> Result<Arc<dyn StorageBackend>> {
    let backend_url = BackendUrl::from(&opts.repo_path)?;

    let backend: Arc<dyn StorageBackend> = match &backend_url {
        BackendUrl::Local(repo_path) => {
            tracing::info!(target: "backend", "Initializing LocalFS backend at {:?}", repo_path);
            Arc::new(LocalFS::new(repo_path.to_path_buf()))
        }
        BackendUrl::Sftp(username, host, port, repo_path) => {
            tracing::info!(target: "backend", "Initializing SFTP backend at {username}@{host}:{port}{:?}", repo_path);
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
                        sftp::AuthMethod::Password(ui::cli::request_password(&prompt)?)
                    }
                };

                match SftpBackend::new(
                    repo_path.clone(),
                    username.clone(),
                    host.clone(),
                    *port,
                    auth_method,
                    &opts,
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
            tracing::info!(target: "backend", "Initializing S3 backend (bucket: {}, prefix: {:?})", bucket, prefix);
            let endpoint = match std::env::var("AWS_ENDPOINT_URL") {
                Ok(v) => v,
                Err(_) => ui::cli::request_input("S3 Endpoint (leave empty for AWS)")?
                    .unwrap_or_else(|| "amazonaws.com".to_string()),
            };

            let region = match std::env::var("AWS_DEFAULT_REGION") {
                Ok(v) => v,
                Err(_) => {
                    ui::cli::request_input("S3 Region")?.unwrap_or_else(|| "us-east-1".to_string())
                }
            };

            let access_key = match std::env::var("AWS_ACCESS_KEY_ID") {
                Ok(v) => Zeroizing::new(v),
                Err(_) => {
                    Zeroizing::new(ui::cli::request_input("AWS Access Key ID")?.unwrap_or_default())
                }
            };

            let secret_key = match std::env::var("AWS_SECRET_ACCESS_KEY") {
                Ok(v) => Zeroizing::new(v),
                Err(_) => ui::cli::request_password("AWS Secret Access Key")?,
            };

            Arc::new(S3Backend::new(
                region,
                bucket.clone(),
                prefix.clone(),
                endpoint,
                access_key,
                secret_key,
            )?)
        }
    };

    // Dry backend wrapper
    let backend = match opts.dry_backend {
        true => Arc::new(DryBackend::new(backend.clone())),
        false => backend,
    };

    // Rate limiter wrapper
    let backend: Arc<dyn StorageBackend> = match &backend_url {
        BackendUrl::Sftp(..) => backend, // Native interleaved throttling.
        _ => {
            // Other backends use the generic wrapper (Wait-then-Burst / Bursty Debt).
            if opts.limit_upload.is_some() || opts.limit_download.is_some() {
                Arc::new(ThrottledBackend::new(
                    backend,
                    opts.limit_upload,
                    opts.limit_download,
                ))
            } else {
                backend
            }
        }
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

        let parsed_url = Url::parse(url_str).context("Failed to parse URL")?;

        match parsed_url.scheme() {
            "file" => {
                let path = parsed_url
                    .to_file_path()
                    .map_err(|_| anyhow!("Invalid file URL: {}", url_str))?;
                Ok(BackendUrl::Local(path))
            }
            "sftp" => {
                let user = percent_decode_str(parsed_url.username())
                    .decode_utf8()
                    .context("SFTP username contains invalid UTF-8")?
                    .to_string();

                let host = parsed_url
                    .host_str()
                    .ok_or_else(|| anyhow!("SFTP URL '{url_str}' requires a host"))?
                    .to_string();

                let port = parsed_url.port().unwrap_or(22);

                let path_str = percent_decode_str(parsed_url.path())
                    .decode_utf8()
                    .context("SFTP path contains invalid UTF-8")?
                    .to_string();

                // Handle relative vs absolute path in SFTP
                // Standard URL path always starts with /
                // sftp://host/rel -> path /rel -> rel
                // sftp://host//abs -> path //abs -> /abs
                let path = if let Some(stripped) = path_str.strip_prefix("//") {
                    PathBuf::from("/").join(stripped)
                } else {
                    PathBuf::from(path_str.trim_start_matches('/'))
                };

                Ok(BackendUrl::Sftp(user, host, port, path))
            }
            "s3" => {
                let bucket = parsed_url
                    .host_str()
                    .ok_or_else(|| anyhow!("S3 URL '{url_str}' requires a bucket name (host)"))?
                    .to_string();

                let prefix_str = percent_decode_str(parsed_url.path())
                    .decode_utf8()
                    .context("S3 prefix contains invalid UTF-8")?
                    .to_string();
                let prefix = PathBuf::from(prefix_str.trim_start_matches('/'));

                Ok(BackendUrl::S3(bucket, prefix))
            }
            _ => {
                bail!(
                    "Unsupported URL scheme: '{}' for URL '{}'",
                    parsed_url.scheme(),
                    url_str
                );
            }
        }
    }
}

/// Represents a generic filesystem node within a backend.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub enum BackendNode {
    File(PathBuf, u64),
    Dir(PathBuf),
}

impl BackendNode {
    /// Returns the inner path regardless of node type.
    pub fn path(&self) -> &Path {
        match self {
            BackendNode::File(path, _) => path,
            BackendNode::Dir(path) => path,
        }
    }

    /// Consumes the node and returns the inner path.
    pub fn into_path(self) -> PathBuf {
        match self {
            BackendNode::File(path, _) => path,
            BackendNode::Dir(path) => path,
        }
    }

    /// Returns the file name of the inner path.
    pub fn file_name(&self) -> Option<&std::ffi::OsStr> {
        self.path().file_name()
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

        for node in entries {
            match &node {
                BackendNode::File(_, _) => {
                    nodes.push(node);
                }
                BackendNode::Dir(sub_path) => {
                    nodes.push(node.clone());
                    stack.push(sub_path.to_path_buf());
                }
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
            base | 0o600 // rw--------
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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

    #[test]
    fn test_node_attr_default() {
        let attr = NodeAttr {
            size: None,
            uid: None,
            gid: None,
            perm: None,
            atime: None,
            mtime: None,
        };
        assert!(attr.size.is_none());
    }

    #[test]
    fn test_backend_url_parsing() {
        // Local
        assert_eq!(
            BackendUrl::from("/tmp/repo").unwrap(),
            BackendUrl::Local(PathBuf::from("/tmp/repo"))
        );

        #[cfg(not(windows))]
        assert_eq!(
            BackendUrl::from("file:///tmp/repo").unwrap(),
            BackendUrl::Local(PathBuf::from("/tmp/repo"))
        );

        #[cfg(windows)]
        assert_eq!(
            BackendUrl::from("file:///C:/tmp/repo").unwrap(),
            BackendUrl::Local(PathBuf::from("C:/tmp/repo"))
        );

        // SFTP
        let sftp_url = "sftp://user@host:2222/path/to/repo";
        if let BackendUrl::Sftp(user, host, port, path) = BackendUrl::from(sftp_url).unwrap() {
            assert_eq!(user, "user");
            assert_eq!(host, "host");
            assert_eq!(port, 2222);
            assert_eq!(path, PathBuf::from("path/to/repo"));
        } else {
            panic!("Failed to parse SFTP URL");
        }

        // S3
        let s3_url = "s3://my-bucket/my-prefix";
        if let BackendUrl::S3(bucket, prefix) = BackendUrl::from(s3_url).unwrap() {
            assert_eq!(bucket, "my-bucket");
            assert_eq!(prefix, PathBuf::from("my-prefix"));
        } else {
            panic!("Failed to parse S3 URL");
        }
    }
}
