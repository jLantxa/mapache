pub mod cache;
pub mod dry;
pub mod localfs;
pub mod sftp;

use std::{
    path::{Path, PathBuf},
    str::FromStr,
    sync::Arc,
    time::SystemTime,
};

use crate::{backend::sftp::SftpBackend, mapache::ContentIdType};
use anyhow::{Result, anyhow, bail};
use dry::DryBackend;
use localfs::LocalFS;

use crate::{ui, utils::url::Url};

pub struct NodeAttr {
    pub size: Option<u64>,
    pub uid: Option<u32>,
    pub gid: Option<u32>,
    pub perm: Option<u32>,
    pub atime: Option<SystemTime>,
    pub mtime: Option<SystemTime>,
}

#[derive(Debug)]
pub struct Handle<'a> {
    pub path: &'a Path,
    pub hint: Option<StorageHint>,
}

impl<'a> Handle<'a> {
    pub fn new(path: &'a Path) -> Self {
        Self { path, hint: None }
    }

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

#[derive(Debug)]
pub struct StorageHint {
    pub file_type: ContentIdType,
    pub is_metadata: bool,
}

/// Abstraction of a storage backend.
///
/// A backend is a filesystem that can be present in the local machine, a remote
/// machine connected via SFTP, a cloud service, etc.
///
/// This trait provides an interface for file IO operations with the backend.
pub trait StorageBackend: Send + Sync {
    /// Creates the necessary structure (typically just the repo root directory) for the backend
    fn create(&self) -> Result<()>;

    /// Returns true if the root of the backend exists.
    fn root_exists(&self) -> bool;

    /// Returns true if a path exists.
    fn path_exists(&self, path: &Path) -> bool;

    /// Reads from file.
    ///
    /// If `length` is 0, it reads until the end. If `offset` is negative, it reads from the end of
    /// the file.
    fn read(&self, handle: &Handle, offset: isize, length: usize) -> Result<Vec<u8>>;

    /// Writes to file, creating the file if necessary.
    fn write(&self, handle: &Handle, contents: &[u8]) -> Result<()>;

    /// Renames a file. If the destination exists already, it is overwritten.
    fn rename(&self, from: &Path, to: &Path) -> Result<()>;

    // List all paths inside a directory.
    fn list_dir(&self, path: &Path) -> Result<Vec<PathBuf>>;

    /// Creates a new, empty directory at the provided path.
    /// The directory is created recursively, with all of its parent components.
    fn create_dir(&self, path: &Path) -> Result<()>;

    /// Removes a path (directory or file) and all its contents recursively.
    fn remove(&self, file_path: &Path) -> Result<()>;

    // Returns true if the path is a file or an error if the path does not exist.
    fn is_file(&self, path: &Path) -> bool;

    // Returns true if the path is a directory or an error if the path does not exist.
    fn is_dir(&self, path: &Path) -> bool;

    /// Query metadata without following symlinks.
    fn lstat(&self, path: &Path) -> Result<NodeAttr>;
}

pub struct BackendOptions {
    pub repo_path: String,
    pub ssh_pubkey: Option<PathBuf>,
    pub ssh_privatekey: Option<PathBuf>,
    pub dry_backend: bool,
    pub cached: bool,
}

/// Open a new backend and prompt for authentication credentials.
pub fn new_backend_with_prompt(opts: BackendOptions) -> Result<Arc<dyn StorageBackend>> {
    let backend_url = BackendUrl::from(&opts.repo_path)?;

    let backend: Arc<dyn StorageBackend> = match backend_url {
        BackendUrl::Local(repo_path) => Arc::new(LocalFS::new(repo_path)),
        BackendUrl::Sftp(username, host, port, repo_path) => {
            let auth_method = if let Some(private_key) = &opts.ssh_privatekey {
                sftp::AuthMethod::PubKey {
                    pubkey: opts.ssh_pubkey,
                    private_key: private_key.to_path_buf(),
                    passphrase: None,
                }
            } else {
                let password_prompt = format!("{username}@{host}'s password");
                let password = ui::cli::request_password(&password_prompt);
                sftp::AuthMethod::Password(password)
            };

            Arc::new(SftpBackend::new(
                repo_path,
                username,
                host,
                port,
                auth_method,
            )?)
        }
    };

    let backend = match opts.dry_backend {
        true => Arc::new(DryBackend::new(backend.clone())),
        false => backend,
    };

    Ok(backend)
}

/// The URL to a backend. This could be a path to a directory, an SSH URL, or others.
#[derive(Debug, Clone, PartialEq)]
pub enum BackendUrl {
    Local(PathBuf),
    Sftp(String, String, u16, PathBuf), // (user, host, port, path)
}

impl BackendUrl {
    /// Parses a URL string into a `BackendUrl` variant.
    pub fn from(url_str: &str) -> Result<Self> {
        if !url_str.contains("://") {
            return Ok(BackendUrl::Local(PathBuf::from(url_str)));
        }

        let parsed_url = Url::from_str(url_str)?;

        match parsed_url.scheme.as_ref() {
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
            "file" => {
                let path_str: &str = &parsed_url.path.join("/");
                let path_buf = PathBuf::from(path_str);
                Ok(BackendUrl::Local(path_buf))
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

/// A directory or file in a backend with its path.
#[derive(Debug, Clone, PartialEq)]
pub enum BackendNode {
    File(PathBuf),
    Dir(PathBuf),
}

impl BackendNode {
    /// Returns the path to the node.
    pub fn path(&self) -> &Path {
        match self {
            BackendNode::File(path) => path,
            BackendNode::Dir(path) => path,
        }
    }
}

/// Recursively list all files and directories in a backend.
pub fn read_backend_dir(backend: &dyn StorageBackend, path: &Path) -> Result<Vec<BackendNode>> {
    let mut nodes = Vec::new();

    let root_nodes = backend.list_dir(path)?;
    for sub_path in root_nodes {
        if backend.is_file(&sub_path) {
            nodes.push(BackendNode::File(sub_path.to_path_buf()));
        } else if backend.is_dir(&sub_path) {
            nodes.push(BackendNode::Dir(sub_path.to_path_buf()));
            let mut sub_nodes = read_backend_dir(backend, &sub_path)?;
            nodes.append(&mut sub_nodes);
        }
    }

    Ok(nodes)
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
}
