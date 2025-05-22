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

pub mod dry;
pub mod localfs;
pub mod sftp;

use std::{
    path::{Path, PathBuf},
    str::FromStr,
    sync::Arc,
};

use crate::backend::sftp::SftpBackend;
use anyhow::{Result, anyhow, bail};
use dry::DryBackend;
use localfs::LocalFS;

use crate::{cli, utils::url::Url};

/// Abstraction of a storage backend.
///
/// A backend is a filesystem that can be present in the local machine, a remote
/// machine connected via SFTP, a cloud service, etc.
///
/// This trait provides an interface for file IO operations with the backend.
pub trait StorageBackend: Send + Sync {
    /// Creates the necessary structure (typically just the repo root directory) for the backend
    fn create(&self) -> Result<()>;

    fn root_exists(&self) -> bool;

    /// Reads from file.
    fn read(&self, path: &Path) -> Result<Vec<u8>>;

    /// Reads a specific range of bytes from a file, starting at `offset` and reading `length`` bytes.
    fn seek_read(&self, path: &Path, offset: u64, length: u64) -> Result<Vec<u8>>;

    /// Writes to file, creating the file if necessary.
    fn write(&self, path: &Path, contents: &[u8]) -> Result<()>;

    /// Renames a file.
    fn rename(&self, from: &Path, to: &Path) -> Result<()>;

    /// Removes a file.
    fn remove_file(&self, file_path: &Path) -> Result<()>;

    /// Creates a new, empty directory at the provided path.
    fn create_dir(&self, path: &Path) -> Result<()>;

    /// Recursively create a directory and all of its parent components if they are missing.
    fn create_dir_all(&self, path: &Path) -> Result<()>;

    // List all paths inside a directory.
    fn read_dir(&self, path: &Path) -> Result<Vec<PathBuf>>;

    /// Removes an empty directory.
    fn remove_dir(&self, path: &Path) -> Result<()>;

    /// Removes a directory after removing its contents.
    fn remove_dir_all(&self, path: &Path) -> Result<()>;

    /// Returns true if a path exists.
    fn exists(&self, path: &Path) -> bool;

    // Returns true if the path is a file or an error if the path does not exist.
    fn is_file(&self, path: &Path) -> bool;

    // Returns true if the path is a directory or an error if the path does not exist.
    fn is_dir(&self, path: &Path) -> bool;
}

/// Encapsulates a StorageBackend inside a DryBackend.
#[inline]
pub fn make_dry_backend(backend: Arc<dyn StorageBackend>, dry: bool) -> Arc<dyn StorageBackend> {
    match dry {
        true => Arc::new(DryBackend::new(backend.clone())),
        false => backend,
    }
}

pub fn new_backend_with_prompt(url: &str) -> Result<Arc<dyn StorageBackend>> {
    let backend_url = BackendUrl::from(url)?;

    match backend_url {
        BackendUrl::Local(repo_path) => Ok(Arc::new(LocalFS::new(repo_path))),
        BackendUrl::Sftp(username, host, port, repo_path) => {
            let password_prompt = format!("{}@{}'s password", username, host);
            let password = cli::request_password(&password_prompt);
            Ok(Arc::new(SftpBackend::new(
                repo_path, username, host, port, password,
            )?))
        }
    }
}

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

        match parsed_url.scheme.as_str() {
            "sftp" => {
                let user = parsed_url.username.to_string();

                let host = parsed_url
                    .host
                    .ok_or_else(|| anyhow!("SFTP URL '{}' requires a host", url_str))?
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
                    parsed_url.scheme,
                    url_str
                );
            }
        }
    }
}

#[cfg(test)]
mod test {
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
            BackendUrl::Local(PathBuf::from("./dir"))
        );
        assert_eq!(
            BackendUrl::from("file://./dir/").unwrap(),
            BackendUrl::Local(PathBuf::from("./dir/"))
        );
        assert_eq!(
            BackendUrl::from("file://.").unwrap(),
            BackendUrl::Local(PathBuf::from("."))
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
