use std::{
    io::{Read, Seek, SeekFrom, Write},
    net::TcpStream,
    path::{Path, PathBuf},
    sync::Arc,
    time::{Duration, UNIX_EPOCH},
};

use anyhow::{Context, Result, anyhow, bail};
use crossbeam_channel::{Receiver, Sender, bounded};
use ssh2::{RenameFlags, Session, Sftp};

use crate::{
    backend::{Handle, set_readonly_mode},
    repository::repo::REPO_TMP_EXTENSION,
    ui,
};

use super::StorageBackend;

/// Maximum number of concurrent SSH sessions to maintain in the pool.
const MAX_CONNECTION_POOL_SIZE: usize = 5;

/// Supported authentication methods for the SFTP backend.
pub enum AuthMethod {
    /// Standard username/password authentication.
    Password(String),
    /// Public key authentication using a private key file.
    PubKey {
        pubkey: Option<PathBuf>,
        private_key: PathBuf,
        passphrase: Option<String>,
    },
}

/// Represents a single active SFTP connection.
///
/// It encapsulates both the SSH session and the SFTP subsystem handle.
pub struct SftpConnection {
    _session: Arc<Session>,
    sftp: Sftp,
}

impl SftpConnection {
    /// Establishes a new TCP connection and performs SSH handshake and authentication.
    pub fn new(username: &str, host: &str, port: u16, auth_method: &AuthMethod) -> Result<Self> {
        let addr = format!("{host}:{port}");
        let tcp = TcpStream::connect(&addr).context("Failed to connect to SFTP server")?;
        let mut session = Session::new().context("Failed to create SSH session")?;
        session.set_tcp_stream(tcp);
        session
            .handshake()
            .context("Failed to perform SSH handshake")?;

        Self::authenticate(&session, username, auth_method)?;

        // Enable keepalive to prevent timeouts during long-running backups
        session.set_keepalive(true, 30);
        session.set_compress(false);

        let sftp = session.sftp().context("Failed to create SFTP session")?;
        Ok(Self {
            _session: Arc::new(session),
            sftp,
        })
    }

    /// Borrows the SFTP client from the connection.
    pub fn sftp(&self) -> &Sftp {
        &self.sftp
    }

    /// Borrows the SFTP client mutably from the connection.
    pub fn sftp_mut(&mut self) -> &mut Sftp {
        &mut self.sftp
    }

    fn authenticate(session: &Session, username: &str, auth_method: &AuthMethod) -> Result<()> {
        match auth_method {
            AuthMethod::Password(password) => session
                .userauth_password(username, password)
                .context("Failed to authenticate with password"),
            AuthMethod::PubKey {
                pubkey,
                private_key,
                passphrase,
            } => session
                .userauth_pubkey_file(
                    username,
                    pubkey.as_deref(),
                    private_key,
                    passphrase.as_deref(),
                )
                .map_err(|e| anyhow!(format!("Failed to authenticate with pubkey: {e}"))),
        }
    }
}

/// A thread-safe pool of SFTP connections.
///
/// Uses a bounded channel to distribute connections to worker threads.
/// This prevents the overhead of re-authenticating for every file operation.
pub struct SftpConnectionPool {
    sender: Sender<SftpConnection>,
    receiver: Receiver<SftpConnection>,
}

impl SftpConnectionPool {
    /// Initializes the pool by attempting to open `capacity` connections.
    pub fn new(
        capacity: usize,
        username: String,
        host: String,
        port: u16,
        auth_method: &AuthMethod,
    ) -> Result<Self> {
        let mut connections = Vec::new();

        const MAX_CONNECTION_RETRIES: u32 = 3;
        let mut connection_retry_count = 0;
        for _ in 0..capacity {
            match SftpConnection::new(&username, &host, port, auth_method) {
                Ok(conn) => connections.push(conn),
                Err(e) => {
                    // We could not establish a connection. That could mean that we reached a limit
                    // on the server side or it was a punctual error. We can try again.

                    if connection_retry_count < MAX_CONNECTION_RETRIES {
                        ui::cli::warning!(
                            "Failed to establish SFTP connection: {}. Retrying...",
                            e.to_string()
                        );
                        connection_retry_count += 1;
                    } else {
                        ui::cli::warning!("Max connection retries exceeded.");
                        break;
                    }
                }
            }
        }

        let num_established_connections = connections.len();
        if num_established_connections < 1 {
            bail!("Failed to establish SFTP connections");
        }

        let (sender, receiver) = bounded(num_established_connections);
        for connection in connections {
            sender
                .send(connection)
                .expect("Failed to populate connection pool");
        }

        Ok(Self { sender, receiver })
    }

    /// Acquires a connection from the pool. Blocks if all connections are in use.
    pub fn get(&self) -> Result<PooledSftpConnection> {
        let conn = self
            .receiver
            .recv()
            .context("Failed to get connection from pool")?;
        Ok(PooledSftpConnection {
            connection: Some(conn),
            pool_sender: self.sender.clone(),
        })
    }
}

/// RAII guard that returns an `SftpConnection` to the pool when dropped.
pub struct PooledSftpConnection {
    connection: Option<SftpConnection>,
    pool_sender: Sender<SftpConnection>,
}

impl std::ops::Deref for PooledSftpConnection {
    type Target = SftpConnection;

    fn deref(&self) -> &Self::Target {
        self.connection.as_ref().unwrap()
    }
}

impl std::ops::DerefMut for PooledSftpConnection {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.connection.as_mut().unwrap()
    }
}

impl Drop for PooledSftpConnection {
    fn drop(&mut self) {
        if let Some(conn) = self.connection.take() {
            let _ = self.pool_sender.send(conn);
        }
    }
}

/// Storage backend implementation for SFTP.
pub struct SftpBackend {
    base_path: PathBuf,
    pool: Arc<SftpConnectionPool>,
}

impl SftpBackend {
    pub fn new(
        base_path: PathBuf,
        username: String,
        host: String,
        port: u16,
        auth_method: AuthMethod,
    ) -> Result<Self> {
        let pool = Arc::new(SftpConnectionPool::new(
            MAX_CONNECTION_POOL_SIZE,
            username,
            host,
            port,
            &auth_method,
        )?);

        Ok(Self { base_path, pool })
    }

    #[inline]
    fn full_path(&self, path: &Path) -> PathBuf {
        self.base_path.join(path)
    }

    /// Check existence using `lstat` to avoid following symlinks.
    fn exists_exact(&self, path: &Path, sftp: &Sftp) -> bool {
        sftp.lstat(path).is_ok()
    }

    /// Recursively ensures a directory path exists on the remote server.
    fn create_dir_all_internal(&self, path: &Path, sftp: &Sftp) -> Result<()> {
        if self.exists_exact(path, sftp) {
            let metadata = sftp.stat(path)?;
            if metadata.is_dir() {
                return Ok(());
            } else {
                bail!("Path {path:?} exists but is not a directory");
            }
        }

        if let Some(parent) = path.parent()
            && parent != Path::new("")
        {
            self.create_dir_all_internal(parent, sftp)?;
        }

        sftp.mkdir(path, 0o755)
            .with_context(|| format!("Failed to create directory {path:?}' in sftp backend"))
    }

    /// Adjusts remote file permissions.
    /// Used to toggle write access before modification/deletion.
    fn set_readonly_status(&self, sftp: &Sftp, path: &Path, readonly: bool) -> Result<()> {
        let full_path = self.full_path(path);

        let mut stat = match sftp.stat(&full_path) {
            Ok(s) => s,
            Err(_) if !readonly => {
                // If we are trying to make it writable and it doesn't exist, we're done.
                return Ok(());
            }
            Err(e) => return Err(anyhow!(e)).context("Failed to stat remote file"),
        };

        let is_dir = stat.is_dir();
        if let Some(perm) = stat.perm.as_mut() {
            *perm = set_readonly_mode(*perm, readonly, is_dir);
            sftp.setstat(&full_path, stat)?;
        }

        Ok(())
    }

    /// Internal recursive removal logic.
    fn remove_recursively_internal(&self, path: &Path, sftp: &Sftp) -> Result<()> {
        if !self.exists_exact(path, sftp) {
            return Ok(());
        }

        // Ensure the item itself is writable before we try to remove it
        let _ = self.set_readonly_status(sftp, path, false);

        let metadata = sftp.lstat(path)?;
        if metadata.is_file() {
            return sftp
                .unlink(path)
                .with_context(|| format!("Failed to remove file {path:?}' in sftp backend"));
        }

        if metadata.is_dir() {
            let entries = sftp.readdir(path)?;
            for (entry_path, _) in entries {
                self.remove_recursively_internal(&entry_path, sftp)?;
            }
            return sftp
                .rmdir(path)
                .with_context(|| format!("Failed to remove directory {path:?}' in sftp backend"));
        }

        sftp.unlink(path)
            .or_else(|_| sftp.rmdir(path))
            .map_err(|e| anyhow!(e))
    }

    /// Internal rename logic.
    /// Handles overwriting destination and permission toggling.
    fn rename_internal(&self, sftp: &Sftp, from: &Path, to: &Path) -> Result<()> {
        let full_from = self.full_path(from);
        let full_to = self.full_path(to);

        let _ = self.set_readonly_status(sftp, from, false);

        if sftp.stat(&full_to).is_ok() {
            let _ = self.set_readonly_status(sftp, to, false);
            let _ = sftp.unlink(&full_to);
        }

        sftp.rename(&full_from, &full_to, Some(RenameFlags::all()))?;
        let _ = self.set_readonly_status(sftp, to, true);

        Ok(())
    }
}

impl StorageBackend for SftpBackend {
    fn create(&self) -> Result<()> {
        let conn = self.pool.get()?;
        self.create_dir_all_internal(&self.base_path, conn.sftp())
    }

    /// Reads a range of bytes from a remote file.
    fn read(&self, handle: &Handle, offset: isize, length: usize) -> Result<Vec<u8>> {
        let path = handle.path;
        let full_path = self.full_path(path);

        let conn = self.pool.get()?;
        let sftp = conn.sftp();
        let mut file = sftp
            .open(&full_path)
            .with_context(|| format!("Failed to open file {path:?} in sftp backend for reading"))?;

        let seek_from = if offset >= 0 {
            SeekFrom::Start(offset as u64)
        } else {
            SeekFrom::End(offset as i64)
        };

        file.seek(seek_from)?;

        let mut contents = Vec::new();
        if length == 0 {
            file.read_to_end(&mut contents)
                .with_context(|| format!("Failed to read to end of sftp file {path:?}"))?;
        } else {
            let mut limited_reader = file.take(length as u64);
            limited_reader.read_to_end(&mut contents).with_context(|| {
                format!("Failed to read {length} bytes from sftp file {path:?}")
            })?;
        }

        Ok(contents)
    }

    /// Atomic-style write: writes to a temporary file then renames.
    fn write(&self, handle: &Handle, contents: &[u8]) -> Result<()> {
        let path = handle.path;
        let tmp_path = path.with_extension(REPO_TMP_EXTENSION);
        let full_tmp_path = self.full_path(&tmp_path);
        let conn = self.pool.get()?;
        let sftp = conn.sftp();

        let mut file = sftp.create(&full_tmp_path)?;

        if file.write_all(contents).is_err() {
            // Auto-create parent if it doesn't exist on first write failure
            if let Some(parent) = path.parent() {
                let _ = self.create_dir(parent);
            }
            file.write_all(contents)?;
        }

        let full_path = self.full_path(path);
        let _ = self.set_readonly_status(sftp, &full_path, false);

        self.rename_internal(sftp, &tmp_path, path)
    }

    fn rename(&self, from: &Path, to: &Path) -> Result<()> {
        let conn = self.pool.get()?;
        let sftp = conn.sftp();
        self.rename_internal(sftp, from, to)
            .with_context(|| format!("Failed to rename {from:?}' to {to:?}' in sftp backend"))
    }

    fn create_dir(&self, path: &Path) -> Result<()> {
        let full_path = self.full_path(path);
        let conn = self.pool.get()?;
        self.create_dir_all_internal(&full_path, conn.sftp())
    }

    fn remove(&self, path: &Path) -> Result<()> {
        let full_path = self.full_path(path);
        let conn = self.pool.get()?;
        self.remove_recursively_internal(&full_path, conn.sftp())
    }

    fn list_dir(&self, path: &Path) -> Result<Vec<PathBuf>> {
        let full_path = self.full_path(path);
        let conn = self.pool.get()?;
        let entries = conn
            .sftp()
            .readdir(full_path)
            .with_context(|| format!("Could not list directory {path:?}' in sftp backend"))?;

        Ok(entries
            .iter()
            .map(|(p, _)| p.strip_prefix(&self.base_path).unwrap().to_path_buf())
            .collect())
    }

    fn path_exists(&self, path: &Path) -> bool {
        let full_path = self.full_path(path);
        let conn = self.pool.get().unwrap();
        self.exists_exact(&full_path, conn.sftp())
    }

    fn is_file(&self, path: &Path) -> bool {
        let full_path = self.full_path(path);
        let conn = self.pool.get().unwrap();
        conn.sftp()
            .lstat(&full_path)
            .map(|s| s.is_file())
            .unwrap_or(false)
    }

    fn is_dir(&self, path: &Path) -> bool {
        let full_path = self.full_path(path);
        let conn = self.pool.get().unwrap();
        conn.sftp()
            .lstat(&full_path)
            .map(|s| s.is_dir())
            .unwrap_or(false)
    }

    /// Fetches file metadata from the remote server.
    fn lstat(&self, path: &Path) -> Result<super::NodeAttr> {
        let full_path = self.full_path(path);
        let conn = self.pool.get()?;
        let meta = conn.sftp().lstat(&full_path)?;

        let to_system_time = |t: u64| {
            if t == 0 {
                None
            } else {
                Some(UNIX_EPOCH + Duration::from_secs(t))
            }
        };

        Ok(super::NodeAttr {
            size: meta.size,
            uid: meta.uid,
            gid: meta.gid,
            perm: meta.perm,
            atime: meta.atime.and_then(to_system_time),
            mtime: meta.mtime.and_then(to_system_time),
        })
    }
}
