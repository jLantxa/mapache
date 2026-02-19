use std::{
    io::{Read, Seek, SeekFrom, Write},
    net::TcpStream,
    path::{Path, PathBuf},
    sync::Arc,
    time::{Duration, UNIX_EPOCH},
};

use anyhow::{Context, Result, anyhow, bail};
use async_trait::async_trait;
use crossbeam_channel::{Receiver, Sender, bounded};
use ssh2::{RenameFlags, Session, Sftp};
use tokio::task;

use crate::{
    backend::{Handle, WriteContents, set_readonly_mode},
    repository::repo::REPO_TMP_EXTENSION,
    ui,
};

use super::{NodeAttr, StorageBackend};

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

        // Keepalive to prevent timeouts during long-running backups
        session.set_keepalive(true, 30);
        session.set_compress(false);

        let sftp = session.sftp().context("Failed to create SFTP session")?;
        Ok(Self {
            _session: Arc::new(session),
            sftp,
        })
    }

    pub fn sftp(&self) -> &Sftp {
        &self.sftp
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
pub struct SftpConnectionPool {
    sender: Sender<SftpConnection>,
    receiver: Receiver<SftpConnection>,
}

impl SftpConnectionPool {
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

        let established = connections.len();
        if established < 1 {
            bail!("Failed to establish SFTP connections");
        }

        let (sender, receiver) = bounded(established);
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

/// RAII guard that returns an SftpConnection to the pool when dropped.
pub struct PooledSftpConnection {
    connection: Option<SftpConnection>,
    pool_sender: Sender<SftpConnection>,
}

impl std::ops::Deref for PooledSftpConnection {
    type Target = SftpConnection;

    fn deref(&self) -> &Self::Target {
        self.connection
            .as_ref()
            .expect("PooledSftpConnection missing conn")
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
///
/// Uses blocking ssh2 APIs but offloads work via tokio::task::spawn_blocking.
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
    fn full_path(base_path: &Path, path: &Path) -> PathBuf {
        base_path.join(path)
    }

    /// Existence check using lstat (full remote path).
    fn exists_exact_full(sftp: &Sftp, full_path: &Path) -> bool {
        sftp.lstat(full_path).is_ok()
    }

    /// Recursively ensure directory exists (full remote path).
    fn create_dir_all_full(sftp: &Sftp, full_path: &Path) -> Result<()> {
        if Self::exists_exact_full(sftp, full_path) {
            let meta = sftp.stat(full_path)?;
            if meta.is_dir() {
                return Ok(());
            }
            bail!("Path {full_path:?} exists but is not a directory");
        }

        if let Some(parent) = full_path.parent()
            && !parent.as_os_str().is_empty()
        {
            Self::create_dir_all_full(sftp, parent)?;
        }

        sftp.mkdir(full_path, 0o755)
            .with_context(|| format!("Failed to create directory {full_path:?} in sftp backend"))
    }

    /// Toggle readonly permissions (full remote path).
    fn set_readonly_status_full(sftp: &Sftp, full_path: &Path, readonly: bool) -> Result<()> {
        let mut stat = match sftp.stat(full_path) {
            Ok(s) => s,
            Err(_) if !readonly => return Ok(()), // making writable and doesn't exist => ok
            Err(e) => return Err(anyhow!(e)).context("Failed to stat remote file"),
        };

        let is_dir = stat.is_dir();
        if let Some(perm) = stat.perm.as_mut() {
            *perm = set_readonly_mode(*perm, readonly, is_dir);
            sftp.setstat(full_path, stat)?;
        }
        Ok(())
    }

    /// Recursive removal (full remote path).
    fn remove_recursively_full(sftp: &Sftp, full_path: &Path) -> Result<()> {
        if !Self::exists_exact_full(sftp, full_path) {
            return Ok(());
        }

        let _ = Self::set_readonly_status_full(sftp, full_path, false);

        let meta = sftp.lstat(full_path)?;
        if meta.is_file() {
            return sftp
                .unlink(full_path)
                .with_context(|| format!("Failed to remove file {full_path:?} in sftp backend"));
        }

        if meta.is_dir() {
            let entries = sftp.readdir(full_path)?;
            for (p, _) in entries {
                if p.file_name().is_some_and(|n| n == "." || n == "..") {
                    continue;
                }
                Self::remove_recursively_full(sftp, &p)?;
            }
            return sftp.rmdir(full_path).with_context(|| {
                format!("Failed to remove directory {full_path:?} in sftp backend")
            });
        }

        // Fallback for other node types
        sftp.unlink(full_path)
            .or_else(|_| sftp.rmdir(full_path))
            .map_err(|e| anyhow!(e))
    }

    /// Rename with overwrite + permission toggling (full remote paths).
    fn rename_full(sftp: &Sftp, full_from: &Path, full_to: &Path) -> Result<()> {
        let _ = Self::set_readonly_status_full(sftp, full_from, false);

        if sftp.stat(full_to).is_ok() {
            let _ = Self::set_readonly_status_full(sftp, full_to, false);
            let _ = sftp.unlink(full_to);
        }

        sftp.rename(full_from, full_to, Some(RenameFlags::all()))?;
        let _ = Self::set_readonly_status_full(sftp, full_to, true);

        Ok(())
    }
}

#[async_trait]
impl StorageBackend for SftpBackend {
    async fn create(&self) -> Result<()> {
        let base_path = self.base_path.clone();
        let pool = self.pool.clone();

        task::spawn_blocking(move || {
            let conn = pool.get()?;
            SftpBackend::create_dir_all_full(conn.sftp(), &base_path)
        })
        .await
        .map_err(|e| anyhow!(e))
        .context("SFTP create task failed")?
    }

    async fn path_exists(&self, path: &Path) -> bool {
        let base_path = self.base_path.clone();
        let pool = self.pool.clone();
        let path = path.to_path_buf();

        task::spawn_blocking(move || {
            let full = SftpBackend::full_path(&base_path, &path);
            let conn = match pool.get() {
                Ok(c) => c,
                Err(_) => return false,
            };
            SftpBackend::exists_exact_full(conn.sftp(), &full)
        })
        .await
        .unwrap_or(false)
    }

    async fn read(&self, handle: &Handle, offset: isize, length: usize) -> Result<Vec<u8>> {
        let base_path = self.base_path.clone();
        let pool = self.pool.clone();
        let path = handle.path.to_path_buf();

        task::spawn_blocking(move || {
            let full = SftpBackend::full_path(&base_path, &path);
            let conn = pool.get()?;
            let sftp = conn.sftp();

            let mut file = sftp.open(&full).with_context(|| {
                format!("Failed to open file {path:?} in sftp backend for reading")
            })?;

            let seek_from = if offset >= 0 {
                SeekFrom::Start(offset as u64)
            } else {
                SeekFrom::End(offset as i64)
            };
            file.seek(seek_from)?;

            let mut contents = if length > 0 {
                Vec::with_capacity(length)
            } else {
                Vec::new()
            };

            if length == 0 {
                file.read_to_end(&mut contents)
                    .with_context(|| format!("Failed to read to end of sftp file {path:?}"))?;
            } else {
                unsafe {
                    // SAFETY: set_len is called only after read_exact successfully populates the buffer.
                    let slice = std::slice::from_raw_parts_mut(contents.as_mut_ptr(), length);
                    file.read_exact(slice).with_context(|| {
                        format!("Failed to read {length} bytes from sftp file {path:?}")
                    })?;
                    contents.set_len(length);
                }
            }

            Ok(contents)
        })
        .await
        .map_err(|e| anyhow!(e))
        .context("SFTP read task failed")?
    }

    async fn write(&self, handle: &Handle, contents: WriteContents<'_>) -> Result<()> {
        let base_path = self.base_path.clone();
        let pool = self.pool.clone();
        let path = handle.path.to_path_buf();
        let data = contents.into_owned();

        task::spawn_blocking(move || {
            let tmp_path = path.with_extension(REPO_TMP_EXTENSION);

            let full_tmp = SftpBackend::full_path(&base_path, &tmp_path);
            let full_dst = SftpBackend::full_path(&base_path, &path);

            let conn = pool.get()?;
            let sftp = conn.sftp();

            // Ensure parent exists
            if let Some(parent) = full_tmp.parent() {
                let _ = SftpBackend::create_dir_all_full(sftp, parent);
            }

            // Write tmp
            let mut file = sftp.create(&full_tmp).with_context(|| {
                format!("Failed to create tmp file {tmp_path:?} in sftp backend")
            })?;
            file.write_all(&data).with_context(|| {
                format!("Failed to write tmp file {tmp_path:?} in sftp backend")
            })?;

            // Commit tmp -> dst (atomic-ish)
            let _ = SftpBackend::set_readonly_status_full(sftp, &full_dst, false);
            SftpBackend::rename_full(sftp, &full_tmp, &full_dst)
                .with_context(|| format!("Failed to commit tmp write {tmp_path:?} -> {path:?}"))
        })
        .await
        .map_err(|e| anyhow!(e))
        .context("SFTP write task failed")?
    }

    async fn rename(&self, from: &Path, to: &Path) -> Result<()> {
        let base_path = self.base_path.clone();
        let pool = self.pool.clone();
        let from = from.to_path_buf();
        let to = to.to_path_buf();

        task::spawn_blocking(move || {
            let full_from = SftpBackend::full_path(&base_path, &from);
            let full_to = SftpBackend::full_path(&base_path, &to);

            let conn = pool.get()?;
            SftpBackend::rename_full(conn.sftp(), &full_from, &full_to)
                .with_context(|| format!("Failed to rename {from:?} -> {to:?} in sftp backend"))
        })
        .await
        .map_err(|e| anyhow!(e))
        .context("SFTP rename task failed")?
    }

    async fn list_dir(&self, path: &Path) -> Result<Vec<PathBuf>> {
        let base_path = self.base_path.clone();
        let pool = self.pool.clone();
        let path = path.to_path_buf();

        task::spawn_blocking(move || {
            let full = SftpBackend::full_path(&base_path, &path);
            let conn = pool.get()?;
            let entries = conn
                .sftp()
                .readdir(&full)
                .with_context(|| format!("Could not list directory {path:?} in sftp backend"))?;

            let mut out = Vec::new();
            for (p, _) in entries {
                if p.file_name().is_some_and(|n| n == "." || n == "..") {
                    continue;
                }
                if let Ok(rel) = p.strip_prefix(&base_path) {
                    out.push(rel.to_path_buf());
                }
            }
            Ok(out)
        })
        .await
        .map_err(|e| anyhow!(e))
        .context("SFTP list_dir task failed")?
    }

    async fn create_dir(&self, path: &Path) -> Result<()> {
        let base_path = self.base_path.clone();
        let pool = self.pool.clone();
        let path = path.to_path_buf();

        task::spawn_blocking(move || {
            let full = SftpBackend::full_path(&base_path, &path);
            let conn = pool.get()?;
            SftpBackend::create_dir_all_full(conn.sftp(), &full)
        })
        .await
        .map_err(|e| anyhow!(e))
        .context("SFTP create_dir task failed")?
    }

    async fn remove(&self, file_path: &Path) -> Result<()> {
        let base_path = self.base_path.clone();
        let pool = self.pool.clone();
        let path = file_path.to_path_buf();

        task::spawn_blocking(move || {
            let full = SftpBackend::full_path(&base_path, &path);
            let conn = pool.get()?;
            SftpBackend::remove_recursively_full(conn.sftp(), &full)
        })
        .await
        .map_err(|e| anyhow!(e))
        .context("SFTP remove task failed")?
    }

    async fn is_file(&self, path: &Path) -> bool {
        let base_path = self.base_path.clone();
        let pool = self.pool.clone();
        let path = path.to_path_buf();

        task::spawn_blocking(move || {
            let full = SftpBackend::full_path(&base_path, &path);
            let conn = match pool.get() {
                Ok(c) => c,
                Err(_) => return false,
            };
            conn.sftp()
                .lstat(&full)
                .map(|s| s.is_file())
                .unwrap_or(false)
        })
        .await
        .unwrap_or(false)
    }

    async fn is_dir(&self, path: &Path) -> bool {
        let base_path = self.base_path.clone();
        let pool = self.pool.clone();
        let path = path.to_path_buf();

        task::spawn_blocking(move || {
            let full = SftpBackend::full_path(&base_path, &path);
            let conn = match pool.get() {
                Ok(c) => c,
                Err(_) => return false,
            };
            conn.sftp()
                .lstat(&full)
                .map(|s| s.is_dir())
                .unwrap_or(false)
        })
        .await
        .unwrap_or(false)
    }

    async fn lstat(&self, path: &Path) -> Result<NodeAttr> {
        let base_path = self.base_path.clone();
        let pool = self.pool.clone();
        let path = path.to_path_buf();

        task::spawn_blocking(move || {
            let full = SftpBackend::full_path(&base_path, &path);
            let conn = pool.get()?;
            let meta = conn.sftp().lstat(&full)?;

            let to_system_time = |t: u64| {
                if t == 0 {
                    None
                } else {
                    Some(UNIX_EPOCH + Duration::from_secs(t))
                }
            };

            Ok(NodeAttr {
                size: meta.size,
                uid: meta.uid,
                gid: meta.gid,
                perm: meta.perm,
                atime: meta.atime.and_then(to_system_time),
                mtime: meta.mtime.and_then(to_system_time),
            })
        })
        .await
        .map_err(|e| anyhow!(e))
        .context("SFTP lstat task failed")?
    }
}
