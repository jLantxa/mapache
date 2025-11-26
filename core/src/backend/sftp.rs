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

use crate::{backend::Handle, repository::repo::REPO_TMP_EXTENSION, ui};

use super::StorageBackend;

const MAX_CONNECTION_POOL_SIZE: usize = 5;

pub enum AuthMethod {
    Password(String),
    PubKey {
        pubkey: Option<PathBuf>,
        private_key: PathBuf,
        passphrase: Option<String>,
    },
}

/// Represents a single SFTP connection, holding its SSH session and SFTP client.
pub struct SftpConnection {
    _session: Arc<Session>,
    sftp: Sftp,
}

impl SftpConnection {
    /// Creates a new SFTP connection.
    pub fn new(username: &str, host: &str, port: u16, auth_method: &AuthMethod) -> Result<Self> {
        let addr = format!("{host}:{port}");
        let tcp = TcpStream::connect(&addr).context("Failed to connect to SFTP server")?;
        let mut session = Session::new().context("Failed to create SSH session")?;
        session.set_tcp_stream(tcp);
        session
            .handshake()
            .context("Failed to perform SSH handshake")?;

        Self::authenticate(&session, username, auth_method)?;

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
        // Authenticate
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

/// A pool of SFTP connections.
pub struct SftpConnectionPool {
    sender: Sender<SftpConnection>,
    receiver: Receiver<SftpConnection>,
}

impl SftpConnectionPool {
    /// Creates a new connection pool with a specified capacity.
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

    /// Gets an SFTP connection from the pool, blocking until one is available.
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

/// A wrapper for an SFTP connection obtained from the pool.
/// When dropped, the connection is returned to the pool.
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
            self.pool_sender
                .send(conn)
                .expect("Failed to return connection to pool");
        }
    }
}

/// A remote backend connected by SFTP.
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

    /// Returns true if the exact path given exists (not as a relative path to the backend root).
    fn exists_exact(&self, path: &Path, sftp: &Sftp) -> bool {
        sftp.lstat(path).is_ok()
    }

    fn create_dir_all_internal(&self, path: &Path, sftp: &Sftp) -> Result<()> {
        if self.exists_exact(path, sftp) {
            let metadata = sftp
                .stat(path)
                .with_context(|| format!("Failed to get metadata for path: {path:?}"))?;
            if metadata.is_dir() {
                return Ok(());
            } else {
                return Err(anyhow::anyhow!(
                    "Path {path:?} exists but is not a directory"
                ));
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

    /// Recursively removes a directory/file.
    fn remove_recursively_internal(&self, path: &Path, sftp: &Sftp) -> Result<()> {
        if !self.exists_exact(path, sftp) {
            return Ok(());
        }

        match sftp.lstat(path) {
            Ok(metadata) => {
                if metadata.is_file() {
                    return sftp.unlink(path).with_context(|| {
                        format!("Failed to remove file {path:?}' in sftp backend")
                    });
                }

                if metadata.is_dir() {
                    let entries = sftp.readdir(path).with_context(|| {
                        format!("Could not list directory {path:?}' in sftp backend")
                    })?;

                    for (entry_path, _entry_metadata) in entries {
                        self.remove_recursively_internal(&entry_path, sftp)?;
                    }

                    return sftp.rmdir(path).with_context(|| {
                        format!("Failed to remove dir {path:?}' in sftp backend")
                    });
                }

                sftp.unlink(path)
                    .or_else(|_| sftp.rmdir(path))
                    .with_context(|| {
                        format!("Failed to remove item {path:?}' in sftp backend (not file/dir)")
                    })
            }
            Err(e) => Err(e).context(format!(
                "Failed to stat path {path:?}' for recursive removal"
            )),
        }
    }

    fn rename_internal(&self, sftp: &Sftp, from: &Path, to: &Path) -> Result<()> {
        let full_from = self.full_path(from);
        let full_to = self.full_path(to);

        // Remove destination if it already exists. This is a workaround for
        // SFTP servers where the OVERWRITE flag is not honoured.
        if sftp.stat(&full_to).is_ok() {
            sftp.unlink(&full_to)?;
        }
        sftp.rename(&full_from, &full_to, Some(RenameFlags::all()))?;

        Ok(())
    }

    fn set_read_only_internal(&self, sftp: &Sftp, path: &Path) -> Result<()> {
        let full_path = self.full_path(path);

        let mut stat = sftp.stat(&full_path)?;

        if let Some(perm) = stat.perm.as_mut() {
            *perm &= !0o222;
        } else {
            return Ok(());
        }

        sftp.setstat(&full_path, stat)?;

        Ok(())
    }
}

impl StorageBackend for SftpBackend {
    fn create(&self) -> Result<()> {
        let conn = self.pool.get()?;
        self.create_dir_all_internal(&self.base_path, conn.sftp())
    }

    fn root_exists(&self) -> bool {
        let conn = self.pool.get().unwrap();
        self.exists_exact(&self.base_path, conn.sftp())
    }

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

        file.seek(seek_from)
            .with_context(|| format!("Failed to seek to offset {offset} in sftp file {path:?}"))?;

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

    fn write(&self, handle: &Handle, contents: &[u8]) -> Result<()> {
        let path = handle.path;
        let tmp_path = path.with_extension(REPO_TMP_EXTENSION);
        let full_tmp_path = self.full_path(&tmp_path);
        let conn = self.pool.get()?;
        let sftp = conn.sftp();

        // Write to a tmp path
        let mut file = sftp
            .create(&full_tmp_path)
            .with_context(|| format!("Failed to create file for writing: {tmp_path:?}"))?;
        if file.write_all(contents).is_err() {
            // If error, try creating the parent directory first and try again.
            let parent_dir = path.parent().with_context(|| {
                format!(
                    "Could not create parent directory for '{}' in sftp backend",
                    path.display()
                )
            })?;
            let _ = self.create_dir(parent_dir);

            file.write_all(contents)
                .with_context(|| format!("Failed to write to file: {tmp_path:?}"))?;
        }

        // Rename to the final path reusing the connection.
        self.rename_internal(sftp, &tmp_path, path)
            .with_context(|| {
                format!("Failed to rename {tmp_path:?}' to {path:?}' after write in sftp backend")
            })?;

        let _ = self.set_read_only_internal(sftp, path);

        Ok(())
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
            .map(|(path, _meta)| path.strip_prefix(&self.base_path).unwrap().to_path_buf())
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
        match conn.sftp().lstat(&full_path) {
            Ok(stat) => stat.is_file(),
            Err(_) => false,
        }
    }

    fn is_dir(&self, path: &Path) -> bool {
        let full_path = self.full_path(path);

        let conn = self.pool.get().unwrap();
        match conn.sftp().lstat(&full_path) {
            Ok(stat) => stat.is_dir(),
            Err(_) => false,
        }
    }

    fn lstat(&self, path: &Path) -> Result<super::NodeAttr> {
        let full_path = self.full_path(path);

        let conn = self.pool.get().unwrap();
        let meta = conn.sftp().lstat(&full_path)?;

        Ok(super::NodeAttr {
            size: meta.size,
            uid: meta.uid,
            gid: meta.gid,
            perm: meta.perm,
            atime: meta
                .atime
                .map(|time| UNIX_EPOCH + Duration::from_secs(time)),
            mtime: meta
                .mtime
                .map(|time| UNIX_EPOCH + Duration::from_secs(time)),
        })
    }
}
