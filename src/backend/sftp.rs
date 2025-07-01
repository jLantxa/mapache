// mapache is an incremental backup tool
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

use std::{
    io::{Read, Seek, SeekFrom, Write},
    net::TcpStream,
    path::{Path, PathBuf},
    sync::Arc,
};

use anyhow::{Context, Result, anyhow, bail};
use crossbeam_channel::{Receiver, Sender, bounded};
use ssh2::{RenameFlags, Session, Sftp};

use crate::ui;

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
        let tcp = TcpStream::connect(&addr).with_context(|| "Failed to connect to SFTP server")?;
        let mut session = Session::new().with_context(|| "Failed to create SSH session")?;
        session.set_tcp_stream(tcp);
        session
            .handshake()
            .with_context(|| "Failed to perform SSH handshake")?;

        Self::authenticate(&session, username, auth_method)?;

        session.set_keepalive(true, 30);
        session.set_compress(false);

        let sftp = session
            .sftp()
            .with_context(|| "Failed to create SFTP session")?;
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
                .with_context(|| "Failed to authenticate with password"),
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
                .map_err(|e| {
                    anyhow!(format!(
                        "Failed to authenticate with pubkey: {}",
                        e.to_string()
                    ))
                }),
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
            .with_context(|| "Failed to get connection from pool")?;
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

pub struct SftpBackend {
    repo_path: PathBuf,
    pool: Arc<SftpConnectionPool>,
}

impl SftpBackend {
    pub fn new(
        repo_path: PathBuf,
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

        Ok(Self { repo_path, pool })
    }

    #[inline]
    fn full_path(&self, path: &Path) -> PathBuf {
        self.repo_path.join(path)
    }

    /// Returns true if the exact path given exists (not as a relative path to the backend root).
    fn exists_exact(&self, path: &Path, sftp: &Sftp) -> bool {
        sftp.lstat(path).is_ok()
    }

    /// Creates a directory with the exact path given (not as a relative path to the backend root).
    fn create_dir_exact(&self, path: &Path, sftp: &Sftp) -> Result<()> {
        let stats = sftp.lstat(path);
        if let Ok(stats) = stats {
            if !stats.is_dir() {
                bail!(format!(
                    "Failed to create directory {:?}' in sftp backend. Path exists, but it is not a directory.",
                    path
                ))
            } else {
                Ok(())
            }
        } else {
            sftp.mkdir(path, 0o755)
                .with_context(|| format!("Failed to create directory {path:?}' in sftp backend"))
        }
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
                    "Path {:?} exists but is not a directory",
                    path
                ));
            }
        }

        if let Some(parent) = path.parent() {
            if parent != Path::new("") {
                self.create_dir_all_internal(parent, sftp)?;
            }
        }

        sftp.mkdir(path, 0o755)
            .with_context(|| format!("Failed to create directory {path:?}' in sftp backend"))
    }

    fn remove_dir_all_internal(&self, path: &Path, sftp: &Sftp) -> Result<()> {
        if !self.exists_exact(path, sftp) {
            return Ok(());
        }

        let metadata = sftp
            .lstat(path)
            .with_context(|| format!("Failed to get metadata for path: {path:?}"))?;

        if metadata.is_file() {
            sftp.unlink(path)
                .with_context(|| format!("Failed to remove file {path:?}' in sftp backend"))?;
            return Ok(());
        }

        let entries = sftp
            .readdir(path)
            .with_context(|| format!("Could not list directory {path:?}' in sftp backend"))?;

        for (entry_path, entry_metadata) in entries {
            if entry_metadata.is_dir() {
                self.remove_dir_all_internal(&entry_path, sftp)?;
            } else {
                sftp.unlink(&entry_path).with_context(|| {
                    format!("Failed to remove file {entry_path:?}' in sftp backend")
                })?;
            }
        }

        sftp.rmdir(path)
            .with_context(|| format!("Failed to remove dir {path:?}' in sftp backend"))
    }
}

impl StorageBackend for SftpBackend {
    fn create(&self) -> Result<()> {
        let conn = self.pool.get()?;
        self.create_dir_all_internal(&self.repo_path, conn.sftp())
    }

    fn root_exists(&self) -> bool {
        let conn = self.pool.get().unwrap();
        self.exists_exact(&self.repo_path, conn.sftp())
    }

    fn read(&self, path: &Path) -> Result<Vec<u8>> {
        let full_path = self.full_path(path);

        let conn = self.pool.get()?;
        let mut file = conn.sftp().open(full_path).with_context(|| {
            format!("Failed to open file {path:?}\' in sftp backend for reading")
        })?;
        let mut contents = Vec::new();
        file.read_to_end(&mut contents)
            .with_context(|| format!("Failed to read file {path:?}\' in sftp backend"))?;
        Ok(contents)
    }

    fn seek_read(&self, path: &Path, offset: u64, length: u64) -> Result<Vec<u8>> {
        let full_path = self.full_path(path);

        let conn = self.pool.get()?;
        let mut file = conn.sftp().open(full_path).with_context(|| {
            format!("Failed to open file {path:?}\' in sftp backend for ranged reading")
        })?;

        // Read into preallocated vector
        let mut contents = vec![0; length as usize];

        if offset > 0 {
            let _ = file.seek(SeekFrom::Start(offset));
        }

        file.read_exact(&mut contents)
            .with_context(|| format!("Failed to seek read file {path:?}\' in sftp backend"))?;
        Ok(contents)
    }

    fn write(&self, path: &Path, contents: &[u8]) -> Result<()> {
        let full_path = self.full_path(path);

        let conn = self.pool.get()?;
        let mut file = conn
            .sftp()
            .create(&full_path)
            .with_context(|| format!("Failed to create file for writing: {path:?}"))?;
        file.write_all(contents)
            .with_context(|| format!("Failed to write to file: {path:?}"))?;
        Ok(())
    }

    fn rename(&self, from: &Path, to: &Path) -> Result<()> {
        let full_path_from = self.full_path(from);
        let full_path_from_to = self.full_path(to);

        let conn = self.pool.get()?;
        conn.sftp()
            .rename(
                &full_path_from,
                &full_path_from_to,
                Some(RenameFlags::all()),
            )
            .with_context(|| format!("Failed to rename {from:?}\' to {to:?}\' in sftp backend"))
    }

    fn remove_file(&self, file_path: &Path) -> Result<()> {
        let full_path = self.full_path(file_path);

        let conn = self.pool.get()?;
        conn.sftp()
            .unlink(&full_path)
            .with_context(|| format!("Failed to remove file {file_path:?}\' in sftp backend"))
    }

    fn create_dir(&self, path: &Path) -> Result<()> {
        let full_path = self.full_path(path);

        let conn = self.pool.get()?;
        self.create_dir_exact(&full_path, conn.sftp())
    }

    #[inline]
    fn create_dir_all(&self, path: &Path) -> Result<()> {
        let full_path = self.full_path(path);

        let conn = self.pool.get()?;
        self.create_dir_all_internal(&full_path, conn.sftp())
    }

    fn read_dir(&self, path: &Path) -> Result<Vec<PathBuf>> {
        let full_path = self.full_path(path);

        let conn = self.pool.get()?;
        let entries = conn
            .sftp()
            .readdir(full_path)
            .with_context(|| format!("Could not list directory {path:?}\' in sftp backend"))?;

        Ok(entries
            .iter()
            .map(|(path, _meta)| path.strip_prefix(&self.repo_path).unwrap().to_path_buf())
            .collect())
    }

    fn remove_dir(&self, path: &Path) -> Result<()> {
        let full_path = self.full_path(path);

        let conn = self.pool.get()?;
        conn.sftp()
            .rmdir(&full_path)
            .with_context(|| format!("Failed to remove dir {path:?}\' in sftp backend"))
    }

    fn remove_dir_all(&self, path: &Path) -> Result<()> {
        let full_path = self.full_path(path);

        let conn = self.pool.get()?;
        self.remove_dir_all_internal(&full_path, conn.sftp())
    }

    fn exists(&self, path: &Path) -> bool {
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
}
