use std::{
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::{Duration, UNIX_EPOCH},
};

use anyhow::{Context, Result, anyhow, bail};
use async_trait::async_trait;
use russh::{
    client,
    keys::{PrivateKeyWithHashAlg, PublicKey, load_secret_key},
};
use russh_sftp::client::SftpSession;
use russh_sftp::protocol::{FileAttributes, OpenFlags};
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};
use tokio::sync::Mutex;
use tokio::time::timeout;

use crate::{
    backend::{
        Handle, NodeAttr, RetryOptions, StorageBackend, WriteContents, retry, set_readonly_mode,
    },
    repository::repo::REPO_TMP_EXTENSION,
    ui,
};

/// Maximum number of concurrent SFTP connections to maintain.
/// Multiple connections provide better throughput over high-latency networks
/// by using multiple TCP streams.
const MAX_SFTP_CONNECTIONS: usize = 3;
const CONNECTION_TIMEOUT: Duration = Duration::from_secs(30);

/// Errors specific to the SFTP backend.
#[derive(Debug)]
pub enum SftpError {
    AuthenticationFailed(String),
}

impl std::fmt::Display for SftpError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SftpError::AuthenticationFailed(user) => {
                write!(f, "SFTP authentication failed for user: {}", user)
            }
        }
    }
}

impl std::error::Error for SftpError {}

/// Supported authentication methods for the SFTP backend.
#[derive(Clone, Debug)]
pub enum AuthMethod {
    /// Standard username/password authentication.
    Password(String),
    /// Public key authentication using a private key file.
    PubKey {
        private_key: PathBuf,
        passphrase: Option<String>,
    },
}

#[derive(Clone)]
struct MapacheSftpHandler;

impl client::Handler for MapacheSftpHandler {
    type Error = anyhow::Error;

    async fn check_server_key(
        &mut self,
        _server_public_key: &PublicKey,
    ) -> Result<bool, Self::Error> {
        Ok(true)
    }
}

/// A wrapper around an SFTP session to make it easier to use.
pub struct SftpConnection {
    sftp: SftpSession,
    // We keep the session alive by holding it here.
    _session: client::Handle<MapacheSftpHandler>,
}

impl SftpConnection {
    pub async fn new(
        username: &str,
        host: &str,
        port: u16,
        auth_method: &AuthMethod,
    ) -> Result<Self> {
        let config = Arc::new(client::Config {
            keepalive_interval: Some(Duration::from_secs(30)),
            ..Default::default()
        });

        let mut session = client::connect(config, (host, port), MapacheSftpHandler)
            .await
            .context("Failed to connect to SFTP server")?;

        let auth_res = match auth_method {
            AuthMethod::Password(password) => {
                session.authenticate_password(username, password).await?
            }
            AuthMethod::PubKey {
                private_key,
                passphrase,
            } => {
                let key = load_secret_key(private_key, passphrase.as_deref())
                    .context("Failed to load private key")?;
                let pk = PrivateKeyWithHashAlg::new(Arc::new(key), None);
                session.authenticate_publickey(username, pk).await?
            }
        };

        if !auth_res.success() {
            return Err(anyhow!(SftpError::AuthenticationFailed(
                username.to_string()
            )));
        }

        let channel = session
            .channel_open_session()
            .await
            .context("Failed to open SSH channel")?;
        channel
            .request_subsystem(true, "sftp")
            .await
            .context("Failed to request SFTP subsystem")?;

        let sftp = SftpSession::new(channel.into_stream())
            .await
            .context("Failed to initialize SFTP session")?;

        Ok(Self {
            sftp,
            _session: session,
        })
    }

    pub fn is_alive(&self) -> bool {
        !self._session.is_closed()
    }
}

#[allow(dead_code)]
trait FileAttributesExt {
    fn is_file(&self) -> bool;
    fn is_dir(&self) -> bool;
}

impl FileAttributesExt for FileAttributes {
    fn is_file(&self) -> bool {
        self.permissions.is_some_and(|p| (p & 0o170000) == 0o100000)
    }
    fn is_dir(&self) -> bool {
        self.permissions.is_some_and(|p| (p & 0o170000) == 0o040000)
    }
}

/// A high-performance collection of SFTP connections.
///
/// Unlike a traditional pool that limits concurrency by popping connections,
/// this manager allows multiple concurrent operations on every connection,
/// maximizing the asynchronous capabilities of russh-sftp.
pub struct SftpConnectionManager {
    connections: Vec<Mutex<Arc<SftpConnection>>>,
    next_index: AtomicUsize,
    username: String,
    host: String,
    port: u16,
    auth_method: AuthMethod,
}

impl SftpConnectionManager {
    pub async fn new(
        capacity: usize,
        username: String,
        host: String,
        port: u16,
        auth_method: AuthMethod,
    ) -> Result<Self> {
        let mut connections = Vec::new();

        // Establish the first connection sequentially to verify credentials.
        // This prevents multiple failed authentication attempts if the password is wrong.
        let first_conn = timeout(
            CONNECTION_TIMEOUT,
            SftpConnection::new(&username, &host, port, &auth_method),
        )
        .await
        .context("Timeout establishing initial SFTP connection")?
        .context("Failed to establish initial SFTP connection")?;

        connections.push(Mutex::new(Arc::new(first_conn)));

        // Establish the remaining connections in parallel.
        if capacity > 1 {
            let mut join_handles = Vec::new();
            let auth_method_arc = Arc::new(auth_method.clone());

            for _ in 1..capacity {
                let u = username.clone();
                let h = host.clone();
                let am = auth_method_arc.clone();
                join_handles.push(tokio::spawn(async move {
                    timeout(CONNECTION_TIMEOUT, SftpConnection::new(&u, &h, port, &am)).await
                }));
            }

            for handle in join_handles {
                match handle.await {
                    Ok(Ok(Ok(conn))) => connections.push(Mutex::new(Arc::new(conn))),
                    Ok(Ok(Err(e))) => {
                        ui::cli::warning!("Failed to establish auxiliary SFTP connection: {}", e);
                    }
                    Ok(Err(_)) => {
                        ui::cli::warning!("Timeout establishing auxiliary SFTP connection");
                    }
                    Err(e) => {
                        ui::cli::warning!(
                            "Task panicked establishing auxiliary SFTP connection: {}",
                            e
                        );
                    }
                }
            }
        }

        Ok(Self {
            connections,
            next_index: AtomicUsize::new(0),
            username,
            host,
            port,
            auth_method,
        })
    }

    /// Picks a connection using a round-robin strategy and reconnects if dead.
    /// Since SftpSession is thread-safe and async, we can use the same
    /// connection for many concurrent requests.
    pub async fn get_connection(&self) -> Result<Arc<SftpConnection>> {
        let idx = self.next_index.fetch_add(1, Ordering::Relaxed) % self.connections.len();
        let mut guard = self.connections[idx].lock().await;

        if !guard.is_alive() {
            // Reconnect
            let conn = timeout(
                CONNECTION_TIMEOUT,
                SftpConnection::new(&self.username, &self.host, self.port, &self.auth_method),
            )
            .await
            .context("Timeout re-establishing SFTP connection")?
            .context("Failed to re-establish SFTP connection")?;
            *guard = Arc::new(conn);
        }

        Ok(guard.clone())
    }
}

/// Storage backend implementation for SFTP using russh-sftp.
pub struct SftpBackend {
    base_path: PathBuf,
    manager: Arc<SftpConnectionManager>,
    retry_opts: RetryOptions,
}

impl SftpBackend {
    pub async fn new(
        base_path: PathBuf,
        username: String,
        host: String,
        port: u16,
        auth_method: AuthMethod,
    ) -> Result<Self> {
        let manager = Arc::new(
            SftpConnectionManager::new(MAX_SFTP_CONNECTIONS, username, host, port, auth_method)
                .await?,
        );

        let retry_opts = RetryOptions {
            max_attempts: 4,
            base_delay: Duration::from_millis(200),
            request_timeout: Duration::from_secs(60),
        };

        Ok(Self {
            base_path,
            manager,
            retry_opts,
        })
    }

    /// Wraps an async operation with exponential backoff retries and timeouts.
    async fn retry<T, F, Fut>(&self, op: F) -> Result<T>
    where
        F: FnMut() -> Fut,
        Fut: std::future::Future<Output = Result<T>>,
    {
        retry("SFTP", &self.retry_opts, op).await
    }

    #[inline]
    fn full_path(&self, path: &Path) -> PathBuf {
        self.base_path.join(path)
    }

    async fn exists_exact_full(sftp: &SftpSession, full_path: &Path) -> bool {
        sftp.metadata(full_path.to_string_lossy()).await.is_ok()
    }

    async fn create_dir_all_full(sftp: &SftpSession, full_path: &Path) -> Result<()> {
        let path_str = full_path.to_string_lossy().to_string();
        if Self::exists_exact_full(sftp, full_path).await {
            let meta = sftp.metadata(&path_str).await?;
            if meta.is_dir() {
                return Ok(());
            }
            bail!("Path {:?} exists but is not a directory", full_path);
        }

        if let Some(parent) = full_path.parent()
            && !parent.as_os_str().is_empty()
        {
            Box::pin(Self::create_dir_all_full(sftp, parent)).await?;
        }

        sftp.create_dir(&path_str)
            .await
            .with_context(|| format!("Failed to create directory {:?} in sftp backend", full_path))
    }

    async fn set_readonly_status_full(
        sftp: &SftpSession,
        full_path: &Path,
        readonly: bool,
    ) -> Result<()> {
        let path_str = full_path.to_string_lossy().to_string();
        let mut stat = match sftp.metadata(&path_str).await {
            Ok(s) => s,
            Err(_) if !readonly => return Ok(()),
            Err(e) => return Err(anyhow!(e)).context("Failed to stat remote file"),
        };

        let is_dir = stat.is_dir();
        if let Some(perm) = stat.permissions {
            let new_perm = set_readonly_mode(perm, readonly, is_dir);
            if new_perm != perm {
                stat.permissions = Some(new_perm);
                sftp.set_metadata(&path_str, stat).await?;
            }
        }
        Ok(())
    }

    async fn remove_recursively_full(sftp: &SftpSession, full_path: &Path) -> Result<()> {
        if !Self::exists_exact_full(sftp, full_path).await {
            return Ok(());
        }

        let path_str = full_path.to_string_lossy().to_string();
        let _ = Self::set_readonly_status_full(sftp, full_path, false).await;

        let meta = sftp.symlink_metadata(&path_str).await?;
        if meta.is_file() {
            return sftp
                .remove_file(&path_str)
                .await
                .with_context(|| format!("Failed to remove file {:?} in sftp backend", full_path));
        }

        if meta.is_dir() {
            let entries = sftp.read_dir(&path_str).await?;
            for entry in entries {
                let name = entry.file_name();
                if name == "." || name == ".." {
                    continue;
                }
                let p = full_path.join(name);
                Box::pin(Self::remove_recursively_full(sftp, &p)).await?;
            }
            return sftp.remove_dir(&path_str).await.with_context(|| {
                format!("Failed to remove directory {:?} in sftp backend", full_path)
            });
        }

        // Fallback
        if let Err(e) = sftp.remove_file(&path_str).await
            && let Err(_e2) = sftp.remove_dir(&path_str).await
        {
            return Err(anyhow!(e));
        }
        Ok(())
    }

    async fn rename_full(sftp: &SftpSession, full_from: &Path, full_to: &Path) -> Result<()> {
        let from_str = full_from.to_string_lossy().to_string();
        let to_str = full_to.to_string_lossy().to_string();

        let _ = Self::set_readonly_status_full(sftp, full_from, false).await;

        if Self::exists_exact_full(sftp, full_to).await {
            let _ = Self::set_readonly_status_full(sftp, full_to, false).await;
            let _ = sftp.remove_file(&to_str).await;
        }

        sftp.rename(&from_str, &to_str)
            .await
            .with_context(|| format!("Failed to rename {:?} -> {:?}", full_from, full_to))?;

        let _ = Self::set_readonly_status_full(sftp, full_to, true).await;

        Ok(())
    }
}

#[async_trait]
impl StorageBackend for SftpBackend {
    async fn create(&self) -> Result<()> {
        self.retry(|| async {
            let conn = self.manager.get_connection().await?;
            Self::create_dir_all_full(&conn.sftp, &self.base_path).await
        })
        .await
    }

    async fn path_exists(&self, path: &Path) -> bool {
        let full = self.full_path(path);
        let res = self
            .retry(|| async {
                let conn = self.manager.get_connection().await?;
                Ok(Self::exists_exact_full(&conn.sftp, &full).await)
            })
            .await;

        res.unwrap_or(false)
    }

    async fn read(&self, handle: &Handle, offset: isize, length: usize) -> Result<Vec<u8>> {
        let full = self.full_path(handle.path);

        self.retry(|| async {
            let conn = self.manager.get_connection().await?;
            let path_str = full.to_string_lossy().to_string();

            let mut file = conn.sftp.open(&path_str).await.with_context(|| {
                format!(
                    "Failed to open file {:?} in sftp backend for reading",
                    handle.path
                )
            })?;

            let real_offset = if offset >= 0 {
                offset as u64
            } else {
                let meta = file.metadata().await?;
                let size = meta.size.unwrap_or(0);
                if (offset.unsigned_abs() as u64) > size {
                    0
                } else {
                    size - (offset.unsigned_abs() as u64)
                }
            };

            file.seek(std::io::SeekFrom::Start(real_offset)).await?;

            let mut contents = if length > 0 {
                vec![0u8; length]
            } else {
                Vec::new()
            };

            if length == 0 {
                file.read_to_end(&mut contents).await?;
            } else {
                file.read_exact(&mut contents).await?;
            }

            Ok(contents)
        })
        .await
    }

    async fn write(&self, handle: &Handle, contents: WriteContents<'_>) -> Result<()> {
        let tmp_path = handle.path.with_extension(REPO_TMP_EXTENSION);
        let full_tmp = self.full_path(&tmp_path);
        let full_dst = self.full_path(handle.path);

        self.retry(|| async {
            let conn = self.manager.get_connection().await?;

            // Ensure parent exists
            if let Some(parent) = full_tmp.parent() {
                let _ = Self::create_dir_all_full(&conn.sftp, parent).await;
            }

            // Write tmp
            let mut file = conn
                .sftp
                .open_with_flags(
                    full_tmp.to_string_lossy(),
                    OpenFlags::CREATE | OpenFlags::TRUNCATE | OpenFlags::WRITE,
                )
                .await
                .with_context(|| {
                    format!("Failed to create tmp file {:?} in sftp backend", tmp_path)
                })?;

            file.write_all(&contents).await.with_context(|| {
                format!("Failed to write tmp file {:?} in sftp backend", tmp_path)
            })?;

            file.shutdown().await?;

            // Commit tmp -> dst
            Self::rename_full(&conn.sftp, &full_tmp, &full_dst).await
        })
        .await
    }

    async fn rename(&self, from: &Path, to: &Path) -> Result<()> {
        let full_from = self.full_path(from);
        let full_to = self.full_path(to);

        self.retry(|| async {
            let conn = self.manager.get_connection().await?;
            Self::rename_full(&conn.sftp, &full_from, &full_to).await
        })
        .await
    }

    async fn list_dir(&self, path: &Path) -> Result<Vec<PathBuf>> {
        let full = self.full_path(path);

        self.retry(|| async {
            let conn = self.manager.get_connection().await?;

            let entries = conn
                .sftp
                .read_dir(full.to_string_lossy())
                .await
                .with_context(|| format!("Could not list directory {:?} in sftp backend", path))?;

            let mut out = Vec::new();
            for entry in entries {
                let name = entry.file_name();
                if name == "." || name == ".." {
                    continue;
                }
                out.push(path.join(name));
            }

            Ok(out)
        })
        .await
    }

    async fn create_dir(&self, path: &Path) -> Result<()> {
        let full = self.full_path(path);

        self.retry(|| async {
            let conn = self.manager.get_connection().await?;
            Self::create_dir_all_full(&conn.sftp, &full).await
        })
        .await
    }

    async fn remove(&self, file_path: &Path) -> Result<()> {
        let full = self.full_path(file_path);

        self.retry(|| async {
            let conn = self.manager.get_connection().await?;
            Self::remove_recursively_full(&conn.sftp, &full).await
        })
        .await
    }

    async fn is_file(&self, path: &Path) -> bool {
        let full = self.full_path(path);
        let res = self
            .retry(|| async {
                let conn = self.manager.get_connection().await?;
                let meta = conn.sftp.metadata(full.to_string_lossy()).await;
                Ok(meta.map(|s| s.is_file()).unwrap_or(false))
            })
            .await;

        res.unwrap_or(false)
    }

    async fn is_dir(&self, path: &Path) -> bool {
        let full = self.full_path(path);
        let res = self
            .retry(|| async {
                let conn = self.manager.get_connection().await?;
                let meta = conn.sftp.metadata(full.to_string_lossy()).await;
                Ok(meta.map(|s| s.is_dir()).unwrap_or(false))
            })
            .await;

        res.unwrap_or(false)
    }

    async fn lstat(&self, path: &Path) -> Result<NodeAttr> {
        let full = self.full_path(path);

        self.retry(|| async {
            let conn = self.manager.get_connection().await?;
            let meta = conn.sftp.symlink_metadata(full.to_string_lossy()).await?;

            let to_system_time = |t: u32| {
                if t == 0 {
                    None
                } else {
                    Some(UNIX_EPOCH + Duration::from_secs(t as u64))
                }
            };

            Ok(NodeAttr {
                size: meta.size,
                uid: meta.uid,
                gid: meta.gid,
                perm: meta.permissions,
                atime: meta.atime.and_then(to_system_time),
                mtime: meta.mtime.and_then(to_system_time),
            })
        })
        .await
    }
}
