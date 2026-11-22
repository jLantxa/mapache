use std::{
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::{Duration, UNIX_EPOCH},
};

use async_trait::async_trait;
use russh::{
    client,
    keys::{PrivateKeyWithHashAlg, PublicKeyOrCertificate, load_secret_key},
};
use russh_sftp::{
    client::{SftpSession, error::Error as SftpClientError},
    protocol::{FileAttributes, OpenFlags, StatusCode},
};
use tokio::{
    io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt, BufWriter},
    sync::Mutex,
    time::timeout,
};
use zeroize::Zeroizing;

use crate::{
    backend::{
        BackendNode, BackendOptions, Handle, NodeAttr, RetryOptions, StorageBackend, WriteContents,
        limiter::{RateLimiter, ThrottledReader, ThrottledWriter},
        retry_with, set_readonly_mode,
    },
    common::error::{MapacheError, Result},
    repository::repo::REPO_TMP_EXTENSION,
    ui::{self, cli::color::Colorize},
    utils::{self, size},
};

/// Maximum number of concurrent SFTP connections to maintain.
/// Multiple connections provide better throughput over high-latency networks
/// by using multiple TCP streams.
const MAX_SFTP_CONNECTIONS: usize = 5;
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
                write!(f, "sftp authentication failed for user: {}", user)
            }
        }
    }
}

impl std::error::Error for SftpError {}

impl From<russh::Error> for MapacheError {
    fn from(e: russh::Error) -> Self {
        MapacheError::Backend(e.to_string())
    }
}

/// Supported authentication methods for the SFTP backend.
#[derive(Clone, Debug)]
pub enum AuthMethod {
    /// Standard username/password authentication.
    Password(Zeroizing<String>),
    /// Public key authentication using a private key file.
    PubKey {
        private_key: PathBuf,
        passphrase: Option<Zeroizing<String>>,
    },
}

#[derive(Clone)]
struct MapacheSftpHandler {
    host: String,
    port: u16,
    known_hosts_path: Option<PathBuf>,
}

impl client::Handler for MapacheSftpHandler {
    type Error = MapacheError;

    async fn check_server_key(
        &mut self,
        server_public_key: &PublicKeyOrCertificate,
    ) -> std::result::Result<bool, Self::Error> {
        let server_public_key = server_public_key.public_key();
        const SSH_KNOWN_HOSTS_PATH: &str = ".ssh/known_hosts";

        let known_hosts_path = if let Some(ref path) = self.known_hosts_path {
            path.clone()
        } else {
            // Logic for default known_hosts path
            let mut path = None;
            if let Ok(home) = std::env::var("HOME") {
                path = Some(PathBuf::from(home).join(SSH_KNOWN_HOSTS_PATH));
            } else if let Ok(user_profile) = std::env::var("USERPROFILE") {
                path = Some(PathBuf::from(user_profile).join(SSH_KNOWN_HOSTS_PATH));
            }

            // If the directory exists but the file doesn't, we can create it later
            path.unwrap_or_else(|| PathBuf::from(SSH_KNOWN_HOSTS_PATH))
        };

        let host_port = if self.port == 22 {
            self.host.clone()
        } else {
            format!("[{}]:{}", self.host, self.port)
        };

        match verify_known_host(&known_hosts_path, &self.host, self.port, &server_public_key)? {
            HostKeyVerification::Known => return Ok(true),
            HostKeyVerification::Changed => {
                return Err(MapacheError::Backend(format!(
                    "security alert: Remote host identification has changed for '{}'.\n\
                     This could indicate a man-in-the-middle attack or a legitimate host key rotation.\n\
                     The host key fingerprint received was {}.\n\
                     Please verify the server's key or update your known_hosts file at {:?}.",
                    self.host,
                    utils::base64::encode(&server_public_key.to_bytes().unwrap_or_default()),
                    known_hosts_path
                )));
            }
            HostKeyVerification::Unknown => {}
        }

        // Host not found in known_hosts. Prompt the user.
        let fingerprint = utils::base64::encode(&server_public_key.to_bytes().unwrap_or_default());
        ui::cli::log!(
            "The authenticity of host '{}' can't be established.",
            host_port.cyan()
        );
        ui::cli::log!(
            "{} key fingerprint is {}.",
            server_public_key.algorithm().as_str(),
            fingerprint.yellow()
        );

        let prompt = "Are you sure you want to continue connecting (yes/no/[fingerprint])?";
        let input = ui::cli::request_input(prompt)
            .map_err(|e| MapacheError::Config(format!("failed to read user input: {e}")))?
            .unwrap_or_default()
            .to_lowercase();

        if input == "yes" || input == fingerprint.to_lowercase() {
            // Save to known_hosts
            ui::cli::warning!(
                "Permanently added '{}' ({}) to the list of known hosts.",
                self.host,
                server_public_key.algorithm().as_str()
            );

            if let Some(parent) = known_hosts_path.parent() {
                std::fs::create_dir_all(parent)?;
            }

            let mut file = std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(&known_hosts_path)?;

            use std::io::Write;
            let key_base64 = utils::base64::encode(
                &server_public_key
                    .to_bytes()
                    .map_err(|e| MapacheError::Backend(e.to_string()))?,
            );
            writeln!(
                file,
                "{} {} {}",
                host_port,
                server_public_key.algorithm().as_str(),
                key_base64
            )?;

            Ok(true)
        } else {
            Err(MapacheError::Backend(
                "host key verification failed.".to_string(),
            ))?
        }
    }
}

/// Result of comparing a server key with the recorded host entries.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum HostKeyVerification {
    Known,
    Changed,
    Unknown,
}

/// Check a server key against literal and OpenSSH-hashed `known_hosts` entries.
fn verify_known_host(
    path: &Path,
    host: &str,
    port: u16,
    server_public_key: &russh::keys::PublicKey,
) -> Result<HostKeyVerification> {
    let content = if path.exists() {
        std::fs::read_to_string(path)?
    } else {
        String::new()
    };

    let host_port = if port == 22 {
        host.to_string()
    } else {
        format!("[{host}]:{port}")
    };

    let mut found_host = false;
    for line in content.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }

        let parts: Vec<&str> = line.split_whitespace().collect();
        if parts.len() < 3 {
            continue;
        }

        let hosts = parts[0];
        let host_match = hosts.split(',').any(|h| h == host || h == host_port);

        if host_match {
            found_host = true;
            let key_type = parts[1];
            let key_base64 = parts[2];

            if let Ok(key_bytes) = utils::base64::decode(key_base64)
                && server_public_key.algorithm().as_str() == key_type
                && server_public_key.to_bytes().as_deref() == Ok(&key_bytes[..])
            {
                return Ok(HostKeyVerification::Known);
            }
        }
    }

    if found_host {
        return Ok(HostKeyVerification::Changed);
    }

    // The SSH library verifies hashed entries for the exact host token.
    let recorded = russh::keys::known_hosts::known_host_keys_path(host, port, path)
        .map_err(|e| MapacheError::Backend(format!("failed to check known_hosts: {e}")))?;

    if recorded.is_empty() {
        Ok(HostKeyVerification::Unknown)
    } else if recorded.iter().any(|(_, key)| key == server_public_key) {
        Ok(HostKeyVerification::Known)
    } else {
        Ok(HostKeyVerification::Changed)
    }
}

#[cfg(test)]
mod known_hosts_tests {
    use std::io::Write;

    use russh::keys::parse_public_key_base64;

    use super::*;

    const HASHED_EXAMPLE_COM: &str = concat!(
        "|1|O33ESRMWPVkMYIwJ1Uw+n877jTo=|nuuC5vEqXlEZ/8BXQR7m619W6Ak= ",
        "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAILIG2T/B0l0gaqj3puu510tu9N1OkQ4znY3LYuEm5zCF"
    );
    const MATCHING_KEY: &str =
        "AAAAC3NzaC1lZDI1NTE5AAAAILIG2T/B0l0gaqj3puu510tu9N1OkQ4znY3LYuEm5zCF";

    fn known_hosts_file(entries: &[&str]) -> tempfile::NamedTempFile {
        let mut file = tempfile::NamedTempFile::new().unwrap();
        for entry in entries {
            writeln!(file, "{entry}").unwrap();
        }
        file.flush().unwrap();
        file
    }

    #[test]
    fn hashed_entry_matches_recorded_key() {
        let file = known_hosts_file(&[HASHED_EXAMPLE_COM]);
        let key = parse_public_key_base64(MATCHING_KEY).unwrap();

        assert_eq!(
            verify_known_host(file.path(), "example.com", 22, &key).unwrap(),
            HostKeyVerification::Known
        );
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
        known_hosts: Option<PathBuf>,
        auth_method: &AuthMethod,
    ) -> Result<Self> {
        let config = Arc::new(client::Config {
            keepalive_interval: Some(Duration::from_secs(30)),
            ..Default::default()
        });

        let handler = MapacheSftpHandler {
            host: host.to_string(),
            port,
            known_hosts_path: known_hosts,
        };

        let mut session = client::connect(config, (host, port), handler)
            .await
            .map_err(|e| match &e {
                MapacheError::Auth(_) | MapacheError::Backend(_) => e,
                _ => MapacheError::Backend(format!("failed to connect to SFTP server: {}", e)),
            })?;

        let auth_res = match auth_method {
            AuthMethod::Password(password) => session
                .authenticate_password(username, password.as_str())
                .await
                .map_err(|e| {
                    MapacheError::Backend(format!("ssh password authentication failed: {}", e))
                })?,
            AuthMethod::PubKey {
                private_key,
                passphrase,
            } => {
                let key = load_secret_key(private_key, passphrase.as_ref().map(|p| p.as_str()))
                    .map_err(|e| {
                        MapacheError::Backend(format!("failed to load private key: {}", e))
                    })?;
                let pk = PrivateKeyWithHashAlg::new(Arc::new(key), None);
                session
                    .authenticate_publickey(username, pk)
                    .await
                    .map_err(|e| {
                        MapacheError::Backend(format!(
                            "ssh public key authentication failed: {}",
                            e
                        ))
                    })?
            }
        };

        if !auth_res.success() {
            let user = username.to_string();
            return Err(MapacheError::Auth(format!(
                "sftp authentication failed for user: {}",
                user
            )));
        }

        let channel = session
            .channel_open_session()
            .await
            .map_err(|e| MapacheError::Backend(format!("failed to open SSH channel: {}", e)))?;
        channel.request_subsystem(true, "sftp").await.map_err(|e| {
            MapacheError::Backend(format!("failed to request SFTP subsystem: {}", e))
        })?;

        let sftp = SftpSession::new(channel.into_stream()).await.map_err(|e| {
            MapacheError::Backend(format!("failed to initialize SFTP session: {}", e))
        })?;

        Ok(Self {
            sftp,
            _session: session,
        })
    }

    pub fn is_alive(&self) -> bool {
        !self._session.is_closed()
    }
}

trait FileAttributesExt {
    fn is_file(&self) -> bool;
}

impl FileAttributesExt for FileAttributes {
    fn is_file(&self) -> bool {
        // Many SFTP servers do not report POSIX mode bits at all, in which case
        // no S_IFREG bit is available. Treat anything that is not a directory
        // as a file so such servers (e.g. Windows OpenSSH) classify correctly.
        !self.is_dir()
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
    known_hosts: Option<PathBuf>,
    auth_method: AuthMethod,
}

impl SftpConnectionManager {
    pub async fn new(
        capacity: usize,
        username: String,
        host: String,
        port: u16,
        known_hosts: Option<PathBuf>,
        auth_method: AuthMethod,
    ) -> Result<Self> {
        let mut connections = Vec::new();

        // Establish the first connection sequentially to verify credentials.
        // This prevents multiple failed authentication attempts if the password is wrong.
        let first_conn = timeout(
            CONNECTION_TIMEOUT,
            SftpConnection::new(&username, &host, port, known_hosts.clone(), &auth_method),
        )
        .await
        .map_err(|e| {
            MapacheError::Backend(format!(
                "timeout establishing initial SFTP connection: {}",
                e
            ))
        })?
        .map_err(|e| match &e {
            MapacheError::Auth(_) | MapacheError::Backend(_) => e,
            _ => MapacheError::Backend(format!(
                "failed to establish initial SFTP connection: {}",
                e
            )),
        })?;

        connections.push(Mutex::new(Arc::new(first_conn)));

        // Establish the remaining connections in parallel.
        if capacity > 1 {
            let mut join_handles = Vec::new();
            let auth_method_arc = Arc::new(auth_method.clone());
            let known_hosts_arc = Arc::new(known_hosts.clone());

            for _ in 1..capacity {
                let u = username.clone();
                let h = host.clone();
                let am = auth_method_arc.clone();
                let kh = known_hosts_arc.clone();
                join_handles.push(tokio::spawn(async move {
                    timeout(
                        CONNECTION_TIMEOUT,
                        SftpConnection::new(&u, &h, port, (*kh).clone(), &am),
                    )
                    .await
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
            known_hosts,
            auth_method,
        })
    }

    /// Picks a connection via round-robin, reconnecting dead ones.
    /// Connections are scanned without blocking; a task that finds a busy slot
    /// simply moves on to the next one, so an in-flight reconnect (up to
    /// `CONNECTION_TIMEOUT`) does not stall every other operation.
    pub async fn get_connection(&self) -> Result<Arc<SftpConnection>> {
        let n = self.connections.len();
        let start = self.next_index.fetch_add(1, Ordering::Relaxed) % n;

        for step in 0..n {
            let conn = &self.connections[(start + step) % n];
            if let Ok(mut guard) = conn.try_lock() {
                if guard.is_alive() {
                    return Ok(Arc::clone(&guard));
                }
                // Dead connection: reconnect while holding its lock so that a
                // single task performs the handshake for every waiter.
                let new_conn = Self::connect_with_timeout(
                    &self.username,
                    &self.host,
                    self.port,
                    self.known_hosts.clone(),
                    &self.auth_method,
                )
                .await?;
                *guard = Arc::new(new_conn);
                return Ok(Arc::clone(&guard));
            }
            // Slot is busy (in use or being reconnected): try the next one.
        }

        // Everything is busy: block on the next slot in the rotation.
        let conn = &self.connections[(start + 1) % n];
        let mut guard = conn.lock().await;
        if !guard.is_alive() {
            let new_conn = Self::connect_with_timeout(
                &self.username,
                &self.host,
                self.port,
                self.known_hosts.clone(),
                &self.auth_method,
            )
            .await?;
            *guard = Arc::new(new_conn);
        }
        Ok(Arc::clone(&guard))
    }

    async fn connect_with_timeout(
        username: &str,
        host: &str,
        port: u16,
        known_hosts: Option<PathBuf>,
        auth_method: &AuthMethod,
    ) -> Result<SftpConnection> {
        timeout(
            CONNECTION_TIMEOUT,
            SftpConnection::new(username, host, port, known_hosts, auth_method),
        )
        .await
        .map_err(|e| {
            MapacheError::Backend(format!("timeout re-establishing SFTP connection: {}", e))
        })?
        .map_err(|e| {
            let detail = match &e {
                MapacheError::Backend(s) => s.clone(),
                _ => e.to_string(),
            };
            MapacheError::Backend(format!(
                "failed to re-establish SFTP connection: {}",
                detail
            ))
        })
    }
}

/// Storage backend implementation for SFTP using russh-sftp.
pub struct SftpBackend {
    base_path: PathBuf,
    manager: Arc<SftpConnectionManager>,
    retry_opts: RetryOptions,
    upload_limiter: Option<Arc<RateLimiter>>,
    download_limiter: Option<Arc<RateLimiter>>,
}

impl SftpBackend {
    pub async fn new(
        base_path: PathBuf,
        username: String,
        host: String,
        port: u16,
        auth_method: AuthMethod,
        opts: &BackendOptions,
    ) -> Result<Self> {
        let manager = Arc::new(
            SftpConnectionManager::new(
                MAX_SFTP_CONNECTIONS,
                username,
                host,
                port,
                opts.ssh_known_hosts.clone(),
                auth_method,
            )
            .await?,
        );

        let retry_opts = RetryOptions {
            max_attempts: 4,
            base_delay: Duration::from_millis(200),
            request_timeout: Duration::from_secs(300),
        };

        Ok(Self {
            base_path,
            manager,
            retry_opts,
            upload_limiter: opts.limit_upload.map(|l| Arc::new(RateLimiter::new(l))),
            download_limiter: opts.limit_download.map(|l| Arc::new(RateLimiter::new(l))),
        })
    }

    /// Wraps an async operation with exponential backoff retries and timeouts.
    /// Permanent failures are surfaced immediately: retrying a rename whose
    /// rollback failed could otherwise report success over an inconsistent
    /// destination.
    async fn retry<T, F, Fut>(&self, op: F) -> Result<T>
    where
        F: FnMut() -> Fut,
        Fut: std::future::Future<Output = Result<T>>,
    {
        retry_with("SFTP", &self.retry_opts, op, |e| {
            !matches!(e, MapacheError::BackendPermanent(_))
        })
        .await
    }

    fn full_path(&self, path: &Path) -> Result<PathBuf> {
        super::join_base_path(&self.base_path, path)
    }

    /// Rejects symlinks below the trusted configured base.
    async fn check_no_symlinks(sftp: &SftpSession, base: &Path, full: &Path) -> Result<()> {
        let relative = full.strip_prefix(base).map_err(|_| {
            MapacheError::Backend(format!(
                "path {:?} is outside the repository base {:?}",
                full, base
            ))
        })?;
        let components: Vec<_> = relative.components().collect();
        let mut current = base.to_path_buf();
        for (index, component) in components.iter().enumerate() {
            current.push(component);
            let metadata = match sftp.symlink_metadata(current.to_string_lossy()).await {
                Ok(metadata) => metadata,
                Err(SftpClientError::Status(status))
                    if status.status_code == StatusCode::NoSuchFile =>
                {
                    break;
                }
                Err(e) => {
                    return Err(MapacheError::Backend(format!(
                        "failed to inspect repository path {:?}: {}",
                        full, e
                    )));
                }
            };
            let kind = metadata.file_type();
            if kind.is_symlink() {
                return Err(MapacheError::Backend(format!(
                    "repository path {:?} traverses a symlink",
                    full
                )));
            }
            if index + 1 < components.len() && !kind.is_dir() {
                return Err(MapacheError::Backend(format!(
                    "repository path {:?} traverses a non-directory",
                    full
                )));
            }
        }
        Ok(())
    }

    /// A collision-resistant name used to set an old destination aside while
    /// a replacement is attempted. Removal is best effort; if it fails, the
    /// path is reported so it can be recovered manually.
    fn backup_path(path: &Path) -> PathBuf {
        let mut os = path.as_os_str().to_os_string();
        os.push(format!(
            ".{:032x}.{REPO_TMP_EXTENSION}",
            rand::random::<u128>()
        ));
        PathBuf::from(os)
    }

    async fn exists_exact_full(sftp: &SftpSession, full_path: &Path) -> bool {
        sftp.metadata(full_path.to_string_lossy()).await.is_ok()
    }

    /// Like [`Self::exists_exact_full`], but does not follow symlinks: for a
    /// rename we care about the directory entry itself.
    async fn symlink_exists_exact_full(sftp: &SftpSession, full_path: &Path) -> bool {
        sftp.symlink_metadata(full_path.to_string_lossy())
            .await
            .is_ok()
    }

    fn rename_error(from: &Path, to: &Path, error: impl std::fmt::Display) -> MapacheError {
        MapacheError::Backend(format!(
            "failed to rename {:?} -> {:?}: {}",
            from, to, error
        ))
    }

    /// Creates `full_path` and its parents without traversing symlinks.
    async fn create_dir_all_checked(
        sftp: &SftpSession,
        base: &Path,
        full_path: &Path,
    ) -> Result<()> {
        if full_path == base {
            return Self::create_dir_all_full(sftp, base).await;
        }

        if let Some(parent) = full_path.parent() {
            Box::pin(Self::create_dir_all_checked(sftp, base, parent)).await?;
        }

        let path_str = full_path.to_string_lossy().to_string();
        match sftp.symlink_metadata(&path_str).await {
            Ok(metadata) if metadata.file_type().is_dir() => Ok(()),
            Ok(_) => Err(MapacheError::Backend(format!(
                "path {:?} exists but is not a real directory",
                full_path
            ))),
            Err(SftpClientError::Status(status))
                if status.status_code == StatusCode::NoSuchFile =>
            {
                sftp.create_dir(&path_str).await.map_err(|e| {
                    MapacheError::Backend(format!(
                        "failed to create directory {:?} in sftp backend: {}",
                        full_path, e
                    ))
                })?;
                if let Err(e) = Self::set_readonly_status_full(sftp, full_path, false).await {
                    tracing::warn!(target: "backend", "sftp: failed to set permissions on new dir {full_path:?}: {e}");
                }
                Ok(())
            }
            Err(e) => Err(MapacheError::Backend(format!(
                "failed to inspect directory {:?} in sftp backend: {}",
                full_path, e
            ))),
        }
    }

    async fn create_dir_all_full(sftp: &SftpSession, full_path: &Path) -> Result<()> {
        let path_str = full_path.to_string_lossy().to_string();
        if Self::exists_exact_full(sftp, full_path).await {
            let meta = sftp
                .metadata(&path_str)
                .await
                .map_err(|e| MapacheError::Backend(e.to_string()))?;
            if meta.is_dir() {
                return Ok(());
            }
            return Err(MapacheError::Backend(format!(
                "path {:?} exists but is not a directory",
                full_path
            )));
        }

        if let Some(parent) = full_path.parent()
            && !parent.as_os_str().is_empty()
        {
            Box::pin(Self::create_dir_all_full(sftp, parent)).await?;
        }

        sftp.create_dir(&path_str).await.map_err(|e| {
            MapacheError::Backend(format!(
                "failed to create directory {:?} in sftp backend: {}",
                full_path, e
            ))
        })?;

        // Set default permissions for new directory (e.g. 0o700)
        if let Err(e) = Self::set_readonly_status_full(sftp, full_path, false).await {
            tracing::warn!(target: "backend", "sftp: failed to set permissions on new dir {full_path:?}: {e}");
        }

        Ok(())
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
            Err(e) => {
                return Err(MapacheError::Backend(format!(
                    "failed to stat remote file: {}",
                    e
                )));
            }
        };

        let is_dir = stat.is_dir();
        if let Some(perm) = stat.permissions {
            let new_perm = set_readonly_mode(perm, readonly, is_dir);
            if new_perm != perm {
                stat.permissions = Some(new_perm);
                sftp.set_metadata(&path_str, stat)
                    .await
                    .map_err(|e| MapacheError::Backend(e.to_string()))?;
            }
        }
        Ok(())
    }

    async fn remove_recursively_full(sftp: &SftpSession, full_path: &Path) -> Result<()> {
        let path_str = full_path.to_string_lossy().to_string();
        let meta = match sftp.symlink_metadata(&path_str).await {
            Ok(meta) => meta,
            Err(SftpClientError::Status(status))
                if status.status_code == StatusCode::NoSuchFile =>
            {
                return Ok(());
            }
            Err(e) => {
                return Err(MapacheError::Backend(format!(
                    "failed to inspect {:?} in sftp backend: {}",
                    full_path, e
                )));
            }
        };

        // Never chmod or recurse through a symlink: remove the link itself.
        if meta.file_type().is_symlink() {
            return sftp.remove_file(&path_str).await.map_err(|e| {
                MapacheError::Backend(format!(
                    "failed to remove symlink {:?} in sftp backend: {}",
                    full_path, e
                ))
            });
        }

        if let Err(e) = Self::set_readonly_status_full(sftp, full_path, false).await {
            tracing::warn!(target: "backend", "sftp: failed to unlock {full_path:?} before recursive remove: {e}");
        }

        if meta.is_file() {
            return sftp.remove_file(&path_str).await.map_err(|e| {
                MapacheError::Backend(format!(
                    "failed to remove file {:?} in sftp backend: {}",
                    full_path, e
                ))
            });
        }

        if meta.is_dir() {
            let entries = sftp
                .read_dir(&path_str)
                .await
                .map_err(|e| MapacheError::Backend(e.to_string()))?;
            for entry in entries {
                let name = entry.file_name();
                if name == "." || name == ".." {
                    continue;
                }
                let p = full_path.join(name);
                Box::pin(Self::remove_recursively_full(sftp, &p)).await?;
            }
            return sftp.remove_dir(&path_str).await.map_err(|e| {
                MapacheError::Backend(format!(
                    "failed to remove directory {:?} in sftp backend: {}",
                    full_path, e
                ))
            });
        }

        // Fallback
        if let Err(e) = sftp.remove_file(&path_str).await
            && let Err(_e2) = sftp.remove_dir(&path_str).await
        {
            return Err(MapacheError::Backend(e.to_string()));
        }
        Ok(())
    }

    /// Replaces the destination atomically when the server advertises the
    /// POSIX extension. Unsupported extensions and other errors are delegated
    /// to ordinary rename, which reports the meaningful failure to the caller.
    async fn try_posix_rename(sftp: &SftpSession, from: &str, to: &str, full_to: &Path) -> bool {
        match sftp.posix_rename(from, to).await {
            Ok(replaced) => replaced,
            Err(SftpClientError::Status(status))
                if status.status_code == StatusCode::PermissionDenied =>
            {
                // A read-only destination can be reported here too. Unlock it
                // and give the atomic path one more chance.
                if let Err(e) = Self::set_readonly_status_full(sftp, full_to, false).await {
                    tracing::warn!(target: "backend", "sftp: failed to unlock destination {:?} before rename: {e}", full_to);
                }
                sftp.posix_rename(from, to).await.unwrap_or(false)
            }
            Err(_) => false,
        }
    }

    async fn rename_full(sftp: &SftpSession, full_from: &Path, full_to: &Path) -> Result<()> {
        let from = full_from.to_string_lossy();
        let to = full_to.to_string_lossy();

        if let Err(e) = Self::set_readonly_status_full(sftp, full_from, false).await {
            tracing::warn!(target: "backend", "sftp: failed to unlock source {:?} before rename: {e}", full_from);
        }

        if Self::try_posix_rename(sftp, from.as_ref(), to.as_ref(), full_to).await {
            return Self::set_readonly_status_full(sftp, full_to, true).await;
        }

        let error = match sftp.rename(from.as_ref(), to.as_ref()).await {
            Ok(()) => return Self::set_readonly_status_full(sftp, full_to, true).await,
            Err(e) => e,
        };

        let overwrite_rejected = matches!(
            &error,
            SftpClientError::Status(s)
                if matches!(s.status_code, StatusCode::Failure | StatusCode::PermissionDenied)
        );
        if !overwrite_rejected {
            return Err(Self::rename_error(full_from, full_to, error));
        }

        // A read-only destination is reported as PermissionDenied. Unlock it
        // and retry before considering a replacement.
        if matches!(&error, SftpClientError::Status(s) if s.status_code == StatusCode::PermissionDenied)
        {
            if let Err(e) = Self::set_readonly_status_full(sftp, full_to, false).await {
                tracing::warn!(target: "backend", "sftp: failed to unlock destination {:?} before rename: {e}", full_to);
            }
            if sftp.rename(from.as_ref(), to.as_ref()).await.is_ok() {
                return Self::set_readonly_status_full(sftp, full_to, true).await;
            }
        }

        // When the destination is gone, the failure cannot be an overwrite
        // rejection: a fresh attempt is the only useful recovery.
        if !Self::symlink_exists_exact_full(sftp, full_to).await {
            sftp.rename(from.as_ref(), to.as_ref())
                .await
                .map_err(|e| Self::rename_error(full_from, full_to, e))?;
            return Self::set_readonly_status_full(sftp, full_to, true).await;
        }

        // The server refuses to replace an existing destination. Keep the old
        // file aside until the replacement succeeds so a failure can be undone.
        let backup = Self::backup_path(full_to);
        let backup_str = backup.to_string_lossy();
        sftp.rename(to.as_ref(), backup_str.as_ref())
            .await
            .map_err(|e| Self::rename_error(full_from, full_to, e))?;

        // A concurrent writer may have recreated the destination meanwhile.
        if Self::symlink_exists_exact_full(sftp, full_to).await {
            return match sftp.rename(backup_str.as_ref(), to.as_ref()).await {
                Ok(()) => Err(Self::rename_error(
                    full_from,
                    full_to,
                    "destination reappeared while replacing it",
                )),
                Err(e) => Err(MapacheError::BackendPermanent(format!(
                    "failed to rename {:?} -> {:?}: destination reappeared while replacing it; original destination is at {:?} and could not be restored: {}",
                    full_from, full_to, backup, e
                ))),
            };
        }

        if let Err(e) = sftp.rename(from.as_ref(), to.as_ref()).await {
            return match sftp.rename(backup_str.as_ref(), to.as_ref()).await {
                Ok(()) => Err(Self::rename_error(full_from, full_to, e)),
                Err(restore) => Err(MapacheError::BackendPermanent(format!(
                    "failed to rename {:?} -> {:?}: {}; original destination is at {:?} and could not be restored: {}",
                    full_from, full_to, e, backup, restore
                ))),
            };
        }

        if let Err(e) = Self::remove_recursively_full(sftp, &backup).await {
            tracing::warn!(target: "backend", "sftp: failed to remove replaced destination backup {:?}: {e}", backup);
        }
        Self::set_readonly_status_full(sftp, full_to, true).await
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
        let Ok(full) = self.full_path(path) else {
            return false;
        };
        let res = self
            .retry(|| async {
                let conn = self.manager.get_connection().await?;
                Ok(Self::exists_exact_full(&conn.sftp, &full).await)
            })
            .await;

        res.unwrap_or(false)
    }

    async fn read(&self, handle: &Handle, offset: isize, length: usize) -> Result<Vec<u8>> {
        let full = self.full_path(handle.path)?;
        let limiter = self.download_limiter.clone();
        tracing::trace!(target: "backend", "SFTP: read {:?} (offset={}, length={})", handle.path, offset, length);

        self.retry(|| async {
            let conn = self.manager.get_connection().await?;
            Self::check_no_symlinks(&conn.sftp, &self.base_path, &full).await?;
            let mut file = conn
                .sftp
                .open(full.to_string_lossy().to_string())
                .await
                .map_err(|e| {
                    MapacheError::Backend(format!(
                        "failed to open file {:?} for reading: {}",
                        handle.path, e
                    ))
                })?;

            let file_size = file
                .metadata()
                .await
                .map_err(|e| MapacheError::Backend(e.to_string()))?
                .size
                .unwrap_or(0);
            let real_offset = super::resolve_read_offset(file_size, offset);
            file.seek(std::io::SeekFrom::Start(real_offset)).await?;

            // Cap the read length to the actual remaining bytes so that
            // read_exact doesn't fail with UnexpectedEof on short files.
            let bytes_remaining: usize = file_size.saturating_sub(real_offset) as usize;
            let read_length: usize = match length {
                0 => bytes_remaining,
                _ => std::cmp::min(length, bytes_remaining),
            };

            let mut contents = if length > 0 {
                vec![0u8; read_length]
            } else {
                Vec::new()
            };

            if let Some(l) = &limiter {
                let mut reader = ThrottledReader::new(file, l.clone());
                if length > 0 {
                    reader.read_exact(&mut contents).await?;
                } else {
                    reader.read_to_end(&mut contents).await?;
                }
            } else {
                if length > 0 {
                    file.read_exact(&mut contents).await?;
                } else {
                    file.read_to_end(&mut contents).await?;
                }
            }

            Ok(contents)
        })
        .await
    }

    async fn write(&self, handle: &Handle, contents: WriteContents<'_>) -> Result<()> {
        let mut tmp_os = handle.path.as_os_str().to_os_string();
        tmp_os.push(format!(".{REPO_TMP_EXTENSION}"));
        let tmp_path = PathBuf::from(tmp_os);
        let full_tmp = self.full_path(&tmp_path)?;
        let full_dst = self.full_path(handle.path)?;
        let limiter = self.upload_limiter.clone();
        tracing::trace!(target: "backend", "SFTP: write {:?} ({} bytes)", handle.path, contents.len());

        self.retry(|| async {
            let conn = self.manager.get_connection().await?;

            if let Some(parent) = full_tmp.parent() {
                Self::create_dir_all_checked(&conn.sftp, &self.base_path, parent).await?;
            }
            Self::check_no_symlinks(&conn.sftp, &self.base_path, &full_tmp).await?;
            Self::check_no_symlinks(&conn.sftp, &self.base_path, &full_dst).await?;

            let file = conn
                .sftp
                .open_with_flags(
                    full_tmp.to_string_lossy(),
                    OpenFlags::CREATE | OpenFlags::TRUNCATE | OpenFlags::WRITE,
                )
                .await
                .map_err(|e| {
                    MapacheError::Backend(format!(
                        "failed to create tmp file {:?}: {}",
                        tmp_path, e
                    ))
                })?;

            if let Some(l) = &limiter {
                // Stack: BufWriter -> ThrottledWriter -> SftpFile
                // This ensures that we buffer large amounts of data (e.g. 1 MiB)
                // but shape the traffic as it is sent to the underlying network
                // in smaller SFTP chunks.
                let throttled = ThrottledWriter::new(file, l.clone());
                let mut writer = BufWriter::with_capacity(size::MiB as usize, throttled);
                writer.write_all(&contents).await?;
                writer.flush().await?;
                writer.shutdown().await?;
            } else {
                let mut writer = BufWriter::with_capacity(size::MiB as usize, file);
                writer.write_all(&contents).await?;
                writer.flush().await?;
                writer.shutdown().await?;
            }

            Self::rename_full(&conn.sftp, &full_tmp, &full_dst).await
        })
        .await
    }

    async fn rename(&self, from: &Path, to: &Path) -> Result<()> {
        let full_from = self.full_path(from)?;
        let full_to = self.full_path(to)?;
        tracing::debug!(target: "backend", "SFTP: rename {:?} -> {:?}", from, to);

        self.retry(|| async {
            let conn = self.manager.get_connection().await?;
            Self::check_no_symlinks(&conn.sftp, &self.base_path, &full_from).await?;
            Self::check_no_symlinks(&conn.sftp, &self.base_path, &full_to).await?;
            Self::rename_full(&conn.sftp, &full_from, &full_to).await
        })
        .await
    }

    async fn list_dir(&self, path: &Path) -> Result<Vec<BackendNode>> {
        let full = self.full_path(path)?;
        tracing::debug!(target: "backend", "SFTP: list_dir {:?}", path);

        self.retry(|| async {
            let conn = self.manager.get_connection().await?;
            Self::check_no_symlinks(&conn.sftp, &self.base_path, &full).await?;

            let entries = conn
                .sftp
                .read_dir(full.to_string_lossy())
                .await
                .map_err(|e| {
                    MapacheError::Backend(format!(
                        "could not list directory {:?} in sftp backend: {}",
                        path, e
                    ))
                })?;

            let mut out = Vec::new();
            for entry in entries {
                let name = entry.file_name();
                if name == "." || name == ".." {
                    continue;
                }
                // Symlinks and special files are not repository entries.
                let file_type = entry.file_type();
                if !file_type.is_file() && !file_type.is_dir() {
                    continue;
                }
                let path = path.join(name);
                let metadata = entry.metadata();
                out.push(super::classify_backend_node(
                    path,
                    file_type.is_file(),
                    metadata.len(),
                ));
            }

            Ok(out)
        })
        .await
    }

    async fn create_dir(&self, path: &Path) -> Result<()> {
        let full = self.full_path(path)?;
        tracing::debug!(target: "backend", "SFTP: create_dir {:?}", path);

        self.retry(|| async {
            let conn = self.manager.get_connection().await?;
            Self::create_dir_all_checked(&conn.sftp, &self.base_path, &full).await
        })
        .await
    }

    async fn remove(&self, file_path: &Path) -> Result<()> {
        let full = self.full_path(file_path)?;
        tracing::debug!(target: "backend", "SFTP: remove {:?}", file_path);

        self.retry(|| async {
            let conn = self.manager.get_connection().await?;
            Self::check_no_symlinks(&conn.sftp, &self.base_path, &full).await?;
            Self::remove_recursively_full(&conn.sftp, &full).await
        })
        .await
    }

    async fn is_file(&self, path: &Path) -> bool {
        let Ok(full) = self.full_path(path) else {
            return false;
        };
        let res = self
            .retry(|| async {
                let conn = self.manager.get_connection().await?;
                Self::check_no_symlinks(&conn.sftp, &self.base_path, &full).await?;
                let meta = conn.sftp.symlink_metadata(full.to_string_lossy()).await;
                Ok(meta.map(|s| s.is_file()).unwrap_or(false))
            })
            .await;

        res.unwrap_or(false)
    }

    async fn is_dir(&self, path: &Path) -> bool {
        let Ok(full) = self.full_path(path) else {
            return false;
        };
        let res = self
            .retry(|| async {
                let conn = self.manager.get_connection().await?;
                Self::check_no_symlinks(&conn.sftp, &self.base_path, &full).await?;
                let meta = conn.sftp.symlink_metadata(full.to_string_lossy()).await;
                Ok(meta.map(|s| s.is_dir()).unwrap_or(false))
            })
            .await;

        res.unwrap_or(false)
    }

    async fn lstat(&self, path: &Path) -> Result<NodeAttr> {
        let full = self.full_path(path)?;

        self.retry(|| async {
            let conn = self.manager.get_connection().await?;
            Self::check_no_symlinks(&conn.sftp, &self.base_path, &full).await?;
            let meta = conn
                .sftp
                .symlink_metadata(full.to_string_lossy())
                .await
                .map_err(|e| MapacheError::Backend(e.to_string()))?;

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

#[cfg(test)]
mod tests {
    use crate::backend::BackendUrl;

    use super::*;

    #[test]
    fn test_sftp_backend_url() -> Result<()> {
        let user = String::from("user");
        let host = String::from("host");

        // Absolute path (starts with // in standard URL logic)
        assert_eq!(
            BackendUrl::from("sftp://user@host:22//home/target")?,
            BackendUrl::Sftp(
                user.clone(),
                host.clone(),
                22,
                PathBuf::from("/home/target")
            )
        );
        // Relative path (starts with / in standard URL logic)
        assert_eq!(
            BackendUrl::from("sftp://user@host:22/base/dir")?,
            BackendUrl::Sftp(user.clone(), host.clone(), 22, PathBuf::from("base/dir"))
        );
        assert_eq!(
            BackendUrl::from("sftp://user@host:22/dir")?,
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

        // Encoding test
        assert_eq!(
            BackendUrl::from("sftp://user%20name@host/path%20with%20spaces")?,
            BackendUrl::Sftp(
                "user name".to_string(),
                host,
                22,
                PathBuf::from("path with spaces")
            )
        );

        Ok(())
    }
}
