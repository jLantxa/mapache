//! In-process SFTP server plus the `SftpBackend` fixture that talks to it.
//!
//! The server is a minimal, filesystem-backed implementation of the SFTP
//! protocol built on the very same `russh` / `russh-sftp` crates the backend
//! uses. It binds an ephemeral loopback port, so no external SSH server is
//! needed and the tests work on any OS and inside a container.
//!
//! The test at the bottom covers SFTP behavior the generic suite in
//! [`conformance`](super::conformance) cannot express: public-key
//! authentication (the suite itself exercises password auth and the data path).

use std::{
    any::Any,
    collections::HashMap,
    fs,
    io::{Read, Seek, SeekFrom, Write},
    net::SocketAddr,
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};

use anyhow::{Context, Result, ensure};
use mapache::backend::{
    BackendOptions, Handle as BackendHandle, StorageBackend as _, WriteContents,
    sftp::{AuthMethod, SftpBackend},
};
use russh::{
    Channel, ChannelId,
    keys::{PrivateKey, PublicKey},
    server::{Auth, ChannelOpenHandle, Msg, Server as _, Session, VerifiedPublicKey},
};
use russh_sftp::protocol::{
    Attrs, Data, File, FileAttributes, Handle, Name, OpenFlags, Status, StatusCode, Version,
};
use tokio::net::TcpListener;
use zeroize::Zeroizing;

use super::{AbortOnDrop, Fixture};

/// Directory (relative to the server root) the repository lives in.
const BASE_DIR: &str = "repo";
const USER: &str = "mapache";
const PASSWORD: &str = "mapache-test-password";

/// A throwaway Ed25519 host key, generated with `ssh-keygen -t ed25519`. Only
/// ever used by the in-process test server.
const HOST_KEY: &str = "-----BEGIN OPENSSH PRIVATE KEY-----\n\
b3BlbnNzaC1rZXktdjEAAAAABG5vbmUAAAAEbm9uZQAAAAAAAAABAAAAMwAAAAtzc2gtZW\n\
QyNTUxOQAAACA28YZP2WVUqdAPKKkm/JPpCuI23znZlQ1H6uXWhB6/gwAAAJhlJrY1ZSa2\n\
NQAAAAtzc2gtZWQyNTUxOQAAACA28YZP2WVUqdAPKKkm/JPpCuI23znZlQ1H6uXWhB6/gw\n\
AAAEDmU+toGIT/bokSDhpXPrq4hjl3lVCeAgQ8Bqfxc7ZAQjbxhk/ZZVSp0A8oqSb8k+kK\n\
4jbfOdmVDUfq5daEHr+DAAAAEW1hcGFjaGUtdGVzdC1ob3N0AQIDBA==\n\
-----END OPENSSH PRIVATE KEY-----\n";

/// A throwaway Ed25519 client key (OpenSSH format), used to exercise
/// public-key authentication against the in-process server.
const CLIENT_KEY: &str = "-----BEGIN OPENSSH PRIVATE KEY-----\n\
b3BlbnNzaC1rZXktdjEAAAAABG5vbmUAAAAEbm9uZQAAAAAAAAABAAAAMwAAAAtzc2gtZW\n\
QyNTUxOQAAACBeE95iK4r9vZyakUVla5vV3oJmO3XTyOXvBxKLIubdowAAAJgUgxGOFIMR\n\
jgAAAAtzc2gtZWQyNTUxOQAAACBeE95iK4r9vZyakUVla5vV3oJmO3XTyOXvBxKLIubdow\n\
AAAEAWI6+9ZePcaabyqB5tCi0xUYZUMtxb1qQ2uG6Znovri14T3mIriv29nJqRRWVrm9Xe\n\
gmY7ddPI5e8HEosi5t2jAAAAE21hcGFjaGUtdGVzdC1jbGllbnQBAg==\n\
-----END OPENSSH PRIVATE KEY-----\n";

/// The authentication method the fixture's backend should use.
#[derive(Clone, Copy, Default)]
pub enum SftpAuth {
    /// Username/password authentication.
    #[default]
    Password,
    /// Public-key authentication with [`CLIENT_KEY`].
    PublicKey,
}

/// The credentials the in-process server is willing to accept.
///
/// The server enforces the policy rather than accepting everything, so the
/// authentication tests exercise both the accept and the reject paths.
#[derive(Clone)]
enum AuthPolicy {
    /// Accept only this password (and reject public-key authentication).
    Password(String),
    /// Accept only this public key (and reject password authentication).
    PublicKey(PublicKey),
}

pub async fn build() -> Result<Fixture> {
    build_with(SftpAuth::default()).await
}

pub async fn build_with(auth: SftpAuth) -> Result<Fixture> {
    let policy = match auth {
        SftpAuth::Password => AuthPolicy::Password(PASSWORD.to_string()),
        SftpAuth::PublicKey => AuthPolicy::PublicKey(client_public_key()?),
    };
    let server = TestServer::start(policy).await?;
    build_fixture(server, BASE_DIR, auth).await
}

/// The Ed25519 public half of [`CLIENT_KEY`].
fn client_public_key() -> Result<PublicKey> {
    Ok(PrivateKey::from_openssh(CLIENT_KEY)
        .context("parsing the test client key")?
        .public_key()
        .clone())
}

/// Builds a fixture whose backend is rooted at `base_path` on `server`.
async fn build_fixture(server: TestServer, base_path: &str, auth: SftpAuth) -> Result<Fixture> {
    fs::create_dir_all(server.root.join(base_path))
        .context("creating SFTP repository directory")?;
    let backend = server.connect(base_path, auth).await?;
    backend
        .create()
        .await
        .context("initializing the SFTP backend")?;

    Ok(Fixture::new(backend, server.into_guards()))
}

/// An in-process SFTP server bound to an ephemeral loopback port and backed by
/// a temporary directory.
struct TestServer {
    addr: SocketAddr,
    root: PathBuf,
    host_key_alg: String,
    host_key_b64: String,
    task: AbortOnDrop,
    dir: tempfile::TempDir,
}

impl TestServer {
    /// Boots the server on an ephemeral loopback port with the given auth policy.
    async fn start(policy: AuthPolicy) -> Result<Self> {
        let dir =
            tempfile::tempdir().context("creating temporary directory for the SFTP server")?;
        let root = dir.path().to_path_buf();

        // Parse the host key and derive the public half *before* the key is
        // moved into the server configuration.
        let host_key = PrivateKey::from_openssh(HOST_KEY).context("parsing the test host key")?;
        // The OpenSSH public-key encoding is `"<algorithm> <base64> [comment]"`,
        // where the second field is exactly `base64(public_key.to_bytes())` --
        // the same key material `SftpBackend` compares against `known_hosts`.
        let openssh_key = host_key
            .public_key()
            .to_openssh()
            .context("encoding the test host key")?;
        let mut key_fields = openssh_key.split_whitespace();
        let host_key_alg = key_fields
            .next()
            .context("OpenSSH public key has no algorithm")?
            .to_string();
        let host_key_b64 = key_fields
            .next()
            .context("OpenSSH public key has no key material")?
            .to_string();

        let config = Arc::new(russh::server::Config {
            // Keep handshakes instant in tests.
            auth_rejection_time: Duration::ZERO,
            auth_rejection_time_initial: Some(Duration::ZERO),
            inactivity_timeout: None,
            keys: vec![host_key],
            ..Default::default()
        });

        let listener = TcpListener::bind(("127.0.0.1", 0))
            .await
            .context("binding the in-process SFTP server")?;
        let addr = listener
            .local_addr()
            .context("reading SFTP server address")?;

        let mut server = Server {
            root: root.clone(),
            policy,
        };
        let task = tokio::spawn(async move {
            let _ = server.run_on_socket(config, &listener).await;
        });

        Ok(Self {
            addr,
            root,
            host_key_alg,
            host_key_b64,
            task: AbortOnDrop::new(task),
            dir,
        })
    }

    /// Connects a backend rooted at `base_path` using the requested scheme.
    async fn connect(&self, base_path: &str, auth: SftpAuth) -> Result<Arc<SftpBackend>> {
        let method = self.auth_method(base_path, auth)?;
        self.connect_with(base_path, method).await
    }

    /// Connects a backend rooted at `base_path` with an explicit `AuthMethod`.
    async fn connect_with(
        &self,
        base_path: &str,
        auth_method: AuthMethod,
    ) -> Result<Arc<SftpBackend>> {
        // `SftpBackend` verifies the host key against a `known_hosts` file built
        // from `base64(public_key.to_bytes())`, which is the second field of the
        // OpenSSH encoding used in `start`.
        let host_port = format!("[{}]:{}", self.addr.ip(), self.addr.port());
        let known_hosts = self
            .root
            .join(format!("known_hosts-{}", sanitize(base_path)));
        fs::write(
            &known_hosts,
            format!(
                "{host_port} {alg} {b64}\n",
                alg = self.host_key_alg,
                b64 = self.host_key_b64
            ),
        )
        .context("writing the test known_hosts file")?;

        let backend = SftpBackend::new(
            PathBuf::from(base_path),
            USER.to_string(),
            self.addr.ip().to_string(),
            self.addr.port(),
            auth_method,
            &BackendOptions {
                repo_path: String::new(),
                ssh_privatekey: None,
                ssh_known_hosts: Some(known_hosts),
                dry_backend: false,
                limit_upload: None,
                limit_download: None,
            },
        )
        .await
        .context("connecting to the in-process SFTP server")?;

        Ok(Arc::new(backend))
    }

    /// Builds the `AuthMethod` for the requested scheme, materializing the client
    /// key on disk for public-key authentication.
    fn auth_method(&self, base_path: &str, auth: SftpAuth) -> Result<AuthMethod> {
        Ok(match auth {
            SftpAuth::Password => AuthMethod::Password(Zeroizing::new(PASSWORD.to_string())),
            SftpAuth::PublicKey => {
                let key_path = self
                    .root
                    .join(format!("client_key-{}", sanitize(base_path)));
                fs::write(&key_path, CLIENT_KEY).context("writing the test client key")?;
                AuthMethod::PubKey {
                    private_key: key_path,
                    passphrase: None,
                }
            }
        })
    }

    fn into_guards(self) -> Vec<Box<dyn Any + Send>> {
        // Abort the server before removing its backing directory.
        vec![Box::new(self.task), Box::new(self.dir)]
    }
}

/// Turns a repository path into a safe file-name fragment.
fn sanitize(path: &str) -> String {
    path.replace('/', "_")
}

// ---------------------------------------------------------------------------
// SSH layer
// ---------------------------------------------------------------------------

#[derive(Clone)]
struct Server {
    root: PathBuf,
    policy: AuthPolicy,
}

impl russh::server::Server for Server {
    type Handler = SshSession;

    fn new_client(&mut self, _peer_addr: Option<SocketAddr>) -> SshSession {
        SshSession {
            root: self.root.clone(),
            policy: self.policy.clone(),
            channels: HashMap::new(),
        }
    }
}

struct SshSession {
    root: PathBuf,
    policy: AuthPolicy,
    channels: HashMap<ChannelId, Channel<Msg>>,
}

impl russh::server::Handler for SshSession {
    type Error = anyhow::Error;

    async fn auth_password(&mut self, _user: &str, password: &str) -> Result<Auth, Self::Error> {
        let accepted = match &self.policy {
            AuthPolicy::Password(expected) => password == expected,
            AuthPolicy::PublicKey(_) => false,
        };
        Ok(if accepted {
            Auth::Accept
        } else {
            Auth::reject()
        })
    }

    async fn auth_publickey(
        &mut self,
        _user: &str,
        verified_public_key: VerifiedPublicKey<'_>,
    ) -> Result<Auth, Self::Error> {
        let accepted = match &self.policy {
            AuthPolicy::Password(_) => false,
            AuthPolicy::PublicKey(expected) => verified_public_key
                .public_key
                .to_bytes()
                .eq(&expected.to_bytes()),
        };
        Ok(if accepted {
            Auth::Accept
        } else {
            Auth::reject()
        })
    }

    async fn channel_open_session(
        &mut self,
        channel: Channel<Msg>,
        reply: ChannelOpenHandle,
        _session: &mut Session,
    ) -> Result<(), Self::Error> {
        self.channels.insert(channel.id(), channel);
        reply.accept().await;
        Ok(())
    }

    async fn subsystem_request(
        &mut self,
        channel_id: ChannelId,
        name: &str,
        session: &mut Session,
    ) -> Result<(), Self::Error> {
        if name != "sftp" {
            session.channel_failure(channel_id)?;
            return Ok(());
        }

        let channel = self
            .channels
            .remove(&channel_id)
            .ok_or_else(|| anyhow::anyhow!("channel {channel_id:?} was not opened"))?;

        session.channel_success(channel_id)?;

        let handler = SftpHandler::new(self.root.clone());
        russh_sftp::server::run(channel.into_stream(), handler).await;

        Ok(())
    }
}

// ---------------------------------------------------------------------------
// SFTP layer
// ---------------------------------------------------------------------------

/// A filesystem-backed SFTP handler. Open file/directory handles are tracked by
/// an opaque string id, exactly as the protocol expects.
struct SftpHandler {
    root: PathBuf,
    next_id: u64,
    files: HashMap<String, fs::File>,
    dirs: HashMap<String, std::vec::IntoIter<File>>,
}

impl SftpHandler {
    fn new(root: PathBuf) -> Self {
        Self {
            root,
            next_id: 0,
            files: HashMap::new(),
            dirs: HashMap::new(),
        }
    }

    /// Maps a client path (repository-relative, e.g. `repo/packs/x`) to a path
    /// inside the server root.
    fn real(&self, path: &str) -> PathBuf {
        self.root.join(path.trim_start_matches('/'))
    }

    fn next_handle(&mut self) -> String {
        self.next_id += 1;
        format!("handle-{}", self.next_id)
    }
}

fn ok(id: u32) -> Status {
    Status {
        id,
        status_code: StatusCode::Ok,
        error_message: "Ok".to_string(),
        language_tag: "en-US".to_string(),
    }
}

fn status_from_io(err: &std::io::Error) -> StatusCode {
    match err.kind() {
        std::io::ErrorKind::NotFound => StatusCode::NoSuchFile,
        std::io::ErrorKind::PermissionDenied => StatusCode::PermissionDenied,
        _ => StatusCode::Failure,
    }
}

/// Builds SFTP attributes from filesystem metadata. On Windows the raw
/// permission bits are normalized so the read-only round-trip used by the
/// backend is stable.
fn attrs_from(meta: &fs::Metadata) -> FileAttributes {
    #[cfg(not(windows))]
    {
        FileAttributes::from(meta)
    }

    #[cfg(windows)]
    {
        let mut attrs = FileAttributes::from(meta);
        if attrs.permissions.is_some() {
            attrs.permissions = Some(if meta.permissions().readonly() {
                0o400
            } else {
                0o600
            });
        }
        attrs.set_dir(meta.is_dir());
        attrs.set_regular(!meta.is_dir());
        attrs
    }
}

/// Applies the permission bits carried by an `SSH_FXP_SETSTAT`/`SSH_FXP_MKDIR`
/// request, if any.
fn apply_permissions(path: &Path, attrs: &FileAttributes) -> Result<(), StatusCode> {
    let Some(mode) = attrs.permissions else {
        return Ok(());
    };

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(path, fs::Permissions::from_mode(mode & 0o7777))
            .map_err(|e| status_from_io(&e))?;
    }
    #[cfg(windows)]
    {
        let metadata = fs::metadata(path).map_err(|e| status_from_io(&e))?;
        let mut permissions = metadata.permissions();
        permissions.set_readonly(mode & 0o200 == 0);
        fs::set_permissions(path, permissions).map_err(|e| status_from_io(&e))?;
    }
    #[cfg(not(any(unix, windows)))]
    {
        let _ = (path, mode);
    }

    Ok(())
}

impl russh_sftp::server::Handler for SftpHandler {
    type Error = StatusCode;

    fn unimplemented(&self) -> StatusCode {
        StatusCode::OpUnsupported
    }

    async fn init(
        &mut self,
        _version: u32,
        _extensions: HashMap<String, String>,
    ) -> Result<Version, StatusCode> {
        Ok(Version::new())
    }

    async fn open(
        &mut self,
        id: u32,
        filename: String,
        pflags: OpenFlags,
        _attrs: FileAttributes,
    ) -> Result<Handle, StatusCode> {
        let options: fs::OpenOptions = pflags.into();
        let file = options
            .open(self.real(&filename))
            .map_err(|e| status_from_io(&e))?;

        let handle = self.next_handle();
        self.files.insert(handle.clone(), file);
        Ok(Handle { id, handle })
    }

    async fn close(&mut self, id: u32, handle: String) -> Result<Status, StatusCode> {
        self.files.remove(&handle);
        self.dirs.remove(&handle);
        Ok(ok(id))
    }

    async fn read(
        &mut self,
        id: u32,
        handle: String,
        offset: u64,
        len: u32,
    ) -> Result<Data, StatusCode> {
        let file = self.files.get_mut(&handle).ok_or(StatusCode::Failure)?;
        let size = file.metadata().map(|m| m.len()).unwrap_or(0);
        if offset >= size {
            return Err(StatusCode::Eof);
        }

        file.seek(SeekFrom::Start(offset))
            .map_err(|e| status_from_io(&e))?;

        let to_read = (size - offset).min(u64::from(len)) as usize;
        let mut buf = vec![0u8; to_read];
        let mut filled = 0;
        while filled < to_read {
            match file.read(&mut buf[filled..]) {
                Ok(0) => break,
                Ok(n) => filled += n,
                Err(e) => return Err(status_from_io(&e)),
            }
        }
        buf.truncate(filled);
        Ok(Data { id, data: buf })
    }

    async fn write(
        &mut self,
        id: u32,
        handle: String,
        offset: u64,
        data: Vec<u8>,
    ) -> Result<Status, StatusCode> {
        let file = self.files.get_mut(&handle).ok_or(StatusCode::Failure)?;
        file.seek(SeekFrom::Start(offset))
            .map_err(|e| status_from_io(&e))?;
        file.write_all(&data).map_err(|e| status_from_io(&e))?;
        Ok(ok(id))
    }

    async fn lstat(&mut self, id: u32, path: String) -> Result<Attrs, StatusCode> {
        let meta = fs::symlink_metadata(self.real(&path)).map_err(|e| status_from_io(&e))?;
        Ok(Attrs {
            id,
            attrs: attrs_from(&meta),
        })
    }

    async fn stat(&mut self, id: u32, path: String) -> Result<Attrs, StatusCode> {
        let meta = fs::metadata(self.real(&path)).map_err(|e| status_from_io(&e))?;
        Ok(Attrs {
            id,
            attrs: attrs_from(&meta),
        })
    }

    async fn fstat(&mut self, id: u32, handle: String) -> Result<Attrs, StatusCode> {
        let file = self.files.get(&handle).ok_or(StatusCode::Failure)?;
        let meta = file.metadata().map_err(|e| status_from_io(&e))?;
        Ok(Attrs {
            id,
            attrs: attrs_from(&meta),
        })
    }

    async fn setstat(
        &mut self,
        id: u32,
        path: String,
        attrs: FileAttributes,
    ) -> Result<Status, StatusCode> {
        apply_permissions(&self.real(&path), &attrs)?;
        Ok(ok(id))
    }

    async fn opendir(&mut self, id: u32, path: String) -> Result<Handle, StatusCode> {
        let mut entries = Vec::new();
        for entry in fs::read_dir(self.real(&path)).map_err(|e| status_from_io(&e))? {
            let entry = entry.map_err(|e| status_from_io(&e))?;
            let name = entry.file_name().to_string_lossy().into_owned();
            let attrs = match entry.metadata() {
                Ok(meta) => attrs_from(&meta),
                Err(_) => FileAttributes::empty(),
            };
            entries.push(File::new(name, attrs));
        }
        entries.sort_by(|a, b| a.filename.cmp(&b.filename));

        let handle = self.next_handle();
        self.dirs.insert(handle.clone(), entries.into_iter());
        Ok(Handle { id, handle })
    }

    async fn readdir(&mut self, id: u32, handle: String) -> Result<Name, StatusCode> {
        let iter = self.dirs.get_mut(&handle).ok_or(StatusCode::Failure)?;
        // The whole (remaining) directory is returned in one packet; the next
        // call reports EOF.
        let files: Vec<File> = iter.collect();
        if files.is_empty() {
            return Err(StatusCode::Eof);
        }
        Ok(Name { id, files })
    }

    async fn remove(&mut self, id: u32, filename: String) -> Result<Status, StatusCode> {
        fs::remove_file(self.real(&filename)).map_err(|e| status_from_io(&e))?;
        Ok(ok(id))
    }

    async fn mkdir(
        &mut self,
        id: u32,
        path: String,
        attrs: FileAttributes,
    ) -> Result<Status, StatusCode> {
        let real = self.real(&path);
        fs::create_dir(&real).map_err(|e| status_from_io(&e))?;
        apply_permissions(&real, &attrs)?;
        Ok(ok(id))
    }

    async fn rmdir(&mut self, id: u32, path: String) -> Result<Status, StatusCode> {
        fs::remove_dir(self.real(&path)).map_err(|e| status_from_io(&e))?;
        Ok(ok(id))
    }

    async fn realpath(&mut self, id: u32, path: String) -> Result<Name, StatusCode> {
        Ok(Name {
            id,
            files: vec![File::dummy(path)],
        })
    }

    async fn rename(
        &mut self,
        id: u32,
        oldpath: String,
        newpath: String,
    ) -> Result<Status, StatusCode> {
        fs::rename(self.real(&oldpath), self.real(&newpath)).map_err(|e| status_from_io(&e))?;
        Ok(ok(id))
    }
}

/// Public-key authentication against the in-process server: the backend
/// connects with [`CLIENT_KEY`] and the server accepts *only* that key, so the
/// whole data path is exercised over a key-authenticated session.
#[tokio::test]
async fn public_key_authentication() -> Result<()> {
    let fixture = build_with(SftpAuth::PublicKey).await?;
    let backend = fixture.backend();

    backend
        .write(
            &BackendHandle::new(Path::new("pubkey.txt")),
            WriteContents::Borrowed(b"authenticated with a key"),
        )
        .await?;

    let data = backend
        .read(&BackendHandle::new(Path::new("pubkey.txt")), 0, 0)
        .await?;
    ensure!(data == b"authenticated with a key");
    Ok(())
}

/// The correct password authenticates; a wrong one is rejected.
#[tokio::test]
async fn rejects_wrong_password() -> Result<()> {
    let server = TestServer::start(AuthPolicy::Password(PASSWORD.to_string())).await?;

    let accepted = server.connect(BASE_DIR, SftpAuth::Password).await;
    ensure!(accepted.is_ok(), "the correct password must authenticate");

    let rejected = server
        .connect_with(
            BASE_DIR,
            AuthMethod::Password(Zeroizing::new("not-the-password".to_string())),
        )
        .await;
    ensure!(rejected.is_err(), "a wrong password must be rejected");
    Ok(())
}

/// A public key the server does not trust must be rejected.
#[tokio::test]
async fn rejects_unknown_public_key() -> Result<()> {
    // The server trusts the *host* key; the client authenticates with `CLIENT_KEY`.
    let trusted = PrivateKey::from_openssh(HOST_KEY)
        .context("parsing the test host key")?
        .public_key()
        .clone();
    let server = TestServer::start(AuthPolicy::PublicKey(trusted)).await?;

    let rejected = server.connect(BASE_DIR, SftpAuth::PublicKey).await;
    ensure!(rejected.is_err(), "an unknown public key must be rejected");
    Ok(())
}

/// A backend whose repository lives in a nested server directory must only see
/// its own files, and must map repository-relative paths correctly.
#[tokio::test]
async fn non_empty_base_path_isolation() -> Result<()> {
    let server = TestServer::start(AuthPolicy::Password(PASSWORD.to_string())).await?;

    let nested = server.connect("nested/repo", SftpAuth::Password).await?;
    let sibling = server.connect("sibling", SftpAuth::Password).await?;
    nested.create().await?;
    sibling.create().await?;

    nested
        .write(
            &BackendHandle::new(Path::new("packs/a.pack")),
            WriteContents::Borrowed(b"data"),
        )
        .await?;

    ensure!(
        nested
            .read(&BackendHandle::new(Path::new("packs/a.pack")), 0, 0)
            .await?
            == b"data"
    );

    // Listing returns repository-relative paths, not server paths.
    let entries = nested.list_dir(Path::new("packs")).await?;
    ensure!(
        entries.len() == 1 && entries[0].path() == Path::new("packs/a.pack"),
        "unexpected entries: {entries:?}"
    );
    ensure!(nested.is_dir(Path::new("packs")).await);

    // A sibling base path on the same server must not see it.
    ensure!(!sibling.path_exists(Path::new("packs/a.pack")).await);
    ensure!(sibling.list_dir(Path::new("")).await?.is_empty());
    Ok(())
}

/// `lstat` must expose the POSIX permission bits applied by the backend's
/// read-only post-processing, so that repository immutability is observable.
#[tokio::test]
async fn lstat_reports_readonly_permissions() -> Result<()> {
    let fixture = build().await?;
    let backend = fixture.backend();

    backend
        .write(
            &BackendHandle::new(Path::new("ro.bin")),
            WriteContents::Borrowed(b"1234"),
        )
        .await?;

    let attr = backend.lstat(Path::new("ro.bin")).await?;
    ensure!(attr.size == Some(4), "unexpected size: {:?}", attr.size);

    #[cfg(any(unix, windows))]
    ensure!(
        attr.perm.map(|perm| perm & 0o777) == Some(0o400),
        "a written file should be read-only, got {:?}",
        attr.perm
    );
    Ok(())
}
