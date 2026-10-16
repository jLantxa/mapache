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
    keys::PrivateKey,
    server::{Auth, ChannelOpenHandle, Msg, Server as _, Session},
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

pub async fn build() -> Result<Fixture> {
    build_with(SftpAuth::default()).await
}

pub async fn build_with(auth: SftpAuth) -> Result<Fixture> {
    let dir = tempfile::tempdir().context("creating temporary directory for the SFTP server")?;
    let root = dir.path().to_path_buf();
    fs::create_dir_all(root.join(BASE_DIR)).context("creating SFTP repository directory")?;

    // Parse the host key and derive the public half *before* the key is moved
    // into the server configuration.
    let host_key = PrivateKey::from_openssh(HOST_KEY).context("parsing the test host key")?;
    // The OpenSSH public-key encoding is `"<algorithm> <base64> [comment]"`,
    // where the second field is exactly `base64(public_key.to_bytes())` -- the
    // same key material `SftpBackend` compares against `known_hosts`.
    let openssh_key = host_key
        .public_key()
        .to_openssh()
        .context("encoding the test host key")?;
    let mut key_fields = openssh_key.split_whitespace();
    let host_key_alg = key_fields
        .next()
        .context("OpenSSH public key has no algorithm")?;
    let host_key_b64 = key_fields
        .next()
        .context("OpenSSH public key has no key material")?;

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

    let mut server = Server { root: root.clone() };
    let task = tokio::spawn(async move {
        let _ = server.run_on_socket(config, &listener).await;
    });

    // `SftpBackend` verifies the host key against a `known_hosts` file built
    // from `base64(public_key.to_bytes())`, which is the second field of the
    // OpenSSH encoding used above.
    let host_port = format!("[{}]:{}", addr.ip(), addr.port());
    let known_hosts = root.join("known_hosts");
    fs::write(
        &known_hosts,
        format!("{host_port} {host_key_alg} {host_key_b64}\n"),
    )
    .context("writing the test known_hosts file")?;

    let backend = Arc::new(
        SftpBackend::new(
            PathBuf::from(BASE_DIR),
            USER.to_string(),
            addr.ip().to_string(),
            addr.port(),
            auth_method(&root, auth)?,
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
        .context("connecting to the in-process SFTP server")?,
    );

    backend
        .create()
        .await
        .context("initializing the SFTP backend")?;

    Ok(Fixture::new(
        backend,
        // Abort the server before removing its backing directory.
        vec![Box::new(AbortOnDrop::new(task)), Box::new(dir)],
    ))
}

/// Builds the `AuthMethod` for the requested scheme, materializing the client
/// key on disk for public-key authentication.
fn auth_method(root: &Path, auth: SftpAuth) -> Result<AuthMethod> {
    Ok(match auth {
        SftpAuth::Password => AuthMethod::Password(Zeroizing::new(PASSWORD.to_string())),
        SftpAuth::PublicKey => {
            let key_path = root.join("client_key");
            fs::write(&key_path, CLIENT_KEY).context("writing the test client key")?;
            AuthMethod::PubKey {
                private_key: key_path,
                passphrase: None,
            }
        }
    })
}

// ---------------------------------------------------------------------------
// SSH layer
// ---------------------------------------------------------------------------

#[derive(Clone)]
struct Server {
    root: PathBuf,
}

impl russh::server::Server for Server {
    type Handler = SshSession;

    fn new_client(&mut self, _peer_addr: Option<SocketAddr>) -> SshSession {
        SshSession {
            root: self.root.clone(),
            channels: HashMap::new(),
        }
    }
}

struct SshSession {
    root: PathBuf,
    channels: HashMap<ChannelId, Channel<Msg>>,
}

impl russh::server::Handler for SshSession {
    type Error = anyhow::Error;

    async fn auth_password(&mut self, _user: &str, _password: &str) -> Result<Auth, Self::Error> {
        Ok(Auth::Accept)
    }

    async fn auth_publickey(
        &mut self,
        _user: &str,
        _public_key: &russh::keys::PublicKey,
    ) -> Result<Auth, Self::Error> {
        Ok(Auth::Accept)
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
/// connects with [`CLIENT_KEY`] and the server accepts the key, so the whole
/// data path is exercised over a key-authenticated session.
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
