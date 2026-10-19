//! In-process S3 server (`s3s` + `s3s-fs`) plus the `S3Backend` fixture that
//! talks to it.
//!
//! The server is a Rust implementation of the S3 API that stores objects in a
//! temporary directory. It binds an ephemeral loopback port and validates
//! SigV4 signatures, so the tests exercise the real request/response path
//! against a local endpoint with no external infrastructure.
//!
//! The tests at the bottom cover S3 behavior the generic suite in
//! [`conformance`](super::conformance) cannot express: multipart uploads,
//! listing pagination (including keys that sort before the page size),
//! recursive bulk removal, prefix isolation and `Last-Modified` parsing.

use std::{
    any::Any,
    net::SocketAddr,
    path::{Path, PathBuf},
    sync::Arc,
};

use anyhow::{Context, Result, ensure};
use hyper_util::{
    rt::{TokioExecutor, TokioIo},
    server::conn::auto::Builder,
};
use mapache::backend::{Handle, StorageBackend as _, WriteContents, s3::S3Backend};
use s3s::{auth::SimpleAuth, service::S3ServiceBuilder};
use tokio::net::TcpListener;
use zeroize::Zeroizing;

use super::{AbortOnDrop, Fixture};

const ACCESS_KEY: &str = "mapache-test-access-key";
const SECRET_KEY: &str = "mapache-test-secret-key";
const BUCKET: &str = "mapache-test-bucket";
const REGION: &str = "us-east-1";

/// Optional tuning for the S3 fixture.
#[derive(Default)]
pub struct S3BuildOptions {
    /// Multipart-upload threshold, in bytes. Overrides the runtime default.
    pub multipart_threshold: Option<u64>,
    /// Multipart part size, in bytes. Overrides the runtime default.
    pub multipart_part_size: Option<u64>,
    /// Key prefix (the repository's location inside the bucket). Defaults to the
    /// bucket root.
    pub prefix: Option<PathBuf>,
}

pub async fn build() -> Result<Fixture> {
    build_with(S3BuildOptions::default()).await
}

pub async fn build_with(options: S3BuildOptions) -> Result<Fixture> {
    let server = TestServer::start().await?;
    let prefix = options.prefix.clone().unwrap_or_default();
    let backend = Arc::new(server.connect(prefix, &options)?);
    backend
        .create()
        .await
        .context("initializing the S3 backend")?;

    Ok(Fixture::new(backend, server.into_guards()))
}

/// An in-process S3 server (`s3s` + `s3s-fs`) backed by a temporary directory.
struct TestServer {
    addr: SocketAddr,
    task: AbortOnDrop,
    dir: tempfile::TempDir,
}

impl TestServer {
    /// Boots the server on an ephemeral loopback port.
    async fn start() -> Result<Self> {
        let dir = tempfile::tempdir().context("creating temporary directory for the S3 server")?;
        let root = dir.path().to_path_buf();

        // `s3s-fs` rejects requests for an unknown bucket, and the backend's
        // `create()` performs a connectivity check (a bucket listing), so the
        // bucket has to exist up front.
        std::fs::create_dir_all(root.join(BUCKET)).context("creating the S3 bucket directory")?;

        let fs = s3s_fs::FileSystem::new(&root)
            .map_err(|e| anyhow::anyhow!("starting the in-process S3 filesystem: {e:?}"))?;
        let mut builder = S3ServiceBuilder::new(fs);
        builder.set_auth(SimpleAuth::from_single(ACCESS_KEY, SECRET_KEY));
        let service = builder.build();

        let listener = TcpListener::bind(("127.0.0.1", 0))
            .await
            .context("binding the in-process S3 server")?;
        let addr = listener.local_addr().context("reading S3 server address")?;

        let http = Builder::new(TokioExecutor::new());
        let task = tokio::spawn(async move {
            loop {
                let Ok((socket, _)) = listener.accept().await else {
                    break;
                };
                let connection = http
                    .serve_connection(TokioIo::new(socket), service.clone())
                    .into_owned();
                tokio::spawn(async move {
                    let _ = connection.await;
                });
            }
        });

        Ok(Self {
            addr,
            task: AbortOnDrop::new(task),
            dir,
        })
    }

    /// Builds a backend rooted at `prefix` inside the shared bucket.
    fn connect(&self, prefix: PathBuf, options: &S3BuildOptions) -> Result<S3Backend> {
        let mut backend = S3Backend::new(
            REGION.to_string(),
            BUCKET.to_string(),
            prefix,
            format!("http://{}", self.addr),
            Zeroizing::new(ACCESS_KEY.to_string()),
            Zeroizing::new(SECRET_KEY.to_string()),
        )
        .context("constructing the S3 backend")?;

        if let (Some(threshold), Some(part_size)) =
            (options.multipart_threshold, options.multipart_part_size)
        {
            backend = backend.with_multipart_config(threshold, part_size);
        }

        Ok(backend)
    }

    fn into_guards(self) -> Vec<Box<dyn Any + Send>> {
        // Abort the server before removing its backing directory.
        vec![Box::new(self.task), Box::new(self.dir)]
    }
}

/// A backend configured with a non-empty key prefix must store and list objects
/// relative to that prefix, and a sibling backend sharing the same bucket must
/// not see them.
#[tokio::test]
async fn non_empty_prefix_isolation() -> Result<()> {
    let server = TestServer::start().await?;

    let prefixed =
        Arc::new(server.connect(PathBuf::from("nested/repo"), &S3BuildOptions::default())?);
    let plain = Arc::new(server.connect(PathBuf::new(), &S3BuildOptions::default())?);
    prefixed.create().await?;
    plain.create().await?;

    prefixed
        .write(
            &Handle::new(Path::new("packs/a.pack")),
            WriteContents::Borrowed(b"data"),
        )
        .await?;

    ensure!(
        prefixed
            .read(&Handle::new(Path::new("packs/a.pack")), 0, 0)
            .await?
            == b"data"
    );

    // Listing returns repository-relative paths, not raw keys.
    let entries = prefixed.list_dir(Path::new("packs")).await?;
    ensure!(
        entries.len() == 1 && entries[0].path() == Path::new("packs/a.pack"),
        "unexpected entries: {entries:?}"
    );
    ensure!(prefixed.is_dir(Path::new("packs")).await);

    // The prefix-less backend sees the prefix as a top-level directory, proving
    // the object was stored underneath it...
    let root = plain.list_dir(Path::new("")).await?;
    ensure!(
        root.iter().any(|n| n.path() == Path::new("nested")),
        "expected a 'nested' directory at the bucket root: {root:?}"
    );
    // ...and does not see the repository-relative path.
    ensure!(!plain.path_exists(Path::new("packs/a.pack")).await);
    Ok(())
}

/// A write larger than the (test-sized) threshold must go through the
/// multipart-upload path and round-trip byte-for-byte, including range reads
/// across part boundaries.
///
/// The part size stays at S3's enforced minimum (5 MiB) because `s3s-fs`
/// rejects smaller non-final parts with `EntityTooSmall`, exactly like AWS.
#[tokio::test]
async fn multipart_upload_roundtrip() -> Result<()> {
    const MIB: usize = 1024 * 1024;
    const PART_SIZE: usize = 5 * MIB;
    const THRESHOLD: u64 = MIB as u64;

    let fixture = build_with(S3BuildOptions {
        multipart_threshold: Some(THRESHOLD),
        multipart_part_size: Some(PART_SIZE as u64),
        ..S3BuildOptions::default()
    })
    .await?;
    let backend = fixture.backend();

    // A bit over one full part, so the upload spans two parts.
    let data: Vec<u8> = (0..(PART_SIZE + 1234)).map(|i| (i % 251) as u8).collect();
    backend
        .write(
            &Handle::new(Path::new("big.bin")),
            WriteContents::Borrowed(&data),
        )
        .await?;

    let whole = backend
        .read(&Handle::new(Path::new("big.bin")), 0, 0)
        .await?;
    ensure!(whole == data, "multipart round-trip mismatch");

    let size = backend.lstat(Path::new("big.bin")).await?.size;
    ensure!(size == Some(data.len() as u64), "unexpected size {size:?}");

    // A range that starts in the first part and ends in the second.
    let range = backend
        .read(
            &Handle::new(Path::new("big.bin")),
            (PART_SIZE - 100) as isize,
            300,
        )
        .await?;
    ensure!(
        range == data[PART_SIZE - 100..PART_SIZE + 200],
        "range read mismatch"
    );
    Ok(())
}

/// `list_dir` must follow `NextContinuationToken` until the listing is
/// exhausted. With the server's 1000-key page size, 1001 objects force a
/// second page.
#[tokio::test]
async fn list_dir_follows_pagination() -> Result<()> {
    let fixture = Fixture::s3().await?;
    let backend = fixture.backend();

    const COUNT: u32 = 1001;
    for i in 0..COUNT {
        let path = format!("pages/{i:04}");
        backend
            .write(
                &Handle::new(Path::new(&path)),
                WriteContents::Borrowed(b"x"),
            )
            .await?;
    }

    let listed = backend.list_dir(Path::new("pages")).await?;
    ensure!(
        listed.len() == COUNT as usize,
        "list_dir returned {} entries, expected {COUNT}",
        listed.len()
    );

    let recursive = backend.list_dir_recursive(Path::new("pages")).await?;
    ensure!(
        recursive.len() == COUNT as usize,
        "list_dir_recursive returned {} entries, expected {COUNT}",
        recursive.len()
    );
    Ok(())
}

/// Listings must not filter keys via `StartAfter`. Passing the page size in the
/// wrong argument slot used to make `StartAfter = "1000"`, silently skipping
/// keys that sort before it.
#[tokio::test]
async fn list_dir_includes_keys_sorting_before_page_size() -> Result<()> {
    let fixture = Fixture::s3().await?;
    let backend = fixture.backend();

    for path in ["0/a.txt", "0/b.txt", "0000.txt"] {
        backend
            .write(&Handle::new(Path::new(path)), WriteContents::Borrowed(b"x"))
            .await?;
    }

    ensure!(backend.is_dir(Path::new("0")).await);
    ensure!(backend.is_file(Path::new("0000.txt")).await);
    let nested = backend.list_dir(Path::new("0")).await?;
    ensure!(nested.len() == 2, "unexpected entries: {nested:?}");
    ensure!(backend.list_dir(Path::new("")).await?.len() == 2);

    backend.remove(Path::new("0")).await?;
    backend.remove(Path::new("0000.txt")).await?;
    ensure!(backend.list_dir(Path::new("")).await?.is_empty());
    Ok(())
}

/// `remove` must delete every object in a directory across listing pages. This
/// exercises the `start_after` continuation and the bulk-delete batching.
#[tokio::test]
async fn remove_directory_spans_pages() -> Result<()> {
    let fixture = Fixture::s3().await?;
    let backend = fixture.backend();

    const COUNT: u32 = 1001;
    for i in 0..COUNT {
        let path = format!("bulk/{i:04}");
        backend
            .write(
                &Handle::new(Path::new(&path)),
                WriteContents::Borrowed(b"x"),
            )
            .await?;
    }

    backend.remove(Path::new("bulk")).await?;

    ensure!(!backend.path_exists(Path::new("bulk")).await);
    ensure!(backend.list_dir(Path::new("")).await?.is_empty());
    Ok(())
}

/// `lstat` must report the object's `Last-Modified` timestamp. This exercises
/// the HTTP-date parsing, which must not silently fall back to the epoch.
#[tokio::test]
async fn lstat_reports_modification_time() -> Result<()> {
    let fixture = Fixture::s3().await?;
    let backend = fixture.backend();

    backend
        .write(
            &Handle::new(Path::new("m.bin")),
            WriteContents::Borrowed(b"x"),
        )
        .await?;

    let mtime = backend
        .lstat(Path::new("m.bin"))
        .await?
        .mtime
        .context("last-modified timestamp missing")?;
    let age = std::time::SystemTime::now()
        .duration_since(mtime)
        .unwrap_or_default();
    ensure!(
        age < std::time::Duration::from_secs(300),
        "unexpected object age {age:?}"
    );
    Ok(())
}
