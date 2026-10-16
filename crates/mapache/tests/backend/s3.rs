//! In-process S3 server (`s3s` + `s3s-fs`) plus the `S3Backend` fixture that
//! talks to it.
//!
//! The server is a Rust implementation of the S3 API that stores objects in a
//! temporary directory. It binds an ephemeral loopback port and validates
//! SigV4 signatures, so the tests exercise the real request/response path
//! against a local endpoint with no external infrastructure.
//!
//! The tests at the bottom cover S3 behavior the generic suite in
//! [`conformance`](super::conformance) cannot express: the multipart-upload
//! path and pagination past the server's page size.

use std::{
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
}

pub async fn build() -> Result<Fixture> {
    build_with(S3BuildOptions::default()).await
}

pub async fn build_with(options: S3BuildOptions) -> Result<Fixture> {
    let dir = tempfile::tempdir().context("creating temporary directory for the S3 server")?;
    let root = dir.path().to_path_buf();

    // `s3s-fs` maps buckets to directories under its root, and the backend's
    // `create()` performs a connectivity check that needs the bucket to exist.
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

    let mut backend = S3Backend::new(
        REGION.to_string(),
        BUCKET.to_string(),
        PathBuf::new(),
        format!("http://{addr}"),
        Zeroizing::new(ACCESS_KEY.to_string()),
        Zeroizing::new(SECRET_KEY.to_string()),
    )
    .context("constructing the S3 backend")?;

    if let (Some(threshold), Some(part_size)) =
        (options.multipart_threshold, options.multipart_part_size)
    {
        backend = backend.with_multipart_config(threshold, part_size);
    }

    let backend = Arc::new(backend);
    backend
        .create()
        .await
        .context("initializing the S3 backend")?;

    Ok(Fixture::new(
        backend,
        // Abort the server before removing its backing directory.
        vec![Box::new(AbortOnDrop::new(task)), Box::new(dir)],
    ))
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
