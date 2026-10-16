//! `LocalFS` fixture: a backend rooted at a temporary directory.

use std::sync::Arc;

use anyhow::{Context, Result};
use mapache::backend::{StorageBackend as _, localfs::LocalFS};
use tempfile::tempdir;

use super::Fixture;

pub async fn build() -> Result<Fixture> {
    let dir = tempdir().context("creating temporary directory for LocalFS")?;
    let backend = Arc::new(LocalFS::new(dir.path().to_path_buf()));
    backend
        .create()
        .await
        .context("initializing LocalFS backend")?;

    Ok(Fixture::new(
        backend,
        // Keep the directory alive for the whole test.
        vec![Box::new(dir)],
    ))
}
