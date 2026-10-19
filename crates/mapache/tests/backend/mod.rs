//! In-process storage backend conformance tests.
//!
//! Every backend is driven through the same suite ([`conformance`]) against a
//! *live* implementation:
//!
//! - [`LocalFS`](mapache::backend::localfs::LocalFS) over a temporary directory,
//! - an in-process SFTP server (built on `russh`) backed by a temporary
//!   directory,
//! - an in-process S3 server (`s3s` + `s3s-fs`) backed by a temporary directory.
//!
//! The remote servers run *inside the test process* and listen on loopback, so
//! the whole suite runs with a plain `cargo test`: no external services, no
//! service containers, no downloads and no network access. It behaves
//! identically on a developer machine, on a CI runner and inside a container.
//!
//! The shared contract lives in [`conformance`]; each backend module (`local`,
//! `sftp`, `s3`) provides its fixture and any tests specific to it.

pub mod conformance;
pub mod local;
pub mod s3;
pub mod sftp;

use std::{any::Any, sync::Arc};

use anyhow::Result;
use mapache::backend::StorageBackend;

/// A backend under test plus the resources that must outlive the test
/// (temporary directories, background servers, ...).
pub struct Fixture {
    backend: Arc<dyn StorageBackend>,
    /// Kept alive (and dropped) after the backend. Order matters: the server
    /// task must stop before its backing directory is removed.
    _guards: Vec<Box<dyn Any + Send>>,
}

impl Fixture {
    pub(crate) fn new(backend: Arc<dyn StorageBackend>, guards: Vec<Box<dyn Any + Send>>) -> Self {
        Self {
            backend,
            _guards: guards,
        }
    }

    /// A `LocalFS` backend rooted at a temporary directory.
    pub async fn local() -> Result<Self> {
        local::build().await
    }

    /// A backend talking to an in-process SFTP server.
    pub async fn sftp() -> Result<Self> {
        sftp::build().await
    }

    /// A backend talking to an in-process S3 server.
    pub async fn s3() -> Result<Self> {
        s3::build().await
    }

    /// The initialized backend under test.
    pub fn backend(&self) -> &dyn StorageBackend {
        self.backend.as_ref()
    }
}

/// Aborts a tokio task when dropped; used to stop the in-process test servers.
pub(crate) struct AbortOnDrop(tokio::task::JoinHandle<()>);

impl AbortOnDrop {
    pub(crate) fn new(task: tokio::task::JoinHandle<()>) -> Self {
        Self(task)
    }
}

impl Drop for AbortOnDrop {
    fn drop(&mut self) {
        self.0.abort();
    }
}

/// Expands to one `#[tokio::test]` per backend for each named conformance
/// function in [`conformance`], so a single contract function is exercised
/// against `local`, `sftp` and `s3` independently.
macro_rules! backend_conformance {
    ($($name:ident),* $(,)?) => {
        $(
            mod $name {
                #[tokio::test]
                async fn local() -> anyhow::Result<()> {
                    let fixture = crate::backend::Fixture::local().await?;
                    crate::backend::conformance::$name(fixture.backend()).await
                }

                #[tokio::test]
                async fn sftp() -> anyhow::Result<()> {
                    let fixture = crate::backend::Fixture::sftp().await?;
                    crate::backend::conformance::$name(fixture.backend()).await
                }

                #[tokio::test]
                async fn s3() -> anyhow::Result<()> {
                    let fixture = crate::backend::Fixture::s3().await?;
                    crate::backend::conformance::$name(fixture.backend()).await
                }
            }
        )*
    };
}

backend_conformance!(
    create_is_idempotent,
    write_read_roundtrip,
    write_replaces_existing_contents,
    write_creates_missing_parent_directories,
    write_and_read_empty_file,
    read_with_length_past_eof_is_truncated,
    read_with_zero_length_reads_to_eof_from_offset,
    read_with_negative_offset_reads_tail,
    read_with_negative_offset_before_start_reads_whole_file,
    read_past_eof_returns_empty,
    read_missing_file_fails,
    path_exists_reflects_writes,
    is_file_and_is_dir_classify_correctly,
    lstat_reports_file_size,
    lstat_missing_path_fails,
    list_dir_lists_immediate_children,
    list_dir_reflects_removals,
    remove_file_is_idempotent,
    remove_directory_is_recursive,
    rename_moves_file,
    rename_replaces_existing_destination,
    list_dir_recursive_finds_all_files,
    create_dir_then_write_read_works,
    unicode_paths_roundtrip,
    paths_with_spaces_roundtrip,
);
