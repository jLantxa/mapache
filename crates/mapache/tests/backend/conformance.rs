//! Generic [`StorageBackend`] conformance suite.
//!
//! Each function asserts one piece of the `StorageBackend` contract that every
//! backend must honor. They are invoked once per backend by the
//! `backend_conformance!` macro in [`mod.rs`](super), so a failure pinpoints
//! both the contract and the backend that violates it
//! (e.g. `backend::write_read_roundtrip::s3`).
//!
//! The functions only rely on behavior that is common to all backends; anything
//! backend-specific (symlinks, permission bits, S3 multipart, ...) lives in the
//! corresponding backend module instead (e.g. [`s3`](super::s3)).

use std::path::Path;

use anyhow::{Result, ensure};
use mapache::backend::{BackendNode, Handle, StorageBackend, WriteContents};

/// Writes `data` to `path`.
async fn put(backend: &dyn StorageBackend, path: &str, data: &[u8]) -> Result<()> {
    backend
        .write(&Handle::new(Path::new(path)), WriteContents::Borrowed(data))
        .await?;
    Ok(())
}

/// Reads the entire file at `path`.
async fn get(backend: &dyn StorageBackend, path: &str) -> Result<Vec<u8>> {
    Ok(backend.read(&Handle::new(Path::new(path)), 0, 0).await?)
}

/// Reads `length` bytes from `path`, starting at `offset`.
async fn get_range(
    backend: &dyn StorageBackend,
    path: &str,
    offset: isize,
    length: usize,
) -> Result<Vec<u8>> {
    Ok(backend
        .read(&Handle::new(Path::new(path)), offset, length)
        .await?)
}

/// Normalizes a backend path to a forward-slash, platform-independent string.
fn normalize(path: &Path) -> String {
    path.to_string_lossy().replace('\\', "/")
}

/// Sorted list of the paths of `nodes`.
fn sorted_paths(nodes: &[BackendNode]) -> Vec<String> {
    let mut paths: Vec<String> = nodes.iter().map(|n| normalize(n.path())).collect();
    paths.sort();
    paths
}

/// Sorted list of the paths of the *file* nodes only.
fn sorted_files(nodes: &[BackendNode]) -> Vec<String> {
    let mut paths: Vec<String> = nodes
        .iter()
        .filter_map(|n| match n {
            BackendNode::File(path, _) => Some(normalize(path)),
            BackendNode::Dir(_) => None,
        })
        .collect();
    paths.sort();
    paths
}

/// Sorted list of the paths of the *directory* nodes only.
fn sorted_dirs(nodes: &[BackendNode]) -> Vec<String> {
    let mut paths: Vec<String> = nodes
        .iter()
        .filter_map(|n| match n {
            BackendNode::Dir(path) => Some(normalize(path)),
            BackendNode::File(_, _) => None,
        })
        .collect();
    paths.sort();
    paths
}

pub async fn create_is_idempotent(backend: &dyn StorageBackend) -> Result<()> {
    // The fixture already called `create()` once; a second call must not fail.
    backend.create().await?;
    Ok(())
}

pub async fn write_read_roundtrip(backend: &dyn StorageBackend) -> Result<()> {
    let data = b"the quick brown fox jumps over the lazy dog";
    put(backend, "hello.txt", data).await?;
    ensure!(get(backend, "hello.txt").await? == data);
    Ok(())
}

pub async fn write_replaces_existing_contents(backend: &dyn StorageBackend) -> Result<()> {
    put(backend, "f.txt", b"first").await?;
    put(backend, "f.txt", b"second, longer").await?;
    ensure!(get(backend, "f.txt").await? == b"second, longer");
    Ok(())
}

pub async fn write_creates_missing_parent_directories(backend: &dyn StorageBackend) -> Result<()> {
    put(backend, "a/b/c/deep.txt", b"deep").await?;
    ensure!(get(backend, "a/b/c/deep.txt").await? == b"deep");
    Ok(())
}

pub async fn write_and_read_empty_file(backend: &dyn StorageBackend) -> Result<()> {
    put(backend, "empty", b"").await?;
    ensure!(get(backend, "empty").await?.is_empty());
    ensure!(backend.is_file(Path::new("empty")).await);
    Ok(())
}

pub async fn read_with_length_past_eof_is_truncated(backend: &dyn StorageBackend) -> Result<()> {
    put(backend, "r.txt", b"short").await?;
    ensure!(get_range(backend, "r.txt", 2, 1000).await? == b"ort");
    Ok(())
}

pub async fn read_with_zero_length_reads_to_eof_from_offset(
    backend: &dyn StorageBackend,
) -> Result<()> {
    put(backend, "r.txt", b"abcdef").await?;
    ensure!(get_range(backend, "r.txt", 3, 0).await? == b"def");
    Ok(())
}

pub async fn read_with_negative_offset_reads_tail(backend: &dyn StorageBackend) -> Result<()> {
    put(backend, "r.txt", b"abcdef").await?;
    ensure!(get_range(backend, "r.txt", -2, 0).await? == b"ef");
    Ok(())
}

pub async fn read_with_negative_offset_before_start_reads_whole_file(
    backend: &dyn StorageBackend,
) -> Result<()> {
    put(backend, "r.txt", b"abc").await?;
    ensure!(get_range(backend, "r.txt", -100, 0).await? == b"abc");
    Ok(())
}

pub async fn read_missing_file_fails(backend: &dyn StorageBackend) -> Result<()> {
    ensure!(
        backend
            .read(&Handle::new(Path::new("does-not-exist")), 0, 0)
            .await
            .is_err(),
        "reading a missing file must fail"
    );
    Ok(())
}

pub async fn path_exists_reflects_writes(backend: &dyn StorageBackend) -> Result<()> {
    ensure!(!backend.path_exists(Path::new("x.txt")).await);
    put(backend, "x.txt", b"x").await?;
    ensure!(backend.path_exists(Path::new("x.txt")).await);
    Ok(())
}

pub async fn is_file_and_is_dir_classify_correctly(backend: &dyn StorageBackend) -> Result<()> {
    put(backend, "f.txt", b"x").await?;
    put(backend, "d/x.txt", b"y").await?;

    ensure!(backend.is_file(Path::new("f.txt")).await);
    ensure!(!backend.is_dir(Path::new("f.txt")).await);

    ensure!(backend.is_dir(Path::new("d")).await);
    ensure!(!backend.is_file(Path::new("d")).await);

    ensure!(!backend.is_file(Path::new("missing")).await);
    ensure!(!backend.is_dir(Path::new("missing")).await);
    Ok(())
}

pub async fn lstat_reports_file_size(backend: &dyn StorageBackend) -> Result<()> {
    put(backend, "s.txt", b"12345").await?;
    let attr = backend.lstat(Path::new("s.txt")).await?;
    ensure!(attr.size == Some(5), "expected size 5, got {:?}", attr.size);
    Ok(())
}

pub async fn lstat_missing_path_fails(backend: &dyn StorageBackend) -> Result<()> {
    ensure!(
        backend.lstat(Path::new("missing")).await.is_err(),
        "lstat on a missing path must fail"
    );
    Ok(())
}

pub async fn list_dir_lists_immediate_children(backend: &dyn StorageBackend) -> Result<()> {
    put(backend, "d/f1.txt", b"a").await?;
    put(backend, "d/f2.txt", b"bb").await?;
    put(backend, "top.txt", b"ccc").await?;

    let entries = backend.list_dir(Path::new("d")).await?;
    ensure!(
        sorted_files(&entries) == vec!["d/f1.txt", "d/f2.txt"],
        "unexpected entries: {:?}",
        sorted_paths(&entries)
    );

    let root = backend.list_dir(Path::new("")).await?;
    ensure!(sorted_dirs(&root) == vec!["d"], "{:?}", sorted_paths(&root));
    ensure!(
        sorted_files(&root) == vec!["top.txt"],
        "{:?}",
        sorted_paths(&root)
    );
    Ok(())
}

pub async fn list_dir_reflects_removals(backend: &dyn StorageBackend) -> Result<()> {
    put(backend, "d/a.txt", b"a").await?;
    put(backend, "d/b.txt", b"b").await?;
    backend.remove(Path::new("d/a.txt")).await?;

    let entries = backend.list_dir(Path::new("d")).await?;
    ensure!(
        sorted_files(&entries) == vec!["d/b.txt"],
        "{:?}",
        sorted_paths(&entries)
    );
    Ok(())
}

pub async fn remove_file_is_idempotent(backend: &dyn StorageBackend) -> Result<()> {
    put(backend, "r.txt", b"x").await?;
    backend.remove(Path::new("r.txt")).await?;
    ensure!(!backend.path_exists(Path::new("r.txt")).await);
    // Removing an already-absent path must not fail.
    backend.remove(Path::new("r.txt")).await?;
    Ok(())
}

pub async fn rename_moves_file(backend: &dyn StorageBackend) -> Result<()> {
    put(backend, "from.txt", b"payload").await?;
    backend
        .rename(Path::new("from.txt"), Path::new("to.txt"))
        .await?;
    ensure!(get(backend, "to.txt").await? == b"payload");
    ensure!(!backend.path_exists(Path::new("from.txt")).await);
    Ok(())
}

pub async fn rename_replaces_existing_destination(backend: &dyn StorageBackend) -> Result<()> {
    put(backend, "src.txt", b"src").await?;
    put(backend, "dst.txt", b"dst").await?;
    backend
        .rename(Path::new("src.txt"), Path::new("dst.txt"))
        .await?;
    ensure!(get(backend, "dst.txt").await? == b"src");
    ensure!(!backend.path_exists(Path::new("src.txt")).await);
    Ok(())
}

pub async fn list_dir_recursive_finds_all_files(backend: &dyn StorageBackend) -> Result<()> {
    put(backend, "root/a.txt", b"a").await?;
    put(backend, "root/sub/b.txt", b"b").await?;
    put(backend, "root/sub/deep/c.txt", b"c").await?;

    let nodes = backend.list_dir_recursive(Path::new("root")).await?;
    ensure!(
        sorted_files(&nodes) == vec!["root/a.txt", "root/sub/b.txt", "root/sub/deep/c.txt"],
        "{:?}",
        sorted_paths(&nodes)
    );
    ensure!(sorted_dirs(&nodes).is_empty(), "{:?}", sorted_paths(&nodes));
    Ok(())
}

pub async fn create_dir_then_write_read_works(backend: &dyn StorageBackend) -> Result<()> {
    // `create_dir` is a no-op on flat object stores, but must succeed everywhere
    // and leave the backend able to store a file under the created prefix.
    backend.create_dir(Path::new("nested/dir")).await?;
    put(backend, "nested/dir/file.txt", b"content").await?;
    ensure!(get(backend, "nested/dir/file.txt").await? == b"content");
    Ok(())
}

pub async fn unicode_paths_roundtrip(backend: &dyn StorageBackend) -> Result<()> {
    let path = "rocket-🚀/mañana.txt";
    put(backend, path, b"unicode").await?;
    ensure!(get(backend, path).await? == b"unicode");
    ensure!(backend.is_file(Path::new(path)).await);
    Ok(())
}

pub async fn paths_with_spaces_roundtrip(backend: &dyn StorageBackend) -> Result<()> {
    let path = "with space/file name.txt";
    put(backend, path, b"spaces").await?;
    ensure!(get(backend, path).await? == b"spaces");
    let parent = backend.list_dir(Path::new("with space")).await?;
    ensure!(
        sorted_files(&parent) == vec!["with space/file name.txt"],
        "{:?}",
        sorted_paths(&parent)
    );
    Ok(())
}
