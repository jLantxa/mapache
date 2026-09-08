use std::{
    fmt::Display,
    io,
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};

use clap::Args;
use futures::{StreamExt, TryStreamExt};
use indicatif::{ProgressBar, ProgressStyle};
use serde::Serialize;

use crate::{
    backend::{BackendNode, StorageBackend, new_backend_with_prompt},
    commands::{GlobalArgs, ToExitCode, cleanup::CleanupHandler, with_repository_lock},
    common::{ID, error::MapacheError, global::GlobalOpts},
    fs::tree::SerializedNodeStream,
    repository::{
        packer::Packer,
        repo::{
            MANIFEST_PATH, REPO_DROPPED_EXTENSION, REPO_ECC_EXTENSION, REPO_TMP_EXTENSION,
            Repository,
        },
        snapshot::Snapshot,
        storage::SecureStorage,
    },
    ui::{self, SPINNER_TICK_CHARS, cli::color::Colorize, default_bar_draw_target},
    utils::{self, collections::ShardedIdSet},
};

/// Snapshots analyzed concurrently. Each one walks its own tree, so this bounds
/// the number of in-flight metadata reads against the backend.
const SNAPSHOT_ANALYSIS_CONCURRENCY: usize = 4;

/// Pack footers parsed concurrently when `--full` is requested.
const PACK_FOOTER_CONCURRENCY: usize = 8;

/// Width of the label column in the human readable report.
const LABEL_WIDTH: usize = 26;

#[derive(Debug, thiserror::Error)]
pub enum StatsError {
    #[error(transparent)]
    Repo(#[from] MapacheError),
    #[error(transparent)]
    Io(#[from] io::Error),
}

impl ToExitCode for StatsError {
    fn to_exit_code(&self) -> i32 {
        match self {
            StatsError::Repo(_) => 1,
            StatsError::Io(_) => 4,
        }
    }
}

#[derive(Args, Debug, Clone)]
#[clap(about = "Show repository statistics")]
pub struct CmdArgs {
    /// Parse pack footers for physical statistics (expensive)
    #[clap(long, default_value_t = false)]
    pub full: bool,
}

#[derive(Serialize)]
struct PacksOutput {
    count: usize,
    total_bytes: u64,
    ecc_count: usize,
    ecc_bytes: u64,
    other_count: usize,
    other_bytes: u64,
    parsed_footer: bool,
    footer_blob_count: Option<usize>,
    footer_encoded_bytes: Option<u64>,
    footer_raw_bytes: Option<u64>,
    footer_dangling_blobs: Option<usize>,
}

#[derive(Serialize)]
struct IndicesOutput {
    count: usize,
    total_bytes: u64,
    ecc_count: usize,
    ecc_bytes: u64,
    indexed_blobs: u64,
    indexed_raw_bytes: u64,
    indexed_encoded_bytes: u64,
}

#[derive(Serialize)]
struct SnapshotsOutput {
    count: usize,
    total_snapshot_bytes: u64,
    ecc_count: usize,
    ecc_bytes: u64,
    referenced_blobs: u64,
    referenced_data_blobs: u64,
    referenced_tree_blobs: u64,
    referenced_raw_bytes: u64,
    referenced_encoded_bytes: u64,
    referenced_raw_bytes_data: u64,
    referenced_encoded_bytes_data: u64,
    referenced_raw_bytes_tree: u64,
    referenced_encoded_bytes_tree: u64,
    unreferenced_blobs: u64,
    unreferenced_encoded_bytes: u64,
    compression_ratio_total: f32,
    compression_ratio_data: f32,
    compression_ratio_tree: f32,
    total_restorable_bytes: u64,
}

#[derive(Serialize)]
struct KeysOutput {
    count: usize,
    total_bytes: u64,
}

#[derive(Serialize)]
struct StatsOutput {
    packs: PacksOutput,
    indices: IndicesOutput,
    snapshots: SnapshotsOutput,
    keys: KeysOutput,
    manifest_bytes: u64,
    total_repo_bytes: u64,
}

/// Aggregated count and byte total for a group of repository files.
#[derive(Default, Clone, Copy)]
struct FileGroup {
    count: usize,
    bytes: u64,
}

impl FileGroup {
    fn push(&mut self, size: u64) {
        self.count += 1;
        self.bytes = self.bytes.saturating_add(size);
    }
}

/// Result of a single recursive listing of the objects directory.
#[derive(Default)]
struct ObjectScan {
    packs: FileGroup,
    ecc: FileGroup,
    /// `.tmp` / `.dropped` leftovers and anything else unrecognized.
    other: FileGroup,
    pack_ids: Vec<ID>,
}

/// Aggregated pack footer descriptors.
#[derive(Default)]
struct FooterScan {
    blobs: usize,
    encoded_bytes: u64,
    raw_bytes: u64,
    dangling: usize,
}

/// Compute compression ratios (raw / encoded) for total, data, and tree sizes.
/// Returns `(ratio_total, ratio_data, ratio_tree)`.
fn compression_ratios(
    raw_total: u64,
    enc_total: u64,
    raw_data: u64,
    enc_data: u64,
    raw_tree: u64,
    enc_tree: u64,
) -> (f32, f32, f32) {
    let ratio = |raw: u64, enc: u64| {
        if enc == 0 {
            0.0
        } else {
            raw as f32 / enc as f32
        }
    };
    (
        ratio(raw_total, enc_total),
        ratio(raw_data, enc_data),
        ratio(raw_tree, enc_tree),
    )
}

pub async fn run(global_args: &GlobalArgs, args: &CmdArgs) -> Result<(), StatsError> {
    with_repository_lock(
        global_args.auth_file.as_ref(),
        global_args.key.as_ref(),
        new_backend_with_prompt(global_args.backend_options(false))
            .await
            .map_err(|e| {
                StatsError::Io(io::Error::other(format!(
                    "failed to initialize backend: {}",
                    e.inner(),
                )))
            })?,
        global_args.to_repo_config(),
        false,
        global_args.retry_lock_duration,
        global_args.no_lock,
        |repo, secure_storage, lock_handle| async move {
            let cleanup_handler = CleanupHandler::new();
            cleanup_handler.add_lock(lock_handle);

            repo.reload_master_index().await?;

            stats_repository(
                repo.clone(),
                secure_storage,
                repo.backend(),
                args,
                global_args.json,
            )
            .await
        },
    )
    .await
}

/// Result of scanning a flat repository directory.
#[derive(Default)]
struct DirScan {
    files: FileGroup,
    ecc: FileGroup,
}

/// Lists a flat repository directory, deriving file sizes from the listing
/// itself instead of issuing one `lstat` round-trip per file.
/// ECC sidecars (`.ecc` extension) are classified separately.
async fn scan_dir(backend: &dyn StorageBackend, dir: &Path) -> Result<DirScan, StatsError> {
    let mut scan = DirScan::default();
    for node in backend.list_dir(dir).await? {
        if let BackendNode::File(path, size) = node {
            match path.extension().and_then(|e| e.to_str()) {
                Some(REPO_ECC_EXTENSION) => scan.ecc.push(size),
                _ => scan.files.push(size),
            }
        }
    }
    Ok(scan)
}

/// Recursively lists the objects directory once and classifies every entry as a
/// pack, an ECC sidecar, or a leftover file.
async fn scan_objects(
    backend: &dyn StorageBackend,
    dir: &Path,
    collect_pack_ids: bool,
) -> Result<ObjectScan, StatsError> {
    let mut scan = ObjectScan::default();

    for node in backend.list_dir_recursive(dir).await? {
        let BackendNode::File(path, size) = node else {
            continue;
        };

        let name = path
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or_default();
        if let Ok(id) = ID::from_hex(name) {
            scan.packs.push(size);
            if collect_pack_ids {
                scan.pack_ids.push(id);
            }
            continue;
        }

        match path.extension().and_then(|e| e.to_str()) {
            Some(REPO_ECC_EXTENSION) => scan.ecc.push(size),
            Some(REPO_TMP_EXTENSION | REPO_DROPPED_EXTENSION) => scan.other.push(size),
            _ => scan.other.push(size),
        }
    }

    Ok(scan)
}

/// Parses every pack footer concurrently and aggregates the descriptors.
async fn scan_pack_footers(
    repo: Arc<Repository>,
    backend: Arc<dyn StorageBackend>,
    secure_storage: Arc<SecureStorage>,
    pack_ids: &[ID],
    spinner: &ProgressBar,
) -> Result<FooterScan, StatsError> {
    let total = pack_ids.len();
    let done = AtomicUsize::new(0);
    spinner.set_message(format!("parsing pack footers 0/{total}"));

    let partials: Vec<FooterScan> = futures::stream::iter(pack_ids.iter().copied())
        .map(|pack_id| {
            let repo = repo.clone();
            let backend = backend.clone();
            let secure_storage = secure_storage.clone();
            let done = &done;
            async move {
                let descriptors = Packer::parse_pack_footer(
                    repo.as_ref(),
                    backend.as_ref(),
                    secure_storage.as_ref(),
                    &pack_id,
                    secure_storage.nonce_at_end(),
                )
                .await
                .map_err(|e| {
                    StatsError::Repo(MapacheError::Internal(format!(
                        "failed to parse footer for pack {}: {}",
                        pack_id.to_hex(),
                        e.inner()
                    )))
                })?;

                let index = repo.index();
                let mut scan = FooterScan::default();
                for d in descriptors.iter() {
                    scan.blobs += 1;
                    scan.encoded_bytes = scan.encoded_bytes.saturating_add(d.length as u64);
                    scan.raw_bytes = scan.raw_bytes.saturating_add(d.raw_length as u64);
                    if !index.contains(&d.id) {
                        scan.dangling += 1;
                    }
                }

                let n = done.fetch_add(1, Ordering::Relaxed) + 1;
                spinner.set_message(format!("parsing pack footers {n}/{total}"));
                Ok::<_, StatsError>(scan)
            }
        })
        .buffer_unordered(PACK_FOOTER_CONCURRENCY)
        .try_collect()
        .await?;

    Ok(partials
        .into_iter()
        .fold(FooterScan::default(), |mut acc, s| {
            acc.blobs += s.blobs;
            acc.encoded_bytes = acc.encoded_bytes.saturating_add(s.encoded_bytes);
            acc.raw_bytes = acc.raw_bytes.saturating_add(s.raw_bytes);
            acc.dangling += s.dangling;
            acc
        }))
}

async fn stats_repository(
    repo: Arc<Repository>,
    secure_storage: Arc<SecureStorage>,
    backend: Arc<dyn StorageBackend>,
    args: &CmdArgs,
    json_out: bool,
) -> Result<(), StatsError> {
    let spinner = ProgressBar::new_spinner();
    spinner.set_draw_target(default_bar_draw_target());
    spinner.set_style(
        ProgressStyle::default_spinner()
            .template("{spinner:.cyan} Collecting stats... ({msg})")
            .expect("invalid progress bar template for stats spinner")
            .tick_chars(SPINNER_TICK_CHARS),
    );
    spinner.enable_steady_tick(GlobalOpts::progress_refresh_interval());
    spinner.set_message("listing repository files");

    // Every directory is listed once, concurrently, and sizes come straight from
    // the listing rather than a per-file lstat.
    let backend_ref = backend.as_ref();
    let (objects, index_scan, snapshot_scan, keys, manifest_size) = tokio::try_join!(
        scan_objects(backend_ref, repo.objects_path(), args.full),
        scan_dir(backend_ref, repo.index_path()),
        scan_dir(backend_ref, repo.snapshot_path()),
        scan_dir(backend_ref, repo.keys_path()),
        async {
            Ok(backend_ref
                .lstat(Path::new(MANIFEST_PATH))
                .await?
                .size
                .unwrap_or(0))
        },
    )?;

    let total_size = objects
        .packs
        .bytes
        .saturating_add(objects.ecc.bytes)
        .saturating_add(objects.other.bytes)
        .saturating_add(index_scan.files.bytes)
        .saturating_add(index_scan.ecc.bytes)
        .saturating_add(snapshot_scan.files.bytes)
        .saturating_add(snapshot_scan.ecc.bytes)
        .saturating_add(keys.files.bytes)
        .saturating_add(manifest_size);

    // Index-level summary (in memory, no I/O).
    spinner.set_message("scanning index");
    let mut indexed_blobs = 0u64;
    let mut indexed_encoded = 0u64;
    let mut indexed_raw = 0u64;
    repo.index().for_each_id(|_id, loc| {
        indexed_blobs += 1;
        indexed_encoded = indexed_encoded.saturating_add(loc.length as u64);
        indexed_raw = indexed_raw.saturating_add(loc.raw_length as u64);
    });

    // Snapshot-derived summary (index-only).
    let snap_stats = analyze_snapshots(repo.clone(), &spinner).await?;

    let footers = if args.full {
        Some(
            scan_pack_footers(
                repo.clone(),
                backend.clone(),
                secure_storage.clone(),
                &objects.pack_ids,
                &spinner,
            )
            .await?,
        )
    } else {
        None
    };

    spinner.finish_and_clear();

    let (ratio_total, ratio_data, ratio_tree) = compression_ratios(
        snap_stats.total_raw_data_size,
        snap_stats.total_encoded_data_size,
        snap_stats.total_raw_data_size_data,
        snap_stats.total_encoded_data_size_data,
        snap_stats.total_raw_data_size_tree,
        snap_stats.total_encoded_data_size_tree,
    );
    let unreferenced_blobs = indexed_blobs.saturating_sub(snap_stats.num_referenced_blobs);
    let unreferenced_bytes = indexed_encoded.saturating_sub(snap_stats.total_encoded_data_size);

    if json_out {
        let out = StatsOutput {
            packs: PacksOutput {
                count: objects.packs.count,
                total_bytes: objects.packs.bytes,
                ecc_count: objects.ecc.count,
                ecc_bytes: objects.ecc.bytes,
                other_count: objects.other.count,
                other_bytes: objects.other.bytes,
                parsed_footer: args.full,
                footer_blob_count: footers.as_ref().map(|f| f.blobs),
                footer_encoded_bytes: footers.as_ref().map(|f| f.encoded_bytes),
                footer_raw_bytes: footers.as_ref().map(|f| f.raw_bytes),
                footer_dangling_blobs: footers.as_ref().map(|f| f.dangling),
            },
            indices: IndicesOutput {
                count: index_scan.files.count,
                total_bytes: index_scan.files.bytes,
                ecc_count: index_scan.ecc.count,
                ecc_bytes: index_scan.ecc.bytes,
                indexed_blobs,
                indexed_raw_bytes: indexed_raw,
                indexed_encoded_bytes: indexed_encoded,
            },
            snapshots: SnapshotsOutput {
                count: snapshot_scan.files.count,
                total_snapshot_bytes: snapshot_scan.files.bytes,
                ecc_count: snapshot_scan.ecc.count,
                ecc_bytes: snapshot_scan.ecc.bytes,
                referenced_blobs: snap_stats.num_referenced_blobs,
                referenced_data_blobs: snap_stats.num_referenced_data_blobs,
                referenced_tree_blobs: snap_stats.num_referenced_tree_blobs,
                referenced_raw_bytes: snap_stats.total_raw_data_size,
                referenced_encoded_bytes: snap_stats.total_encoded_data_size,
                referenced_raw_bytes_data: snap_stats.total_raw_data_size_data,
                referenced_encoded_bytes_data: snap_stats.total_encoded_data_size_data,
                referenced_raw_bytes_tree: snap_stats.total_raw_data_size_tree,
                referenced_encoded_bytes_tree: snap_stats.total_encoded_data_size_tree,
                unreferenced_blobs,
                unreferenced_encoded_bytes: unreferenced_bytes,
                compression_ratio_total: ratio_total,
                compression_ratio_data: ratio_data,
                compression_ratio_tree: ratio_tree,
                total_restorable_bytes: snap_stats.total_restorable_bytes,
            },
            keys: KeysOutput {
                count: keys.files.count,
                total_bytes: keys.files.bytes,
            },
            manifest_bytes: manifest_size,
            total_repo_bytes: total_size,
        };

        ui::json::emit_static("stats", &out);
        return Ok(());
    }

    section("Packs");
    row(
        "Pack files",
        count_and_size(objects.packs.count, "pack", "packs", objects.packs.bytes),
    );
    if objects.ecc.count > 0 {
        row(
            "ECC sidecars",
            count_and_size(objects.ecc.count, "file", "files", objects.ecc.bytes),
        );
    }
    if objects.other.count > 0 {
        row(
            "Leftover files",
            count_and_size(objects.other.count, "file", "files", objects.other.bytes),
        );
    }
    if let Some(footers) = &footers {
        row(
            "Footer blobs",
            utils::format_count(footers.blobs, "blob", "blobs"),
        );
        row(
            "Footer raw / encoded",
            format!(
                "{} / {}",
                utils::format_size_binary(footers.raw_bytes, 3),
                utils::format_size_binary(footers.encoded_bytes, 3)
            ),
        );
        let dangling = utils::format_count(footers.dangling, "blob", "blobs");
        row(
            "Footer dangling blobs",
            if footers.dangling > 0 {
                dangling.yellow().to_string()
            } else {
                dangling
            },
        );
    }

    ui::cli::log!();
    section("Index");
    row(
        "Index files",
        count_and_size(
            index_scan.files.count,
            "file",
            "files",
            index_scan.files.bytes,
        ),
    );
    if index_scan.ecc.count > 0 {
        row(
            "ECC sidecars",
            count_and_size(index_scan.ecc.count, "file", "files", index_scan.ecc.bytes),
        );
    }
    row(
        "Indexed blobs",
        utils::format_count(indexed_blobs, "blob", "blobs"),
    );
    row(
        "Raw / encoded",
        raw_over_encoded(indexed_raw, indexed_encoded),
    );

    ui::cli::log!();
    section("Snapshots");
    row(
        "Snapshots",
        count_and_size(
            snapshot_scan.files.count,
            "snapshot",
            "snapshots",
            snapshot_scan.files.bytes,
        ),
    );
    if snapshot_scan.ecc.count > 0 {
        row(
            "ECC sidecars",
            count_and_size(
                snapshot_scan.ecc.count,
                "file",
                "files",
                snapshot_scan.ecc.bytes,
            ),
        );
    }
    row(
        "Referenced blobs",
        format!(
            "{} (data: {}, tree: {})",
            snap_stats.num_referenced_blobs,
            snap_stats.num_referenced_data_blobs,
            snap_stats.num_referenced_tree_blobs
        ),
    );
    row(
        "Raw / encoded",
        raw_over_encoded(
            snap_stats.total_raw_data_size,
            snap_stats.total_encoded_data_size,
        ),
    );
    row(
        "Data (raw / encoded)",
        raw_over_encoded(
            snap_stats.total_raw_data_size_data,
            snap_stats.total_encoded_data_size_data,
        ),
    );
    row(
        "Tree (raw / encoded)",
        raw_over_encoded(
            snap_stats.total_raw_data_size_tree,
            snap_stats.total_encoded_data_size_tree,
        ),
    );
    row(
        "Compression ratio",
        format!("{ratio_total:.2}x (data: {ratio_data:.2}x, tree: {ratio_tree:.2}x)"),
    );
    row(
        "Restorable size",
        utils::format_size_binary(snap_stats.total_restorable_bytes, 3),
    );
    if unreferenced_blobs > 0 {
        row(
            "Unreferenced blobs",
            format!(
                "{} ({} reclaimable)",
                utils::format_count(unreferenced_blobs, "blob", "blobs"),
                utils::format_size_binary(unreferenced_bytes, 3)
            ),
        );
    }

    ui::cli::log!();
    section("Keys");
    row(
        "Key files",
        count_and_size(keys.files.count, "key", "keys", keys.files.bytes),
    );

    ui::cli::log!();
    section("Repository");
    row("Manifest", utils::format_size_binary(manifest_size, 3));
    row(
        "Total size",
        utils::format_size_binary(total_size, 3).bold().to_string(),
    );

    Ok(())
}

/// Prints a section title.
fn section(title: &str) {
    ui::cli::log!("{}", format!("{title}:").bold());
}

/// Prints an aligned `label  value` row. The label is padded before styling so
/// ANSI escapes do not count towards the column width.
fn row(label: &str, value: impl Display) {
    ui::cli::log!(
        "  {} {}",
        format!("{:<width$}", label, width = LABEL_WIDTH).dimmed(),
        value
    );
}

fn count_and_size(count: usize, singular: &str, plural: &str, bytes: u64) -> String {
    format!(
        "{} ({})",
        utils::format_count(count, singular, plural),
        utils::format_size_binary(bytes, 3)
    )
}

fn raw_over_encoded(raw: u64, encoded: u64) -> String {
    format!(
        "{} / {}",
        utils::format_size_binary(raw, 3),
        utils::format_size_binary(encoded, 3)
    )
}

#[derive(Default)]
struct SnapshotAnalysis {
    total_raw_data_size: u64,
    total_encoded_data_size: u64,
    num_referenced_blobs: u64,
    num_referenced_data_blobs: u64,
    num_referenced_tree_blobs: u64,

    total_raw_data_size_data: u64,
    total_encoded_data_size_data: u64,
    total_raw_data_size_tree: u64,
    total_encoded_data_size_tree: u64,
    total_restorable_bytes: u64,
}

impl SnapshotAnalysis {
    fn add_blob(&mut self, is_tree: bool, raw: u64, encoded: u64) {
        self.num_referenced_blobs = self.num_referenced_blobs.saturating_add(1);
        self.total_raw_data_size = self.total_raw_data_size.saturating_add(raw);
        self.total_encoded_data_size = self.total_encoded_data_size.saturating_add(encoded);
        if is_tree {
            self.num_referenced_tree_blobs = self.num_referenced_tree_blobs.saturating_add(1);
            self.total_raw_data_size_tree = self.total_raw_data_size_tree.saturating_add(raw);
            self.total_encoded_data_size_tree =
                self.total_encoded_data_size_tree.saturating_add(encoded);
        } else {
            self.num_referenced_data_blobs = self.num_referenced_data_blobs.saturating_add(1);
            self.total_raw_data_size_data = self.total_raw_data_size_data.saturating_add(raw);
            self.total_encoded_data_size_data =
                self.total_encoded_data_size_data.saturating_add(encoded);
        }
    }

    fn merge(&mut self, other: SnapshotAnalysis) {
        self.total_raw_data_size = self
            .total_raw_data_size
            .saturating_add(other.total_raw_data_size);
        self.total_encoded_data_size = self
            .total_encoded_data_size
            .saturating_add(other.total_encoded_data_size);
        self.num_referenced_blobs = self
            .num_referenced_blobs
            .saturating_add(other.num_referenced_blobs);
        self.num_referenced_data_blobs = self
            .num_referenced_data_blobs
            .saturating_add(other.num_referenced_data_blobs);
        self.num_referenced_tree_blobs = self
            .num_referenced_tree_blobs
            .saturating_add(other.num_referenced_tree_blobs);
        self.total_raw_data_size_data = self
            .total_raw_data_size_data
            .saturating_add(other.total_raw_data_size_data);
        self.total_encoded_data_size_data = self
            .total_encoded_data_size_data
            .saturating_add(other.total_encoded_data_size_data);
        self.total_raw_data_size_tree = self
            .total_raw_data_size_tree
            .saturating_add(other.total_raw_data_size_tree);
        self.total_encoded_data_size_tree = self
            .total_encoded_data_size_tree
            .saturating_add(other.total_encoded_data_size_tree);
        self.total_restorable_bytes = self
            .total_restorable_bytes
            .saturating_add(other.total_restorable_bytes);
    }
}

async fn analyze_snapshots(
    repo: Arc<Repository>,
    spinner: &ProgressBar,
) -> Result<SnapshotAnalysis, StatsError> {
    let snapshot_ids = repo.list_snapshot_ids().await?;
    let total = snapshot_ids.len();
    if total == 0 {
        return Ok(SnapshotAnalysis::default());
    }

    // Shared so a blob referenced by several snapshots is only counted once.
    let visited = Arc::new(ShardedIdSet::new());
    let done = AtomicUsize::new(0);
    spinner.set_message(format!("analyzing snapshots 0/{total}"));

    let partials: Vec<SnapshotAnalysis> = futures::stream::iter(snapshot_ids)
        .map(|id| {
            let repo = repo.clone();
            let visited = visited.clone();
            let done = &done;
            async move {
                let snapshot = repo.load_snapshot(&id, None).await?;
                let analysis = analyze_snapshot(repo, snapshot, visited.as_ref()).await?;
                let n = done.fetch_add(1, Ordering::Relaxed) + 1;
                spinner.set_message(format!("analyzing snapshots {n}/{total}"));
                Ok::<_, StatsError>(analysis)
            }
        })
        .buffer_unordered(SNAPSHOT_ANALYSIS_CONCURRENCY)
        .try_collect()
        .await?;

    let mut totals = SnapshotAnalysis::default();
    for partial in partials {
        totals.merge(partial);
    }
    Ok(totals)
}

async fn analyze_snapshot(
    repo: Arc<Repository>,
    snapshot: Snapshot,
    visited: &ShardedIdSet,
) -> Result<SnapshotAnalysis, StatsError> {
    let mut acc = SnapshotAnalysis {
        // Sum of snapshot raw bytes, not deduped.
        total_restorable_bytes: snapshot.size(),
        ..Default::default()
    };
    let index = repo.index();

    if visited.insert(snapshot.tree)
        && let Some(locator) = index.get(&snapshot.tree).await
    {
        acc.add_blob(true, locator.raw_length as u64, locator.length as u64);
    }

    let mut stream = SerializedNodeStream::new(
        repo.clone(),
        Some(snapshot.tree),
        PathBuf::new(),
        None,
        None,
    )
    .await?;

    while let Some(res) = stream.next().await {
        let (_path, stream_node_res_outer) = res?;
        let node = stream_node_res_outer?.node;

        if let Some(tree_id) = &node.tree
            && visited.insert(*tree_id)
            && let Some(locator) = index.get(tree_id).await
        {
            acc.add_blob(true, locator.raw_length as u64, locator.length as u64);
        }

        if let Some(blobs) = node.blobs {
            for blob_id in blobs {
                if visited.insert(blob_id)
                    && let Some(locator) = index.get(&blob_id).await
                {
                    acc.add_blob(false, locator.raw_length as u64, locator.length as u64);
                }
            }
        }
    }

    Ok(acc)
}
