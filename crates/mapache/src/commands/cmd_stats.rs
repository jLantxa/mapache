use std::{
    collections::HashMap,
    fmt::Display,
    io,
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
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
    #[error("stats interrupted by user")]
    Interrupted,
}

impl ToExitCode for StatsError {
    fn to_exit_code(&self) -> i32 {
        match self {
            StatsError::Repo(_) => 1,
            StatsError::Io(_) => 4,
            StatsError::Interrupted => 130,
        }
    }
}

#[derive(Args, Debug, Clone)]
#[clap(
    about = "Show repository statistics",
    long_about = "Show statistics about the repository: number of blobs, snapshots,\n\
        packs, raw and encoded sizes, deduplication savings and ECC overhead.\n\n\
        Use --full to also parse pack footers and report physical statistics,\n\
        which is more accurate but slower on large repositories."
)]
pub struct CmdArgs {
    /// Parse pack footers for physical statistics (expensive)
    #[clap(long, default_value_t = false)]
    pub full: bool,
}

/// Aggregated physical statistics parsed from every pack footer.
#[derive(Debug, Clone, Copy, Default)]
pub struct FooterReport {
    pub blobs: usize,
    pub encoded_bytes: u64,
    pub raw_bytes: u64,
    pub dangling: usize,
    pub duplicate_blobs: usize,
}

/// Everything gathered by [`collect_stats`], independent of how it is
/// presented. Built once and shared by the CLI renderer and the TUI screen.
#[derive(Debug, Clone, Default)]
pub struct StatsReport {
    // Packs (objects directory).
    pub packs_count: usize,
    pub packs_bytes: u64,
    pub packs_ecc_count: usize,
    pub packs_ecc_bytes: u64,
    pub leftover_count: usize,
    pub leftover_bytes: u64,
    /// Present only when collected with `full = true`.
    pub footers: Option<FooterReport>,

    // Index directory and index contents.
    pub index_count: usize,
    pub index_bytes: u64,
    pub index_ecc_count: usize,
    pub index_ecc_bytes: u64,
    pub indexed_blobs: u64,
    pub indexed_raw_bytes: u64,
    pub indexed_encoded_bytes: u64,

    // Snapshots directory and referenced contents.
    pub snapshots_count: usize,
    pub snapshots_bytes: u64,
    pub snapshots_ecc_count: usize,
    pub snapshots_ecc_bytes: u64,
    pub referenced_blobs: u64,
    pub referenced_data_blobs: u64,
    pub referenced_tree_blobs: u64,
    pub referenced_raw_bytes: u64,
    pub referenced_encoded_bytes: u64,
    pub referenced_raw_bytes_data: u64,
    pub referenced_encoded_bytes_data: u64,
    pub referenced_raw_bytes_tree: u64,
    pub referenced_encoded_bytes_tree: u64,
    pub unreferenced_blobs: u64,
    pub unreferenced_encoded_bytes: u64,
    pub total_restorable_bytes: u64,

    // Compression ratios (raw / encoded).
    pub ratio_total: f32,
    pub ratio_data: f32,
    pub ratio_tree: f32,

    // Keys and manifest.
    pub keys_count: usize,
    pub keys_bytes: u64,
    pub manifest_bytes: u64,
    pub total_repo_bytes: u64,

    /// Whether pack footers were parsed.
    pub full: bool,

    /// Physical pack IDs from the objects listing. Kept so the TUI can add
    /// footer data to an existing report without re-listing the directory.
    pub pack_ids: Vec<ID>,
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
    duplicate_blobs: usize,
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

/// Clears the spinner when the command exits with an error. A single
/// "interrupted by user" message is emitted at info level for interrupt errors.
fn finish_spinner_on_error(spinner: &ProgressBar, error: &StatsError) {
    if matches!(error, StatsError::Interrupted) {
        tracing::info!(target: "stats", "Stats interrupted by user");
    }
    spinner.finish_and_clear();
}

/// Returns `Err(StatsError::Interrupted)` if the shutdown flag has been set.
#[inline]
fn check_interrupted(signal: &AtomicBool) -> Result<(), StatsError> {
    if signal.load(Ordering::Acquire) {
        Err(StatsError::Interrupted)
    } else {
        Ok(())
    }
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
            let shutdown_signal = cleanup_handler.interrupted.clone();

            repo.reload_master_index().await?;

            stats_repository(
                repo.clone(),
                secure_storage,
                repo.backend(),
                args,
                global_args.json,
                shutdown_signal,
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
/// pack, an ECC sidecar, or a leftover file. Pack IDs are collected so callers
/// can parse pack footers later without listing the directory again.
async fn scan_objects(backend: &dyn StorageBackend, dir: &Path) -> Result<ObjectScan, StatsError> {
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
            scan.pack_ids.push(id);
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
    shutdown_signal: Arc<AtomicBool>,
    on_progress: ProgressCallback<'_>,
) -> Result<FooterScan, StatsError> {
    let total = pack_ids.len();
    let done = AtomicUsize::new(0);
    on_progress(format!("parsing pack footers 0/{total}"));

    let partials: Vec<FooterScan> = futures::stream::iter(pack_ids.iter().copied())
        .map(|pack_id| {
            let repo = repo.clone();
            let backend = backend.clone();
            let secure_storage = secure_storage.clone();
            let shutdown_signal = shutdown_signal.clone();
            let done = &done;
            async move {
                check_interrupted(&shutdown_signal)?;
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
                let mut seen = HashMap::new();
                for d in descriptors.iter() {
                    // Count duplicates BEFORE updating `seen` so the metric is the
                    // number of phantom descriptor entries (extra footer lines).
                    let entry = seen.entry(d.id).or_insert(0_usize);
                    *entry += 1;
                    if *entry > 1 {
                        // Same ID twice in this pack's footer.
                        scan.duplicate_blobs += 1;
                    } else if let Some(loc) = index.get(&d.id).await {
                        if loc.pack_id != pack_id || loc.offset != d.offset {
                            // Phantom: the authoritative copy lives in another pack
                            // or at another offset.
                            scan.duplicate_blobs += 1;
                        }
                    } else {
                        // No persisted index entry, resident or cold. Both counters
                        // compare a persisted pack footer against the persisted
                        // index, so `contains_exact` would be wrong twice over: it
                        // reports `false` for cold-only blobs, and it trusts the
                        // in-memory `pending_blobs` set. The latter would mask a
                        // genuinely missing blob, and make the answer depend on
                        // whether a backup happens to be running in this process.
                        scan.dangling += 1;
                    }
                    scan.blobs += 1;
                    scan.encoded_bytes = scan.encoded_bytes.saturating_add(d.length as u64);
                    scan.raw_bytes = scan.raw_bytes.saturating_add(d.raw_length as u64);
                }

                let n = done.fetch_add(1, Ordering::Relaxed) + 1;
                on_progress(format!("parsing pack footers {n}/{total}"));
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
            acc.duplicate_blobs += s.duplicate_blobs;
            acc
        }))
}

/// Type-erased progress callback used by [`collect_stats`]. Callers can route the
/// messages to a CLI spinner, a TUI toast, or nowhere at all.
pub type ProgressCallback<'a> = &'a (dyn Fn(String) + Sync);

/// Collects every statistic reported by `mapache stats` without printing
/// anything, so both the CLI renderer and the TUI screen can share it.
pub async fn collect_stats(
    repo: Arc<Repository>,
    secure_storage: Arc<SecureStorage>,
    backend: Arc<dyn StorageBackend>,
    full: bool,
    shutdown_signal: Arc<AtomicBool>,
    on_progress: ProgressCallback<'_>,
) -> Result<StatsReport, StatsError> {
    on_progress("listing repository files".to_string());

    // Every directory is listed once, concurrently, and sizes come straight from
    // the listing rather than a per-file lstat.
    let backend_ref = backend.as_ref();
    let (objects, index_scan, snapshot_scan, keys, manifest_size) = tokio::try_join!(
        // The pack IDs are always collected: the TUI reuses them to add footer
        // data to this report without listing the objects directory again.
        scan_objects(backend_ref, repo.objects_path()),
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

    // Index-level summary (walks every index, hot and cold).
    on_progress("scanning index".to_string());
    let mut indexed_blobs = 0u64;
    let mut indexed_encoded = 0u64;
    let mut indexed_raw = 0u64;
    repo.index()
        .for_each_id(|_id, loc| {
            indexed_blobs += 1;
            indexed_encoded = indexed_encoded.saturating_add(loc.length as u64);
            indexed_raw = indexed_raw.saturating_add(loc.raw_length as u64);
        })
        .await;

    // Snapshot-derived summary (index-only).
    let snap_stats = analyze_snapshots(repo.clone(), shutdown_signal.clone(), on_progress).await?;

    let footers = if full {
        Some(
            scan_pack_footers(
                repo.clone(),
                backend.clone(),
                secure_storage.clone(),
                &objects.pack_ids,
                shutdown_signal.clone(),
                on_progress,
            )
            .await?,
        )
    } else {
        None
    };

    // Blobs tracked in the index but not referenced by any snapshot. With the
    // index now covering hot and cold files, this converges to zero after a
    // clean run (default tolerance only tolerates garbage in a pack, it never
    // counts blobs the repo has forgotten).
    let unreferenced_blobs = indexed_blobs.saturating_sub(snap_stats.num_referenced_blobs);
    let unreferenced_encoded_bytes =
        indexed_encoded.saturating_sub(snap_stats.total_encoded_data_size);

    let (ratio_total, ratio_data, ratio_tree) = compression_ratios(
        snap_stats.total_raw_data_size,
        snap_stats.total_encoded_data_size,
        snap_stats.total_raw_data_size_data,
        snap_stats.total_encoded_data_size_data,
        snap_stats.total_raw_data_size_tree,
        snap_stats.total_encoded_data_size_tree,
    );

    Ok(StatsReport {
        packs_count: objects.packs.count,
        packs_bytes: objects.packs.bytes,
        packs_ecc_count: objects.ecc.count,
        packs_ecc_bytes: objects.ecc.bytes,
        leftover_count: objects.other.count,
        leftover_bytes: objects.other.bytes,
        footers: footers.map(|f| FooterReport {
            blobs: f.blobs,
            encoded_bytes: f.encoded_bytes,
            raw_bytes: f.raw_bytes,
            dangling: f.dangling,
            duplicate_blobs: f.duplicate_blobs,
        }),
        index_count: index_scan.files.count,
        index_bytes: index_scan.files.bytes,
        index_ecc_count: index_scan.ecc.count,
        index_ecc_bytes: index_scan.ecc.bytes,
        indexed_blobs,
        indexed_raw_bytes: indexed_raw,
        indexed_encoded_bytes: indexed_encoded,
        snapshots_count: snapshot_scan.files.count,
        snapshots_bytes: snapshot_scan.files.bytes,
        snapshots_ecc_count: snapshot_scan.ecc.count,
        snapshots_ecc_bytes: snapshot_scan.ecc.bytes,
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
        unreferenced_encoded_bytes,
        total_restorable_bytes: snap_stats.total_restorable_bytes,
        ratio_total,
        ratio_data,
        ratio_tree,
        keys_count: keys.files.count,
        keys_bytes: keys.files.bytes,
        manifest_bytes: manifest_size,
        total_repo_bytes: total_size,
        full,
        pack_ids: objects.pack_ids,
    })
}

/// Parses every pack footer and returns the aggregated physical statistics,
/// without re-scanning the index or the snapshots. Used to add footer data to
/// an already-collected [`StatsReport`] (the TUI's `f` toggle), reusing the
/// pack IDs already gathered by [`collect_stats`].
pub async fn collect_footers(
    repo: Arc<Repository>,
    secure_storage: Arc<SecureStorage>,
    backend: Arc<dyn StorageBackend>,
    pack_ids: &[ID],
    shutdown_signal: Arc<AtomicBool>,
    on_progress: ProgressCallback<'_>,
) -> Result<FooterReport, StatsError> {
    on_progress("parsing pack footers".to_string());
    let footers = scan_pack_footers(
        repo,
        backend,
        secure_storage,
        pack_ids,
        shutdown_signal,
        on_progress,
    )
    .await?;

    Ok(FooterReport {
        blobs: footers.blobs,
        encoded_bytes: footers.encoded_bytes,
        raw_bytes: footers.raw_bytes,
        dangling: footers.dangling,
        duplicate_blobs: footers.duplicate_blobs,
    })
}

async fn stats_repository(
    repo: Arc<Repository>,
    secure_storage: Arc<SecureStorage>,
    backend: Arc<dyn StorageBackend>,
    args: &CmdArgs,
    json_out: bool,
    shutdown_signal: Arc<AtomicBool>,
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

    let report = collect_stats(
        repo,
        secure_storage,
        backend,
        args.full,
        shutdown_signal,
        &|msg| spinner.set_message(msg),
    )
    .await
    .inspect_err(|e| finish_spinner_on_error(&spinner, e))?;

    spinner.finish_and_clear();

    if json_out {
        ui::json::emit_static("stats", &report.to_output());
        return Ok(());
    }

    render_stats(&report);
    Ok(())
}

impl StatsReport {
    /// Builds the JSON representation emitted by `--json`.
    fn to_output(&self) -> StatsOutput {
        StatsOutput {
            packs: PacksOutput {
                count: self.packs_count,
                total_bytes: self.packs_bytes,
                ecc_count: self.packs_ecc_count,
                ecc_bytes: self.packs_ecc_bytes,
                other_count: self.leftover_count,
                other_bytes: self.leftover_bytes,
                parsed_footer: self.footers.is_some(),
                footer_blob_count: self.footers.as_ref().map(|f| f.blobs),
                footer_encoded_bytes: self.footers.as_ref().map(|f| f.encoded_bytes),
                footer_raw_bytes: self.footers.as_ref().map(|f| f.raw_bytes),
                footer_dangling_blobs: self.footers.as_ref().map(|f| f.dangling),
            },
            indices: IndicesOutput {
                count: self.index_count,
                total_bytes: self.index_bytes,
                ecc_count: self.index_ecc_count,
                ecc_bytes: self.index_ecc_bytes,
                indexed_blobs: self.indexed_blobs,
                indexed_raw_bytes: self.indexed_raw_bytes,
                indexed_encoded_bytes: self.indexed_encoded_bytes,
            },
            snapshots: SnapshotsOutput {
                count: self.snapshots_count,
                total_snapshot_bytes: self.snapshots_bytes,
                ecc_count: self.snapshots_ecc_count,
                ecc_bytes: self.snapshots_ecc_bytes,
                referenced_blobs: self.referenced_blobs,
                referenced_data_blobs: self.referenced_data_blobs,
                referenced_tree_blobs: self.referenced_tree_blobs,
                referenced_raw_bytes: self.referenced_raw_bytes,
                referenced_encoded_bytes: self.referenced_encoded_bytes,
                referenced_raw_bytes_data: self.referenced_raw_bytes_data,
                referenced_encoded_bytes_data: self.referenced_encoded_bytes_data,
                referenced_raw_bytes_tree: self.referenced_raw_bytes_tree,
                referenced_encoded_bytes_tree: self.referenced_encoded_bytes_tree,
                unreferenced_blobs: self.unreferenced_blobs,
                unreferenced_encoded_bytes: self.unreferenced_encoded_bytes,
                compression_ratio_total: self.ratio_total,
                compression_ratio_data: self.ratio_data,
                compression_ratio_tree: self.ratio_tree,
                total_restorable_bytes: self.total_restorable_bytes,
            },
            keys: KeysOutput {
                count: self.keys_count,
                total_bytes: self.keys_bytes,
            },
            manifest_bytes: self.manifest_bytes,
            total_repo_bytes: self.total_repo_bytes,
        }
    }
}

/// Prints the human-readable statistics report.
fn render_stats(report: &StatsReport) {
    section("Packs");
    row(
        "Pack files",
        count_and_size(report.packs_count, "pack", "packs", report.packs_bytes),
    );
    if report.packs_ecc_count > 0 {
        row(
            "ECC sidecars",
            count_and_size(
                report.packs_ecc_count,
                "file",
                "files",
                report.packs_ecc_bytes,
            ),
        );
    }
    if report.leftover_count > 0 {
        row(
            "Leftover files",
            count_and_size(
                report.leftover_count,
                "file",
                "files",
                report.leftover_bytes,
            ),
        );
    }
    if let Some(footers) = &report.footers {
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
        if footers.duplicate_blobs > 0 {
            let dupes = utils::format_count(footers.duplicate_blobs, "blob", "blobs");
            row("Footer duplicate blobs", dupes.red().bold().to_string());
        }
    }

    ui::cli::log!();
    section("Index");
    row(
        "Index files",
        count_and_size(report.index_count, "file", "files", report.index_bytes),
    );
    if report.index_ecc_count > 0 {
        row(
            "ECC sidecars",
            count_and_size(
                report.index_ecc_count,
                "file",
                "files",
                report.index_ecc_bytes,
            ),
        );
    }
    row(
        "Indexed blobs",
        utils::format_count(report.indexed_blobs, "blob", "blobs"),
    );
    row(
        "Raw / encoded",
        raw_over_encoded(report.indexed_raw_bytes, report.indexed_encoded_bytes),
    );

    ui::cli::log!();
    section("Snapshots");
    row(
        "Snapshots",
        count_and_size(
            report.snapshots_count,
            "snapshot",
            "snapshots",
            report.snapshots_bytes,
        ),
    );
    if report.snapshots_ecc_count > 0 {
        row(
            "ECC sidecars",
            count_and_size(
                report.snapshots_ecc_count,
                "file",
                "files",
                report.snapshots_ecc_bytes,
            ),
        );
    }
    row(
        "Referenced blobs",
        format!(
            "{} (data: {}, tree: {})",
            report.referenced_blobs, report.referenced_data_blobs, report.referenced_tree_blobs
        ),
    );
    row(
        "Raw / encoded",
        raw_over_encoded(report.referenced_raw_bytes, report.referenced_encoded_bytes),
    );
    row(
        "Data (raw / encoded)",
        raw_over_encoded(
            report.referenced_raw_bytes_data,
            report.referenced_encoded_bytes_data,
        ),
    );
    row(
        "Tree (raw / encoded)",
        raw_over_encoded(
            report.referenced_raw_bytes_tree,
            report.referenced_encoded_bytes_tree,
        ),
    );
    row(
        "Compression ratio",
        format!(
            "{:.2}x (data: {:.2}x, tree: {:.2}x)",
            report.ratio_total, report.ratio_data, report.ratio_tree
        ),
    );
    row(
        "Restorable size",
        utils::format_size_binary(report.total_restorable_bytes, 3),
    );
    if report.unreferenced_blobs > 0 {
        row(
            "Unreferenced blobs",
            format!(
                "{} ({} reclaimable)",
                utils::format_count(report.unreferenced_blobs, "blob", "blobs"),
                utils::format_size_binary(report.unreferenced_encoded_bytes, 3)
            ),
        );
    }

    ui::cli::log!();
    section("Keys");
    row(
        "Key files",
        count_and_size(report.keys_count, "key", "keys", report.keys_bytes),
    );

    ui::cli::log!();
    section("Repository");
    row(
        "Manifest",
        utils::format_size_binary(report.manifest_bytes, 3),
    );
    row(
        "Total size",
        utils::format_size_binary(report.total_repo_bytes, 3)
            .bold()
            .to_string(),
    );
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
    shutdown_signal: Arc<AtomicBool>,
    on_progress: ProgressCallback<'_>,
) -> Result<SnapshotAnalysis, StatsError> {
    let snapshot_ids = repo.list_snapshot_ids().await?;
    let total = snapshot_ids.len();
    if total == 0 {
        return Ok(SnapshotAnalysis::default());
    }

    // Shared so a blob referenced by several snapshots is only counted once.
    let visited = Arc::new(ShardedIdSet::new());
    let done = AtomicUsize::new(0);
    on_progress(format!("analyzing snapshots 0/{total}"));

    let partials: Vec<SnapshotAnalysis> = futures::stream::iter(snapshot_ids)
        .map(|id| {
            let repo = repo.clone();
            let visited = visited.clone();
            let shutdown_signal = shutdown_signal.clone();
            let done = &done;
            async move {
                check_interrupted(&shutdown_signal)?;
                let snapshot = repo.load_snapshot(&id, None).await?;
                let analysis =
                    analyze_snapshot(repo, snapshot, visited.as_ref(), &shutdown_signal).await?;
                let n = done.fetch_add(1, Ordering::Relaxed) + 1;
                on_progress(format!("analyzing snapshots {n}/{total}"));
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
    shutdown_signal: &AtomicBool,
) -> Result<SnapshotAnalysis, StatsError> {
    check_interrupted(shutdown_signal)?;

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
        check_interrupted(shutdown_signal)?;
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
