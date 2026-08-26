use std::{
    fmt, io,
    path::{Path, PathBuf},
    sync::{Arc, atomic::AtomicBool},
    time::Instant,
};

use clap::{ArgGroup, Args};
use futures::{StreamExt, stream};
use indicatif::{ProgressBar, ProgressState};
use parking_lot::Mutex;

use crate::{
    archiver::{
        PipelineStatus, SnapshotOptions,
        progress::{SnapshotProcessSummary, SnapshotProgress},
        run_pipeline, spawn_scanner_task,
    },
    backend::{self, StorageHint, WriteContents},
    bundle::{
        format::{BundleIndex, BundleIndexEntry},
        reader::{BundleReader, extract_nodes_parallel, scan_bundle_tree},
        writer::BundleWriter,
    },
    commands::{
        Compression, GlobalArgs, ToExitCode, UseSnapshot, cleanup::CleanupHandler,
        find_use_snapshot, parse_positive_usize, with_repository_lock,
    },
    common::{
        BlobType, ContentIdType, ID, SaveID,
        defaults::{DEFAULT_SNAPSHOT_READERS, SHORT_SNAPSHOT_ID_LEN},
        error::MapacheError,
        traits::{BlobLoader, BlobSaver},
    },
    fs::{
        calculate_lcp, get_absolute_normalized_path,
        tree::{FSNodeStream, NodeDiff, Tree},
    },
    repository::{self, lock::LockHandle, manifest::EccConfig, repo::Repository},
    ui::{
        cli::{self, color::Colorize},
        default_bar_draw_target, default_progress_style,
        events::{BackupEvent, Event},
    },
    utils::{self, collections::IdSet, format_size_binary, rate_estimator::RateEstimator},
};

#[cfg(all(feature = "mount", unix))]
use crate::{
    fs::path_exists,
    mount::fuse::fs::{MapacheFS, MountOptions},
    utils::size,
};

#[derive(Debug, thiserror::Error)]
pub enum BundleError {
    #[error("bundle failed: {0}")]
    BundleFailed(String),
    #[error(transparent)]
    Io(#[from] io::Error),
    #[error(transparent)]
    Repo(#[from] MapacheError),
    #[error("config error: {0}")]
    Config(String),
}

impl ToExitCode for BundleError {
    fn to_exit_code(&self) -> i32 {
        match self {
            BundleError::BundleFailed(_) => 20,
            BundleError::Io(_) => 1,
            BundleError::Repo(_) => 1,
            BundleError::Config(_) => 2,
        }
    }
}

/// TODO(v1-removal): Remove this function when v1 support is dropped.
fn warn_v1_bundle() {
    cli::warning!(
        "Bundle format v1 is deprecated and will be unsupported in a future release.\n\
        Consider using v2 (default) which includes ECC protection: `mapache bundle --format 2`\n"
    );
}

/// Creates a progress bar with rate estimation for byte-transfer operations
/// (bundle export/import). Returns `(progress_bar, rate_estimator)`.
fn make_transfer_progress_bar(
    total_bytes: u64,
    json: bool,
) -> (Option<ProgressBar>, Arc<Mutex<RateEstimator>>) {
    let rate = Arc::new(Mutex::new(RateEstimator::new(
        crate::common::defaults::UI_RATE_ESTIMATOR_WINDOW,
    )));
    let bar = if !json {
        Some(
            ProgressBar::with_draw_target(Some(total_bytes), default_bar_draw_target()).with_style(
                default_progress_style()
                    .template("[{percent}%] [{bar:20.cyan/white}] [{custom_elapsed}] {bytes_fmt} / {total_fmt} [{data_rate}/s] [ETA: {custom_eta}]")
                    .expect("invalid progress bar template for transfer progress")
                    .with_key("bytes_fmt", |state: &ProgressState, w: &mut dyn fmt::Write| {
                        let _ = w.write_str(&format_size_binary(state.pos(), 3));
                    })
                    .with_key("total_fmt", |state: &ProgressState, w: &mut dyn fmt::Write| {
                        let _ = w.write_str(&format_size_binary(state.len().unwrap_or(0), 3));
                    })
                    .with_key("custom_elapsed", |state: &ProgressState, w: &mut dyn fmt::Write| {
                        let _ = w.write_str(&utils::pretty_print_duration(state.elapsed()));
                    })
                    .with_key("custom_eta", {
                        let re = rate.clone();
                        move |state: &ProgressState, w: &mut dyn fmt::Write| {
                            let pos = state.pos() as f64;
                            let total = state.len().map(|l| l as f64);
                            match re.lock().eta(pos, total.unwrap_or(pos)) {
                                Some(d) => { let _ = w.write_str(&utils::pretty_print_duration(d)); }
                                None => { let _ = w.write_str("--"); }
                            }
                        }
                    })
                    .with_key("data_rate", {
                        let re = rate.clone();
                        move |_state: &ProgressState, w: &mut dyn fmt::Write| {
                            let rate = re.lock().rate().floor() as u64;
                            let _ = w.write_str(&format_size_binary(rate, 1));
                        }
                    }),
            ),
        )
    } else {
        None
    };
    (bar, rate)
}

#[derive(Args, Debug, Clone)]
#[clap(
    about = "Create, extract, mount or transfer .mapache bundle files",
    group = ArgGroup::new("mode").required(true).args(&["bundle", "extract", "export_snapshot", "import"]),
)]
pub struct CmdArgs {
    /// Bundle mode: create a new bundle from source paths
    #[arg(short, long, group = "mode")]
    pub bundle: bool,

    /// Extract mode: extract a bundle to a destination
    #[arg(short = 'x', long, group = "mode")]
    pub extract: bool,

    /// Export mode: export a repository snapshot to a bundle file (requires -r)
    #[arg(long = "export-snapshot", group = "mode")]
    pub export_snapshot: Option<UseSnapshot>,

    /// Import mode: import a bundle file as a snapshot into the repository (requires -r)
    #[arg(short = 'i', long, group = "mode")]
    pub import: bool,

    /// Mount mode: mount a bundle as a filesystem (FUSE)
    #[cfg(all(feature = "mount", unix))]
    #[arg(short, long, group = "mode")]
    pub mount: bool,

    /// Input: source paths (-a), bundle file (-x), or bundle + mountpoint (-m)
    pub input: Vec<PathBuf>,

    /// Output: bundle file (-a) or destination directory (-x). Not used with -m.
    #[arg(short, long)]
    pub output: Option<PathBuf>,

    /// Glob patterns for paths to exclude (bundle mode only)
    #[arg(short = 'e', long)]
    pub exclude: Vec<PathBuf>,

    /// Use a single directory path as the bundle root (bundle mode only)
    #[clap(long = "as-root", value_parser, action = clap::ArgAction::Set, num_args = 0..=1, default_missing_value = "true")]
    pub as_root: Option<bool>,

    /// Number of parallel readers. Must be greater than 0.
    #[clap(long, default_value_t = DEFAULT_SNAPSHOT_READERS, value_parser = parse_readers)]
    pub readers: usize,

    /// Bundle format version (1 or 2). v2 includes ECC protection.
    // TODO(v1-removal): Remove --format flag, always use v2.
    #[clap(long, default_value_t = repository::repo::THIS_REPOSITORY_VERSION)]
    pub format: u32,

    /// Enable Reed-Solomon ECC with the given overhead percentage (0–100).
    ///
    /// A value of 0 disables ECC. When set, the bundle data section is
    /// protected by erasure codes. Fixed K=100, P=overhead.
    #[clap(long, value_parser = clap::value_parser!(u32).range(0..=100))]
    pub ecc: Option<u32>,

    /// Create mountpoint if it does not exist (mount mode only, passes to mount -c)
    #[cfg(all(feature = "mount", unix))]
    #[arg(short, long, default_value_t = false)]
    pub create: bool,

    /// Allow other users to access the mount (mount mode only)
    #[cfg(all(feature = "mount", unix))]
    #[arg(long, default_value_t = false)]
    pub allow_other: bool,

    /// Display files but do not load contents (mount mode only)
    #[cfg(all(feature = "mount", unix))]
    #[arg(long, default_value_t = false)]
    pub metadata_only: bool,

    /// Max size of internal data cache in MiB (mount mode only)
    #[cfg(all(feature = "mount", unix))]
    #[arg(long = "cache-size-mib", default_value_t = 256.0)]
    pub data_cache_size_mib: f32,

    #[arg(skip)]
    pub internal_password: Option<String>,
}

#[cfg(all(feature = "mount", unix))]
impl Default for CmdArgs {
    fn default() -> Self {
        Self {
            bundle: false,
            extract: false,
            export_snapshot: None,
            import: false,
            mount: false,
            input: vec![],
            output: None,
            exclude: vec![],
            as_root: None,
            readers: DEFAULT_SNAPSHOT_READERS,
            create: false,
            allow_other: false,
            metadata_only: false,
            data_cache_size_mib: 256.0,
            format: crate::repository::repo::THIS_REPOSITORY_VERSION,
            ecc: None,
            internal_password: None,
        }
    }
}

#[cfg(not(all(feature = "mount", unix)))]
impl Default for CmdArgs {
    fn default() -> Self {
        Self {
            bundle: false,
            extract: false,
            export_snapshot: None,
            import: false,
            input: vec![],
            output: None,
            exclude: vec![],
            as_root: None,
            readers: DEFAULT_SNAPSHOT_READERS,
            format: crate::repository::repo::THIS_REPOSITORY_VERSION,
            ecc: None,
            internal_password: None,
        }
    }
}

pub async fn run(global: &crate::commands::GlobalArgs, args: &CmdArgs) -> Result<(), BundleError> {
    // TODO(v1-removal): Remove format validation and the v1 branch.
    if args.format < 1 || args.format > 2 {
        return Err(BundleError::Config(format!(
            "unsupported bundle format: {} (supported: 1, 2)",
            args.format
        )));
    }

    if args.format < 2 && args.ecc.is_some() {
        return Err(BundleError::Config(
            "ECC is not supported in bundle format v1; use format 2".to_string(),
        ));
    }

    if args.as_root.is_some() && !args.bundle {
        return Err(BundleError::Config(
            "--as-root can only be used with bundle mode".to_string(),
        ));
    }

    if args.export_snapshot.is_some() {
        run_export_snapshot(global, args).await
    } else if args.import {
        run_import(global, args).await
    } else if args.bundle {
        if args.input.is_empty() {
            return Err(BundleError::Config(
                "bundle mode requires at least one input path".to_string(),
            ));
        }
        run_create(global, args).await
    } else if args.extract {
        if args.input.is_empty() {
            return Err(BundleError::Config(
                "extract mode requires a bundle file as input".to_string(),
            ));
        }
        run_extract(args).await
    } else {
        if args.input.is_empty() {
            return Err(BundleError::Config(
                "mount mode requires a bundle file and mountpoint".to_string(),
            ));
        }
        run_mount(args).await
    }
}

async fn run_create(global: &GlobalArgs, args: &CmdArgs) -> Result<(), BundleError> {
    tracing::info!(target: "bundle", "Starting bundle create command");
    let output = args
        .output
        .as_ref()
        .ok_or_else(|| BundleError::Config("-o is required for bundle mode".to_string()))?;

    let password = match &args.internal_password {
        Some(p) => zeroize::Zeroizing::new(p.clone()),
        None => cli::request_new_password("Enter bundle password", "Confirm password")
            .map_err(|e| BundleError::BundleFailed(e.to_string()))?,
    };

    // TODO(v1-removal): Remove v1 branch, always use ECC.
    if args.format < 2 {
        warn_v1_bundle();
    }

    let ecc_config = args.ecc.and_then(EccConfig::from_overhead);

    let bundle_writer = Arc::new(
        BundleWriter::new(
            output,
            &password,
            global.compression_level.to_level(),
            !matches!(global.compression_level, Compression::None),
            args.format as u16,
            ecc_config,
        )
        .map_err(|e| BundleError::BundleFailed(e.to_string()))?,
    );
    let shutdown_signal = Arc::new(AtomicBool::new(false));
    let progress = Arc::new(SnapshotProgress::new());

    // Normalize source paths to absolute, canonical form.
    // Uses get_absolute_normalized_path (lexical, no filesystem access) rather than
    // canonicalize() to avoid Windows \\?\ verbatim prefixes, keeping paths in a
    // consistent format for PathFilter trie matching with exclude paths.
    let mut absolute_source_paths = Vec::new();
    for p in &args.input {
        match get_absolute_normalized_path(p) {
            Ok(abs) => absolute_source_paths.push(abs),
            Err(_) => absolute_source_paths.push(p.clone()),
        }
    }

    // Normalize exclude paths: resolve relative/msys-style paths to absolute,
    // but leave glob patterns as-is.
    let exclude_paths: Vec<PathBuf> = args
        .exclude
        .iter()
        .map(|p| {
            let s = p.to_string_lossy();
            if s.contains('*') || s.contains('?') {
                p.clone()
            } else {
                get_absolute_normalized_path(p).unwrap_or_else(|_| p.clone())
            }
        })
        .collect();

    // If --as-root is set, expand a single directory into its children
    if args.as_root.unwrap_or(false) {
        if absolute_source_paths.len() != 1 {
            return Err(BundleError::Config(
                "bundle mode --as-root requires exactly one input path".to_string(),
            ));
        }
        let root = &absolute_source_paths[0];
        if !root.is_dir() {
            return Err(BundleError::Config(
                "bundle mode --as-root input must be a directory".to_string(),
            ));
        }
        let mut dir = tokio::fs::read_dir(root).await?;
        let mut children = Vec::new();
        while let Some(entry) = dir.next_entry().await? {
            children.push(entry.path());
        }
        absolute_source_paths = children;
    }

    let snapshot_root_path = if absolute_source_paths.len() == 1 {
        let p = &absolute_source_paths[0];
        p.parent().unwrap_or(p).to_path_buf()
    } else {
        calculate_lcp(&absolute_source_paths, false)
    };

    cli::log!(
        "{} Creating bundle from {} to {}...",
        "[1/1]".bold().cyan(),
        args.input
            .iter()
            .map(|p| p.display().to_string())
            .collect::<Vec<_>>()
            .join(", ")
            .bold(),
        output.display().to_string().bold()
    );

    let event_sender =
        cli::bundle::make_event_sender(cli::bundle::BundleMode::Create, 0, 0, args.readers);

    let snapshot_options = SnapshotOptions {
        absolute_source_paths,
        snapshot_root_path: snapshot_root_path.clone(),
        exclude_paths: exclude_paths.clone(),
        parent_snapshot: None,
        tags: Default::default(),
        description: Some(format!(
            "Bundle of {}",
            args.input
                .iter()
                .map(|p| p.display().to_string())
                .collect::<Vec<_>>()
                .join(", ")
        )),
        no_scan: false,
        with_atime: false,
        stdin: false,
    };

    let fs_stream = FSNodeStream::from_paths(
        snapshot_options.absolute_source_paths.clone(),
        snapshot_options.exclude_paths.clone(),
        false,
    )
    .await?;

    let process_readers = args.readers;

    // Pipeline status for coordinating tasks and error propagation.
    let status = Arc::new(PipelineStatus::new(
        progress.clone(),
        event_sender.clone(),
        shutdown_signal.clone(),
    ));

    // Background Scanner (progress estimation, runs concurrently with processing)
    let scanner_handle = spawn_scanner_task(
        false,
        snapshot_options.absolute_source_paths.clone(),
        exclude_paths.clone(),
        status.clone(),
        event_sender.clone(),
    );

    // Map FSNodeStream output into the DiffTuple format that run_pipeline expects.
    // Bundle always creates everything new: no previous tree, no diff comparison.
    let (diff_tx, diff_rx) = tokio::sync::mpsc::channel(4096);
    let coordinator_status = status.clone();
    let producer_task = tokio::spawn(async move {
        use futures::StreamExt;
        let mut stream = fs_stream;
        while let Some(item) = stream.next().await {
            if coordinator_status.is_failed() {
                break;
            }
            match item {
                Ok((path, Ok(stream_node))) => {
                    let diff_tuple = (path, None, Some(Ok(stream_node)), NodeDiff::New);
                    if diff_tx.send(diff_tuple).await.is_err() {
                        break;
                    }
                }
                Ok((path, Err(e))) => {
                    tracing::warn!(target: "bundle", "Skipping {:?}: {}", path, e);
                }
                Err(e) => {
                    tracing::error!(target: "bundle", "Stream error: {e}");
                    coordinator_status.signal_fatal(MapacheError::Internal(e.to_string()));
                    break;
                }
            }
        }
    });

    // Run the shared pipeline (coordinator + chunker pool + forwarder + tree serializer)
    let pipeline_result = run_pipeline(
        diff_rx,
        bundle_writer.clone() as Arc<dyn BlobSaver>,
        process_readers,
        snapshot_root_path.clone(),
        &snapshot_options.absolute_source_paths,
        false,
        status.clone(),
    )
    .await;

    // Wait for producer and scanner
    if let Err(e) = producer_task.await {
        tracing::error!(target: "bundle", "Producer task panicked: {e}");
    }
    if let Err(e) = scanner_handle.await {
        tracing::warn!(target: "bundle", "Scanner task panicked: {e}");
    }

    let result = pipeline_result?;

    writer_finalize(
        bundle_writer.as_ref(),
        result.root_tree_id,
        output,
        &progress,
    )
    .await?;

    let summary = progress.summary();
    event_sender(Event::Backup(BackupEvent::Finished(summary)));

    Ok(())
}

async fn run_extract(args: &CmdArgs) -> Result<(), BundleError> {
    tracing::info!(target: "bundle", "Starting bundle extract command (bundle={:?}, target={:?})", args.input[0], args.output);
    if args.input.len() != 1 {
        return Err(BundleError::BundleFailed(
            "extract mode requires exactly one bundle file as input".to_string(),
        ));
    }
    let bundle = &args.input[0];
    let destination = args.output.as_deref().unwrap_or(std::path::Path::new("."));

    let password = match &args.internal_password {
        Some(p) => zeroize::Zeroizing::new(p.clone()),
        None => cli::request_password("Enter bundle password")
            .map_err(|e| BundleError::BundleFailed(e.to_string()))?,
    };

    let reader = BundleReader::open(bundle, &password)
        .map_err(|e| BundleError::BundleFailed(e.to_string()))?;
    // TODO(v1-removal): Remove v1 warning when v1 support is dropped.
    if reader.version < 2 {
        warn_v1_bundle();
    }
    let root_tree_id = reader.trailer.root_tree;
    let loader = Arc::new(reader);

    cli::log!("{} Analyzing bundle...", "[1/2]".bold().cyan());
    let (total_items, total_bytes) = scan_bundle_tree(loader.clone(), &root_tree_id).await?;

    cli::log!(
        "{} Extracting {} to {}...",
        "[2/2]".bold().cyan(),
        bundle.display().to_string().bold(),
        destination.display().to_string().bold()
    );

    if !destination.exists() {
        std::fs::create_dir_all(destination)?;
    }

    let event_sender = cli::bundle::make_event_sender(
        cli::bundle::BundleMode::Extract,
        total_items as u64,
        total_bytes,
        args.readers,
    );

    extract_nodes_parallel(
        loader.clone(),
        &root_tree_id,
        destination,
        args.readers,
        event_sender.clone(),
    )
    .await?;

    event_sender(Event::Backup(BackupEvent::Finished(
        SnapshotProcessSummary {
            processed_items_count: total_items as u64,
            processed_bytes: total_bytes,
            diff_counts: crate::repository::snapshot::DiffCounts::default(),
        },
    )));

    cli::log!();
    cli::log!("{}", "Extraction Summary:".bold().cyan());

    let mut data_table = cli::table::Table::new();
    data_table.add_row(vec![
        "Extracted items".to_string(),
        total_items.to_string().bold().white().to_string(),
    ]);
    data_table.add_row(vec![
        "Total size".to_string(),
        format_size_binary(total_bytes, 3)
            .bold()
            .white()
            .to_string(),
    ]);

    cli::log!("{}", data_table.render());
    cli::log!("Extraction completed successfully");
    tracing::info!(target: "bundle", "Bundle extraction completed");

    Ok(())
}

async fn export_snapshot_impl(
    repo: Arc<Repository>,
    lock_handle: Option<LockHandle>,
    use_snapshot: &UseSnapshot,
    output: &Path,
    password: &zeroize::Zeroizing<String>,
    global: &GlobalArgs,
    args: &CmdArgs,
) -> Result<(), BundleError> {
    let cleanup_handler = CleanupHandler::new();
    cleanup_handler.add_lock(lock_handle);

    // Block exporting v2 repos to v1 bundles and vice versa.
    let repo_version = repo.repo_version();
    if args.format != repo_version {
        return Err(BundleError::Config(format!(
            "cannot export v{} repository to a v{} bundle",
            repo_version, args.format
        )));
    }

    repo.reload_master_index().await?;

    // Resolve snapshot (supports "latest", prefix, or full ID)
    let (snap_id, snapshot) = find_use_snapshot(repo.clone(), use_snapshot)
        .await?
        .ok_or_else(|| {
            MapacheError::Repo(format!("no snapshot found matching '{}'", use_snapshot))
        })?;

    if !global.json {
        cli::log!(
            "{} Exporting snapshot {} ({})...",
            "[1/3]".bold().cyan(),
            snap_id.to_short_hex(SHORT_SNAPSHOT_ID_LEN).bold(),
            snapshot
                .timestamp
                .format("%Y-%m-%d %H:%M:%S %:z")
                .to_string()
                .bold()
        );
    }

    // Create bundle writer
    // TODO(v1-removal): Remove v1 branch, always use ECC.
    if args.format < 2 {
        warn_v1_bundle();
    }
    let ecc_config = args.ecc.and_then(EccConfig::from_overhead);
    let bundle_writer = Arc::new(
        BundleWriter::new(
            output,
            password,
            global.compression_level.to_level(),
            !matches!(global.compression_level, Compression::None),
            args.format as u16,
            ecc_config,
        )
        .map_err(|e| MapacheError::Repo(e.to_string()))?,
    );

    // Collect all blob IDs from the snapshot tree
    if !global.json {
        cli::log!("{} Walking snapshot tree...", "[2/3]".bold().cyan());
    }

    let mut blob_list: Vec<(ID, BlobType)> = Vec::new();
    let mut tree_stack: Vec<ID> = Vec::new();
    let mut visited_trees: IdSet<ID> = IdSet::default();
    let mut visited_blobs: IdSet<ID> = IdSet::default();
    let mut total_bytes: u64 = 0;
    let index = repo.index();

    tree_stack.push(snapshot.tree);

    while let Some(tree_id) = tree_stack.pop() {
        if !visited_trees.insert(tree_id) {
            continue;
        }
        blob_list.push((tree_id, BlobType::Tree));
        if let Some(loc) = index.get(&tree_id).await {
            total_bytes += loc.raw_length as u64;
        }

        match Tree::load_from_repo(repo.as_ref(), &tree_id).await {
            Ok(tree) => {
                for node in tree.nodes {
                    if let Some(subtree_id) = node.tree {
                        tree_stack.push(subtree_id);
                    }
                    if let Some(blob_ids) = node.blobs {
                        for blob_id in blob_ids {
                            if !visited_blobs.insert(blob_id) {
                                continue;
                            }
                            blob_list.push((blob_id, BlobType::Data));
                            if let Some(loc) = index.get(&blob_id).await {
                                total_bytes += loc.raw_length as u64;
                            }
                        }
                    }
                }
            }
            Err(e) => {
                return Err(
                    MapacheError::Repo(format!("failed to load tree {}: {}", tree_id, e)).into(),
                );
            }
        }
    }

    // Write blobs to bundle concurrently
    let start = Instant::now();
    let total = blob_list.len();

    let (bar, export_rate) = make_transfer_progress_bar(total_bytes, global.json);

    let results: Vec<Result<(), MapacheError>> = stream::iter(blob_list)
        .map(|(blob_id, blob_type)| {
            let repo = repo.clone();
            let bundle_writer = bundle_writer.clone();
            let bar = bar.clone();
            let export_rate = export_rate.clone();
            async move {
                let data = repo.load_blob(&blob_id).await?;
                let blob_size = data.len() as u64;

                tokio::task::spawn_blocking(move || {
                    bundle_writer.save_blob(
                        blob_type,
                        WriteContents::Owned(data),
                        SaveID::WithID(blob_id),
                    )
                })
                .await
                .map_err(|e| MapacheError::Repo(format!("export task panicked: {}", e)))??;

                if let Some(ref bar) = bar {
                    bar.inc(blob_size);
                    export_rate.lock().observe(bar.position() as f64);
                }

                Ok(())
            }
        })
        .buffer_unordered(args.readers)
        .collect::<Vec<Result<(), MapacheError>>>()
        .await;

    for r in results {
        r?;
    }

    if let Some(ref bar) = bar {
        bar.finish_and_clear();
    }

    // Finalize bundle
    bundle_writer
        .finalize(snapshot.tree)
        .map_err(|e| MapacheError::Repo(e.to_string()))?;

    let final_size = std::fs::metadata(output)
        .map_err(|e| MapacheError::Repo(format!("failed to stat bundle file: {}", e)))?
        .len();

    let elapsed = start.elapsed();

    if !global.json {
        cli::log!();
        cli::log!("{}", "Export Summary:".bold().cyan());

        let mut data_table = cli::table::Table::new();
        data_table.add_row(vec![
            "Snapshot".to_string(),
            snap_id
                .to_short_hex(SHORT_SNAPSHOT_ID_LEN)
                .bold()
                .white()
                .to_string(),
        ]);
        data_table.add_row(vec![
            "Exported blobs".to_string(),
            total.to_string().bold().white().to_string(),
        ]);
        data_table.add_row(vec![
            "Original size".to_string(),
            format_size_binary(total_bytes, 3)
                .bold()
                .white()
                .to_string(),
        ]);
        data_table.add_row(vec![
            "Bundle size".to_string(),
            format_size_binary(final_size, 3).bold().green().to_string(),
        ]);

        let ratio = if total_bytes > 0 {
            (final_size as f64 / total_bytes as f64) * 100.0
        } else {
            0.0
        };

        data_table.add_row(vec![
            "Compression ratio".to_string(),
            format!("{:.1}%", ratio).bold().yellow().to_string(),
        ]);
        data_table.add_row(vec![
            "Duration".to_string(),
            utils::pretty_print_duration(elapsed)
                .bold()
                .white()
                .to_string(),
        ]);

        cli::log!("{}", data_table.render());
        cli::log!("Snapshot exported successfully");
    }

    tracing::info!(target: "bundle", "Bundle export completed (size={}, blobs={})", final_size, total);
    Ok(())
}

async fn run_export_snapshot(global: &GlobalArgs, args: &CmdArgs) -> Result<(), BundleError> {
    let use_snapshot = args
        .export_snapshot
        .as_ref()
        .expect("export_snapshot must be Some");

    let output = args
        .output
        .as_ref()
        .ok_or_else(|| BundleError::Config("-o is required for export mode".to_string()))?;

    if global.repo.is_empty() {
        return Err(BundleError::Config(
            "Repository path is required for export mode. Use -r, set MAPACHE_REPOSITORY, or add it to config file.".to_string(),
        ));
    }

    tracing::info!(target: "bundle", "Starting bundle export: snapshot {} to {}", use_snapshot, output.display());

    let password = match &args.internal_password {
        Some(p) => zeroize::Zeroizing::new(p.clone()),
        None => cli::request_new_password("Enter bundle password", "Confirm password")
            .map_err(|e| BundleError::BundleFailed(e.to_string()))?,
    };

    with_repository_lock(
        global.auth_file.as_ref(),
        global.key.as_ref(),
        backend::new_backend_with_prompt(global.backend_options(false))
            .await
            .map_err(|e| {
                BundleError::BundleFailed(format!(
                    "failed to initialize repository backend: {}",
                    e.inner()
                ))
            })?,
        global.to_repo_config(),
        false, // export is read-only on the repository
        global.retry_lock_duration,
        global.no_lock,
        |repo, _secure_storage, lock_handle| {
            let password = password.clone();
            let use_snapshot = use_snapshot.clone();
            let output = output.clone();
            async move {
                export_snapshot_impl(
                    repo,
                    lock_handle,
                    &use_snapshot,
                    &output,
                    &password,
                    global,
                    args,
                )
                .await
            }
        },
    )
    .await?;

    Ok(())
}

async fn run_import_bundle(
    repo: &Arc<Repository>,
    bundle_path: &Path,
    password: &zeroize::Zeroizing<String>,
    global: &GlobalArgs,
    args: &CmdArgs,
) -> Result<(), BundleError> {
    tracing::info!(target: "bundle", "Importing bundle: {}", bundle_path.display());

    let reader = BundleReader::open(bundle_path, password).map_err(|e| {
        BundleError::BundleFailed(format!(
            "failed to open bundle {}: {}",
            bundle_path.display(),
            e
        ))
    })?;

    // TODO(v1-removal): Remove v1 warning when v1 support is dropped.
    if reader.version < 2 {
        warn_v1_bundle();
    }

    // Block importing v1 bundles into v2 repos and vice versa.
    let repo_version = repo.repo_version();
    if reader.version != repo_version as u16 {
        return Err(BundleError::Config(format!(
            "bundle format v{} cannot be imported into a v{} repository",
            reader.version, repo_version
        )));
    }

    let root_tree_id = reader.trailer.root_tree;
    let bundle_index = reader.index().clone();

    let total_blobs = bundle_index.entries.len();
    if !global.json {
        cli::log!(
            "{} {} analyzing ({} blobs)...",
            "[1/3]".bold().cyan(),
            bundle_path
                .file_name()
                .unwrap_or_default()
                .to_string_lossy()
                .bold(),
            total_blobs
        );
    }

    // Filter blobs: only import those not already in the repo
    let dest_index = repo.index();
    let to_import: Vec<BundleIndexEntry> = bundle_index
        .entries
        .iter()
        .filter(|entry| !dest_index.contains(&entry.id))
        .cloned()
        .collect();

    let skipped = total_blobs - to_import.len();
    let import_bytes: u64 = to_import.iter().map(|e| e.raw_length as u64).sum();

    if to_import.is_empty() {
        if !global.json {
            cli::log!(
                "{} All blobs already present — creating snapshot...",
                "[2/3]".bold().cyan()
            );
        }
    } else if !global.json {
        cli::log!(
            "{} {} blobs ({}), {} already present...",
            "[2/3]".bold().cyan(),
            utils::format_count(to_import.len(), "blob", "blobs"),
            format_size_binary(import_bytes, 3).bold(),
            format!("{} skipped", skipped)
        );
    }

    if to_import.is_empty() {
        // Still create a snapshot pointing to the bundle's root tree
        let _ =
            create_import_snapshot(repo, bundle_path, &bundle_index, root_tree_id, global).await?;
        return Ok(());
    }

    // Import blobs concurrently — load from bundle, encode + save to repo
    let loader: Arc<dyn BlobLoader> = Arc::new(reader);

    let (bar, import_rate) = make_transfer_progress_bar(import_bytes, global.json);

    let import_start = Instant::now();
    let imported_count = to_import.len();

    let results: Vec<Result<(), BundleError>> = stream::iter(to_import)
        .map(|entry| {
            let loader = loader.clone();
            let repo_clone = repo.clone();
            let bar = bar.clone();
            let import_rate = import_rate.clone();
            async move {
                let data = loader.load_blob(&entry.id).await.map_err(|e| {
                    BundleError::BundleFailed(format!("failed to load blob {}: {}", entry.id, e))
                })?;

                let blob_size = data.len() as u64;

                tokio::task::spawn_blocking(move || {
                    repo_clone.encode_and_save_blob(
                        entry.blob_type,
                        WriteContents::Owned(data),
                        SaveID::WithID(entry.id),
                    )
                })
                .await
                .map_err(|e| BundleError::BundleFailed(format!("encoding task panicked: {}", e)))?
                .map_err(|e| BundleError::BundleFailed(format!("failed to save blob: {}", e)))?;

                if let Some(ref bar) = bar {
                    bar.inc(blob_size);
                    import_rate.lock().observe(bar.position() as f64);
                }

                Ok(())
            }
        })
        .buffer_unordered(args.readers)
        .collect::<Vec<Result<(), BundleError>>>()
        .await;

    for r in results {
        r?;
    }

    if let Some(ref bar) = bar {
        bar.finish_and_clear();
    }

    // Create snapshot for this bundle
    let snapshot_id =
        create_import_snapshot(repo, bundle_path, &bundle_index, root_tree_id, global).await?;

    if !global.json {
        let elapsed = import_start.elapsed();
        cli::log!();
        cli::log!("{}", "Import Summary:".bold().cyan());

        let mut data_table = cli::table::Table::new();
        data_table.add_row(vec![
            "New snapshot".to_string(),
            snapshot_id
                .to_short_hex(SHORT_SNAPSHOT_ID_LEN)
                .bold()
                .green()
                .to_string(),
        ]);
        data_table.add_row(vec![
            "Blobs imported".to_string(),
            imported_count.to_string().bold().white().to_string(),
        ]);
        data_table.add_row(vec![
            "Blobs skipped (existing)".to_string(),
            skipped.to_string().bold().white().to_string(),
        ]);
        data_table.add_row(vec![
            "Imported size".to_string(),
            format_size_binary(import_bytes, 3)
                .bold()
                .white()
                .to_string(),
        ]);
        data_table.add_row(vec![
            "Duration".to_string(),
            utils::pretty_print_duration(elapsed)
                .bold()
                .white()
                .to_string(),
        ]);

        cli::log!("{}", data_table.render());
    }

    tracing::info!(
        target: "bundle",
        "Bundle import completed (bundle={}, snapshot={}, blobs={})",
        bundle_path.display(),
        snapshot_id,
        imported_count
    );

    Ok(())
}

async fn create_import_snapshot(
    repo: &Arc<Repository>,
    bundle_path: &Path,
    bundle_index: &BundleIndex,
    root_tree_id: ID,
    global: &GlobalArgs,
) -> Result<ID, BundleError> {
    use crate::repository::snapshot::{Snapshot, SnapshotSummary};
    use chrono::Local;

    let abs_bundle_path = crate::fs::get_absolute_normalized_path(bundle_path)
        .unwrap_or_else(|_| bundle_path.to_path_buf());

    let total_blobs = bundle_index.entries.len();
    let data_blobs: u64 = bundle_index
        .entries
        .iter()
        .filter(|e| e.blob_type == BlobType::Data)
        .count() as u64;
    let meta_blobs: u64 = total_blobs as u64 - data_blobs;
    let raw_bytes: u64 = bundle_index
        .entries
        .iter()
        .map(|e| e.raw_length as u64)
        .sum();
    let meta_raw_bytes: u64 = bundle_index
        .entries
        .iter()
        .filter(|e| e.blob_type != BlobType::Data)
        .map(|e| e.raw_length as u64)
        .sum();
    let snapshot = Snapshot {
        timestamp: Local::now(),
        tree: root_tree_id,
        root: PathBuf::from("/"),
        paths: vec![abs_bundle_path],
        description: Some(format!(
            "Imported from bundle {}",
            bundle_path
                .file_name()
                .unwrap_or_default()
                .to_string_lossy()
        )),
        summary: SnapshotSummary {
            processed_items_count: data_blobs,
            processed_bytes: raw_bytes,
            raw_bytes,
            encoded_bytes: raw_bytes,
            meta_raw_bytes,
            meta_encoded_bytes: meta_raw_bytes,
            total_raw_bytes: raw_bytes,
            total_encoded_bytes: raw_bytes,
            data_blobs,
            meta_blobs,
            ..Default::default()
        },
        ..Default::default()
    };

    let snapshot_bytes = serde_json::to_vec(&snapshot)
        .map_err(|e| BundleError::BundleFailed(format!("failed to serialize snapshot: {}", e)))?;

    let (snapshot_id, _size) = repo
        .save_file(
            &SaveID::CalculateID,
            &snapshot_bytes,
            StorageHint {
                file_type: ContentIdType::Snapshot,
                is_metadata: true,
            },
            None,
        )
        .await
        .map_err(|e| BundleError::BundleFailed(format!("failed to save snapshot: {}", e)))?;

    if !global.json {
        cli::log!(
            "{} Created snapshot {}",
            "[3/3]".bold().cyan(),
            snapshot_id
                .to_short_hex(SHORT_SNAPSHOT_ID_LEN)
                .bold()
                .green()
        );
    }

    Ok(snapshot_id)
}

async fn import_impl(
    repo: Arc<Repository>,
    lock_handle: Option<LockHandle>,
    password: &zeroize::Zeroizing<String>,
    global: &GlobalArgs,
    args: &CmdArgs,
) -> Result<(), BundleError> {
    let cleanup_handler = CleanupHandler::new();
    cleanup_handler.add_lock(lock_handle);

    repo.reload_master_index().await?;
    repo.init_pack_saver(args.readers)?;

    for bundle_path in &args.input {
        run_import_bundle(&repo, bundle_path, password, global, args).await?;
    }

    // Flush pack saver
    if !global.json {
        cli::log!(
            "{} Persisting repository index...",
            "[finalize]".bold().cyan()
        );
    }
    repo.flush_and_finalize_pack_saver().await?;

    if !global.json {
        cli::log!();
        if args.input.len() > 1 {
            cli::log!("All {} bundles imported successfully.", args.input.len());
        } else {
            cli::log!("Bundle imported successfully.");
        }
    }

    tracing::info!(target: "bundle", "Bundle import completed for {} bundles", args.input.len());
    Ok(())
}

async fn run_import(global: &GlobalArgs, args: &CmdArgs) -> Result<(), BundleError> {
    if args.input.is_empty() {
        return Err(BundleError::BundleFailed(
            "import mode requires at least one bundle file as input".to_string(),
        ));
    }

    if global.repo.is_empty() {
        return Err(BundleError::Config(
            "Repository path is required for import mode. Use -r, set MAPACHE_REPOSITORY, or add it to config file.".to_string(),
        ));
    }

    tracing::info!(target: "bundle", "Starting bundle import: {} files to repo {}", args.input.len(), global.repo);

    let password = match &args.internal_password {
        Some(p) => zeroize::Zeroizing::new(p.clone()),
        None => cli::request_password("Enter bundle password")
            .map_err(|e| BundleError::BundleFailed(e.to_string()))?,
    };

    with_repository_lock(
        global.auth_file.as_ref(),
        global.key.as_ref(),
        backend::new_backend_with_prompt(global.backend_options(false))
            .await
            .map_err(|e| {
                BundleError::BundleFailed(format!(
                    "failed to initialize repository backend: {}",
                    e.inner()
                ))
            })?,
        global.to_repo_config(),
        false, // non-exclusive lock is used for snapshot/backup
        global.retry_lock_duration,
        global.no_lock,
        |repo, _secure_storage, lock_handle| {
            let password = password.clone();
            async move { import_impl(repo, lock_handle, &password, global, args).await }
        },
    )
    .await?;

    Ok(())
}

#[cfg(all(feature = "mount", unix))]
async fn run_mount(args: &CmdArgs) -> Result<(), BundleError> {
    tracing::info!(target: "bundle", "Starting bundle mount command (bundle={:?})", args.input[0]);
    if args.input.len() != 2 {
        return Err(BundleError::BundleFailed(
            "mount mode requires: bundle.mapache <mountpoint>".to_string(),
        ));
    }
    let bundle = &args.input[0];
    let mountpoint = &args.input[1];

    let actual_mountpoint = mountpoint.clone();
    let mut created_mountpoint = false;

    if !path_exists(&actual_mountpoint).await {
        if args.create {
            std::fs::create_dir_all(&actual_mountpoint)?;
            created_mountpoint = true;
        } else {
            return Err(BundleError::BundleFailed(
                "mountpoint doesn't exist. use -c to create it automatically.".to_string(),
            ));
        }
    } else if !actual_mountpoint.is_dir() {
        return Err(BundleError::BundleFailed(
            "mountpoint must be a directory".to_string(),
        ));
    }

    let canonical_mountpoint = get_absolute_normalized_path(&actual_mountpoint)?;

    let password = match &args.internal_password {
        Some(p) => zeroize::Zeroizing::new(p.clone()),
        None => cli::request_password("Enter bundle password")
            .map_err(|e| BundleError::BundleFailed(e.to_string()))?,
    };

    let reader = BundleReader::open(bundle, &password)
        .map_err(|e| BundleError::BundleFailed(e.to_string()))?;
    // TODO(v1-removal): Remove v1 warning when v1 support is dropped.
    if reader.version < 2 {
        warn_v1_bundle();
    }
    let root_tree_id = reader.trailer.root_tree;
    let loader: Arc<dyn BlobLoader> = Arc::new(reader);

    let cleanup_handler = CleanupHandler::new();
    cli::log!(
        "Mounting bundle {} in {}",
        bundle.display().to_string().bold(),
        canonical_mountpoint.display()
    );

    let data_cache_size = (args.data_cache_size_mib * size::MiB as f32) as u64;
    let allow_other = args.allow_other;
    let metadata_only = args.metadata_only;
    let mp_clone = canonical_mountpoint.clone();

    run_mount_loop(&canonical_mountpoint, cleanup_handler, move |mp| {
        tracing::info!(target: "bundle", "Mounting bundle at {:?}", mp);
        MapacheFS::mount(
            loader,
            None,
            Some(root_tree_id),
            mp,
            MountOptions {
                allow_other,
                metadata_only,
                data_cache_size,
                created_time: chrono::Local::now(),
            },
        )
        .map_err(|e| BundleError::BundleFailed(e.to_string()))
    })
    .await?;

    if created_mountpoint {
        let _ = std::fs::remove_dir_all(&mp_clone);
    }

    Ok(())
}

#[cfg(not(all(feature = "mount", unix)))]
async fn run_mount(_args: &CmdArgs) -> Result<(), BundleError> {
    Err(BundleError::BundleFailed(
        "Mount mode requires FUSE support on Unix systems. Compile with the 'mount' feature."
            .to_string(),
    ))
}

#[cfg(all(feature = "mount", unix))]
pub(crate) async fn run_mount_loop<F, E>(
    mountpoint: &std::path::Path,
    cleanup_handler: CleanupHandler,
    mount_fn: F,
) -> Result<(), E>
where
    F: FnOnce(&std::path::Path) -> Result<(), E> + Send + 'static,
    E: From<MapacheError> + Send + 'static,
{
    cli::log!(
        "Press {} to finish or unmount the filesystem manually.",
        "Ctrl+C".bold()
    );

    let mp_clone = mountpoint.to_path_buf();
    let mount_res = tokio::task::spawn_blocking(move || mount_fn(&mp_clone));

    tokio::select! {
        res = mount_res => {
            res.map_err(|e| E::from(MapacheError::task_panicked("mount", e)))??;
        }
        _ = async {
            loop {
                if cleanup_handler.is_interrupted() {
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            }
        } => {
            cli::log!("Interrupt received. Unmounting...");
            tracing::info!(target: "mount", "Interrupt received. Unmounting {:?}", mountpoint);
            let _ = MapacheFS::<dyn BlobLoader>::unmount(mountpoint);
        }
    }
    tracing::info!(target: "mount", "Mount loop finished");
    Ok(())
}

async fn writer_finalize(
    writer: &BundleWriter,
    root_tree_id: ID,
    output_path: &PathBuf,
    progress: &SnapshotProgress,
) -> Result<(), BundleError> {
    tracing::info!(target: "bundle", "Finalizing bundle with root tree {}", root_tree_id.to_short_hex(8));
    writer
        .finalize(root_tree_id)
        .map_err(|e| BundleError::BundleFailed(e.to_string()))?;

    let final_size = std::fs::metadata(output_path)?.len();
    let summary = progress.summary();

    cli::log!("");
    cli::log!("{}", "Bundle Summary:".bold().cyan());

    let mut data_table = cli::table::Table::new();
    data_table.add_row(vec![
        "Processed items".to_string(),
        summary
            .processed_items_count
            .to_string()
            .bold()
            .white()
            .to_string(),
    ]);
    data_table.add_row(vec![
        "Original size".to_string(),
        format_size_binary(summary.processed_bytes, 3)
            .bold()
            .white()
            .to_string(),
    ]);
    data_table.add_row(vec![
        "Bundle size".to_string(),
        format_size_binary(final_size, 3).bold().green().to_string(),
    ]);

    let ratio = if summary.processed_bytes > 0 {
        (final_size as f64 / summary.processed_bytes as f64) * 100.0
    } else {
        0.0
    };

    data_table.add_row(vec![
        "Compression ratio".to_string(),
        format!("{:.1}%", ratio).bold().yellow().to_string(),
    ]);

    cli::log!("{}", data_table.render());
    cli::log!("Bundle completed successfully");
    tracing::info!(target: "bundle", "Bundle creation completed (size={})", final_size);

    Ok(())
}

fn parse_readers(s: &str) -> Result<usize, String> {
    parse_positive_usize(s).map_err(|e| e.replace("value", "readers"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;

    #[derive(Parser, Debug)]
    #[command(no_binary_name = true)]
    struct BundleArgsParse {
        #[command(flatten)]
        args: CmdArgs,
    }

    #[test]
    fn readers_rejects_zero() {
        let err = BundleArgsParse::try_parse_from([
            "--bundle",
            "src",
            "-o",
            "out.mapache",
            "--readers",
            "0",
        ])
        .expect_err("--readers 0 must be rejected");
        assert!(
            err.to_string().contains("greater than 0"),
            "unexpected error message: {err}"
        );
    }

    #[test]
    fn readers_accepts_positive() {
        let parsed = BundleArgsParse::try_parse_from([
            "--bundle",
            "src",
            "-o",
            "out.mapache",
            "--readers",
            "8",
        ])
        .expect("--readers 8 must parse");
        assert_eq!(parsed.args.readers, 8);
    }
}
