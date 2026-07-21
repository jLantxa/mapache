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
        SnapshotOptions, processor,
        progress::{SnapshotProcessSummary, SnapshotProgress},
        tree_serializer::TreeSerializer,
    },
    backend::{self, BackendOptions, StorageHint, WriteContents},
    bundle::{
        format::{BundleIndex, BundleIndexEntry},
        reader::BundleReader,
        writer::BundleWriter,
    },
    commands::{GlobalArgs, ToExitCode, UseSnapshot, find_use_snapshot, open_repository},
    common::{
        BlobType, ContentIdType, ID, SaveID,
        defaults::{DEFAULT_PACK_SIZE, DEFAULT_SNAPSHOT_READERS, SHORT_SNAPSHOT_ID_LEN},
        error::MapacheError,
        traits::{BlobLoader, BlobSaver},
    },
    fs::{
        calculate_lcp,
        filter::PathFilter,
        get_absolute_normalized_path,
        node::{Metadata, Node},
        tree::{FSNodeStream, NodeDiff, StreamNode, Tree},
    },
    repository::repo::RepoConfig,
    repository::repo::Repository,
    restorer::node_restorer,
    ui::{
        self,
        cli::{self, color::Colorize},
        default_bar_draw_target, default_progress_style,
        events::{BackupEvent, Event, EventSender, RestoreEvent},
    },
    utils::{self, collections::IdSet, format_size_binary, rate_estimator::RateEstimator},
};
#[cfg(all(feature = "mount", unix))]
use crate::{
    commands::cleanup::CleanupHandler,
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
            BundleError::Config(_) => 10,
        }
    }
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

    /// Number of parallel readers
    #[clap(long, default_value_t = DEFAULT_SNAPSHOT_READERS)]
    pub readers: usize,

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
            readers: DEFAULT_SNAPSHOT_READERS,
            create: false,
            allow_other: false,
            metadata_only: false,
            data_cache_size_mib: 256.0,
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
            readers: DEFAULT_SNAPSHOT_READERS,
            internal_password: None,
        }
    }
}

pub async fn run(global: &crate::commands::GlobalArgs, args: &CmdArgs) -> Result<(), BundleError> {
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

    let bundle_writer = Arc::new(
        BundleWriter::new(output, &password, global.compression_level.to_level())
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

    let scanner_sender = event_sender.clone();
    let scanner_paths = absolute_source_paths.clone();
    let scanner_exclude = exclude_paths.clone();
    let scanner_shutdown = shutdown_signal.clone();
    let scanner_handle = tokio::spawn(async move {
        spawn_background_scanner(
            scanner_paths,
            scanner_exclude,
            scanner_sender,
            scanner_shutdown,
        )
        .await;
    });

    let snapshot_options = SnapshotOptions {
        absolute_source_paths,
        snapshot_root_path: snapshot_root_path.clone(),
        exclude_paths: exclude_paths.clone(),
        parent_snapshot: None,
        tags: Default::default(),
        description: Some(format!("Bundle of {:?}", args.input)),
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

    let (processed_tx, mut processed_rx) = tokio::sync::mpsc::channel(4096);

    let saver: Arc<dyn BlobSaver> = bundle_writer.clone();
    let process_shutdown = shutdown_signal.clone();
    let process_progress = progress.clone();
    let process_readers = args.readers;

    let process_sender = event_sender.clone();
    let process_task = tokio::spawn(async move {
        fs_stream
            .for_each_concurrent(process_readers, |item| {
                let saver = saver.clone();
                let progress = process_progress.clone();
                let sender = process_sender.clone();
                let signal = process_shutdown.clone();
                let tx = processed_tx.clone();

                async move {
                    if signal.load(std::sync::atomic::Ordering::Relaxed) {
                        return;
                    }

                    let (path, stream_node_res) = match item {
                        Ok(v) => v,
                        Err(e) => {
                            sender(Event::Backup(BackupEvent::Error(format!(
                                "scan error: {}",
                                e
                            ))));
                            return;
                        }
                    };

                    let stream_node = match stream_node_res {
                        Ok(v) => v,
                        Err(e) => {
                            sender(Event::Backup(BackupEvent::Error(format!(
                                "node error: {}",
                                e
                            ))));
                            return;
                        }
                    };

                    if !stream_node.node.is_dir() {
                        sender(Event::Backup(BackupEvent::NodeProcessing {
                            path: path.clone(),
                            diff: NodeDiff::New,
                            size_hint: Some(stream_node.node.metadata.size),
                        }));
                    }

                    let mut node = stream_node.node;
                    if node.is_file() {
                        let file_size = node.metadata.size;
                        let path_str = path.display().to_string();
                        let saver_clone = saver.clone();
                        let progress_clone = progress.clone();
                        let signal_clone = signal.clone();
                        let chunk_sender = sender.clone();

                        let blobs_res = match tokio::task::spawn_blocking(move || {
                            let file = std::fs::File::open(&path_str)?;
                            processor::chunk_and_store_file(
                                saver_clone.as_ref(),
                                file,
                                file_size,
                                progress_clone.as_ref(),
                                &chunk_sender,
                                signal_clone.as_ref(),
                            )
                        })
                        .await
                        {
                            Ok(res) => res,
                            Err(e) => {
                                sender(Event::Backup(BackupEvent::Error(format!(
                                    "chunking task panicked for {}: {}",
                                    path.display(),
                                    e
                                ))));
                                return;
                            }
                        };

                        match blobs_res {
                            Ok(blobs) => node.blobs = Some(blobs),
                            Err(e) => {
                                sender(Event::Backup(BackupEvent::Error(format!(
                                    "error chunking {}: {}",
                                    path.display(),
                                    e
                                ))));
                                return;
                            }
                        }
                    }

                    progress.processed_node();
                    sender(Event::Backup(BackupEvent::NodeProcessed {
                        path: path.clone(),
                        diff: NodeDiff::New,
                        size_hint: Some(node.metadata.size),
                    }));

                    let _ = tx
                        .send((
                            path,
                            StreamNode {
                                node,
                                num_children: stream_node.num_children,
                            },
                        ))
                        .await;
                }
            })
            .await;
    });

    let mut tree_serializer = TreeSerializer::new(
        bundle_writer.clone(),
        snapshot_root_path.clone(),
        &snapshot_options.absolute_source_paths,
    );

    while let Some((path_buf, stream_node)) = processed_rx.recv().await {
        tree_serializer
            .handle_processed_item((&path_buf, stream_node))
            .await?;
    }

    process_task
        .await
        .map_err(|e| BundleError::BundleFailed(format!("process task panicked: {e}")))?;

    let _ = scanner_handle.await;

    tree_serializer.finalize_root().await?;
    let root_tree_id = tree_serializer
        .root_tree()
        .ok_or_else(|| BundleError::BundleFailed("root tree ID not set".to_string()))?;

    let summary = progress.summary();
    event_sender(Event::Backup(BackupEvent::Finished(summary)));

    writer_finalize(bundle_writer.as_ref(), root_tree_id, output, &progress).await
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

    // Open repository
    let repo_config = RepoConfig {
        pack_size: DEFAULT_PACK_SIZE,
        use_cache: !global.no_cache,
        compression: global.compression_level,
    };

    let repo_backend = backend::new_backend_with_prompt(BackendOptions {
        repo_path: global.repo.clone(),
        ssh_privatekey: global.ssh_privatekey.clone(),
        ssh_known_hosts: global.ssh_known_hosts.clone(),
        dry_backend: false,
        limit_upload: global.limit_upload,
        limit_download: global.limit_download,
    })
    .await
    .map_err(|e| {
        BundleError::BundleFailed(format!(
            "failed to initialize repository backend: {}",
            e.inner()
        ))
    })?;

    let (repo, _ss) = open_repository(
        global.auth_file.as_ref(),
        global.key.as_ref(),
        repo_backend,
        repo_config,
    )
    .await
    .map_err(|e| BundleError::BundleFailed(e.to_string()))?;

    repo.reload_master_index().await?;

    // Resolve snapshot (supports "latest", prefix, or full ID)
    let (snap_id, snapshot) = find_use_snapshot(repo.clone(), use_snapshot)
        .await
        .map_err(|e| BundleError::BundleFailed(e.to_string()))?
        .ok_or_else(|| {
            BundleError::BundleFailed(format!("no snapshot found matching '{}'", use_snapshot))
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
    let bundle_writer = Arc::new(
        BundleWriter::new(output, &password, global.compression_level.to_level())
            .map_err(|e| BundleError::BundleFailed(e.to_string()))?,
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

    tree_stack.push(snapshot.tree);

    while let Some(tree_id) = tree_stack.pop() {
        if !visited_trees.insert(tree_id) {
            continue;
        }
        blob_list.push((tree_id, BlobType::Tree));
        if let Some(loc) = repo.index().get(&tree_id) {
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
                            if let Some(loc) = repo.index().get(&blob_id) {
                                total_bytes += loc.raw_length as u64;
                            }
                        }
                    }
                }
            }
            Err(e) => {
                return Err(BundleError::BundleFailed(format!(
                    "failed to load tree {}: {}",
                    tree_id, e
                )));
            }
        }
    }

    // Write blobs to bundle concurrently
    let start = Instant::now();
    let total = blob_list.len();

    let export_rate = Arc::new(Mutex::new(RateEstimator::new(
        crate::common::defaults::UI_RATE_ESTIMATOR_WINDOW,
    )));
    let bar = if !global.json {
        Some(
            ProgressBar::with_draw_target(Some(total_bytes), default_bar_draw_target()).with_style(
                default_progress_style()
                    .template("[{percent}%] [{bar:20.cyan/white}] [{custom_elapsed}] {bytes_fmt} / {total_fmt} [{data_rate}/s] [ETA: {custom_eta}]")
                    .expect("invalid progress bar template for bundle export")
                    .with_key("bytes_fmt", |state: &ProgressState, w: &mut dyn fmt::Write| {
                        let _ = w.write_str(&format_size_binary(state.pos(), 2));
                    })
                    .with_key("total_fmt", |state: &ProgressState, w: &mut dyn fmt::Write| {
                        let _ = w.write_str(&format_size_binary(state.len().unwrap_or(0), 2));
                    })
                    .with_key("custom_elapsed", |state: &ProgressState, w: &mut dyn fmt::Write| {
                        let _ = w.write_str(&utils::pretty_print_duration(state.elapsed()));
                    })
                    .with_key("custom_eta", {
                        let re = export_rate.clone();
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
                        let re = export_rate.clone();
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

    let results: Vec<Result<(), BundleError>> = stream::iter(blob_list)
        .map(|(blob_id, blob_type)| {
            let repo = repo.clone();
            let bundle_writer = bundle_writer.clone();
            let bar = bar.clone();
            let export_rate = export_rate.clone();
            async move {
                let data = repo.load_blob(&blob_id).await.map_err(|e| {
                    BundleError::BundleFailed(format!("failed to load blob {}: {}", blob_id, e))
                })?;

                let blob_size = data.len() as u64;

                bundle_writer
                    .save_blob(
                        blob_type,
                        WriteContents::Owned(data),
                        SaveID::WithID(blob_id),
                    )
                    .map_err(|e| {
                        BundleError::BundleFailed(format!("failed to save blob to bundle: {}", e))
                    })?;

                if let Some(ref bar) = bar {
                    bar.inc(blob_size);
                    export_rate.lock().observe(bar.position() as f64);
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

    // Finalize bundle
    bundle_writer
        .finalize(snapshot.tree)
        .map_err(|e| BundleError::BundleFailed(e.to_string()))?;

    let final_size = std::fs::metadata(output)
        .map_err(|e| BundleError::BundleFailed(format!("failed to stat bundle file: {}", e)))?
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

    let import_rate = Arc::new(Mutex::new(RateEstimator::new(
        crate::common::defaults::UI_RATE_ESTIMATOR_WINDOW,
    )));
    let bar = if !global.json {
        Some(
            ProgressBar::with_draw_target(Some(import_bytes), default_bar_draw_target()).with_style(
                default_progress_style()
                    .template("[{percent}%] [{bar:20.cyan/white}] [{custom_elapsed}] {bytes_fmt} / {total_fmt} [{data_rate}/s] [ETA: {custom_eta}]")
                    .expect("invalid progress bar template for bundle import")
                    .with_key("bytes_fmt", |state: &ProgressState, w: &mut dyn fmt::Write| {
                        let _ = w.write_str(&format_size_binary(state.pos(), 2));
                    })
                    .with_key("total_fmt", |state: &ProgressState, w: &mut dyn fmt::Write| {
                        let _ = w.write_str(&format_size_binary(state.len().unwrap_or(0), 2));
                    })
                    .with_key("custom_elapsed", |state: &ProgressState, w: &mut dyn fmt::Write| {
                        let _ = w.write_str(&utils::pretty_print_duration(state.elapsed()));
                    })
                    .with_key("custom_eta", {
                        let re = import_rate.clone();
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
                        let re = import_rate.clone();
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
        paths: vec![bundle_path.to_path_buf()],
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

    // Open repository once for all bundles
    let repo_config = RepoConfig {
        pack_size: DEFAULT_PACK_SIZE,
        use_cache: !global.no_cache,
        compression: global.compression_level,
    };

    let repo_backend = backend::new_backend_with_prompt(BackendOptions {
        repo_path: global.repo.clone(),
        ssh_privatekey: global.ssh_privatekey.clone(),
        ssh_known_hosts: global.ssh_known_hosts.clone(),
        dry_backend: false,
        limit_upload: global.limit_upload,
        limit_download: global.limit_download,
    })
    .await
    .map_err(|e| {
        BundleError::BundleFailed(format!(
            "failed to initialize repository backend: {}",
            e.inner()
        ))
    })?;

    let (repo, _ss) = open_repository(
        global.auth_file.as_ref(),
        global.key.as_ref(),
        repo_backend,
        repo_config,
    )
    .await
    .map_err(|e| BundleError::BundleFailed(e.to_string()))?;

    repo.reload_master_index().await?;
    repo.init_pack_saver(args.readers)?;

    for bundle_path in &args.input {
        run_import_bundle(&repo, bundle_path, &password, global, args).await?;
    }

    // Flush pack saver
    if !global.json {
        cli::log!(
            "{} Persisting repository index...",
            "[finalize]".bold().cyan()
        );
    }
    repo.flush_and_finalize_pack_saver()
        .await
        .map_err(|e| BundleError::BundleFailed(format!("failed to finalize packs: {}", e)))?;

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
        "Mount mode requires FUSE support on Unix systems. Compile with the 'fuse' feature."
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

async fn scan_bundle_tree<L>(loader: Arc<L>, tree_id: &ID) -> Result<(usize, u64), BundleError>
where
    L: BlobLoader + ?Sized + 'static,
{
    let mut total_items = 0;
    let mut total_bytes = 0;
    let mut stack = vec![*tree_id];

    while let Some(current_id) = stack.pop() {
        let data = loader
            .load_blob(&current_id)
            .await
            .map_err(|e| BundleError::BundleFailed(e.to_string()))?;
        let tree: Tree =
            serde_json::from_slice(&data).map_err(|e| BundleError::BundleFailed(e.to_string()))?;

        for node in tree.nodes {
            total_items += 1;
            if node.is_dir() {
                if let Some(subtree_id) = node.tree {
                    stack.push(subtree_id);
                }
            } else if node.is_file() {
                total_bytes += node.metadata.size;
            }
        }
    }
    Ok((total_items, total_bytes))
}

async fn extract_nodes_parallel<L>(
    loader: Arc<L>,
    root_id: &ID,
    destination: &Path,
    workers: usize,
    event_sender: EventSender,
) -> Result<(), BundleError>
where
    L: BlobLoader + ?Sized + 'static,
{
    let (tx, rx) = tokio::sync::mpsc::channel::<(PathBuf, Node)>(4096);
    let (dir_tx, dir_rx) = tokio::sync::mpsc::channel::<(PathBuf, Metadata)>(4096);

    let loader_clone = loader.clone();
    let dest_clone = destination.to_path_buf();
    let sender_clone = event_sender.clone();
    let root_id_val = *root_id;

    let walk_task = tokio::spawn(async move {
        let mut stack = vec![(dest_clone, root_id_val)];
        while let Some((current_dest, current_id)) = stack.pop() {
            let data = match loader_clone.load_blob(&current_id).await {
                Ok(d) => d,
                Err(e) => {
                    sender_clone(Event::Backup(BackupEvent::Error(format!(
                        "failed to load tree {}: {}",
                        current_id, e
                    ))));
                    continue;
                }
            };
            let tree: Tree = match serde_json::from_slice(&data) {
                Ok(t) => t,
                Err(e) => {
                    sender_clone(Event::Backup(BackupEvent::Error(format!(
                        "failed to parse tree {}: {}",
                        current_id, e
                    ))));
                    continue;
                }
            };

            for node in tree.nodes {
                let node_path = current_dest.join(&node.name);
                if node.is_dir() {
                    let _ = std::fs::create_dir_all(&node_path);
                    let _ = dir_tx
                        .send((node_path.clone(), node.metadata.clone()))
                        .await;
                    if let Some(subtree_id) = node.tree {
                        stack.push((node_path.clone(), subtree_id));
                    }
                }
                if let Err(e) = tx.send((node_path, node)).await {
                    sender_clone(Event::Backup(BackupEvent::Error(format!(
                        "internal channel error: {}",
                        e
                    ))));
                    break;
                }
            }
        }
    });

    let meta_sender = make_meta_sender();

    let process_future = async {
        let stream = tokio_stream::wrappers::ReceiverStream::new(rx);
        stream
            .for_each_concurrent(workers, |(path, node)| {
                let loader = loader.clone();
                let sender = event_sender.clone();
                let meta_sender = meta_sender.clone();
                async move {
                    sender(Event::Backup(BackupEvent::NodeProcessing {
                        path: path.clone(),
                        diff: NodeDiff::New,
                        size_hint: Some(node.metadata.size),
                    }));

                    if !node.is_file() {
                        if node.is_symlink()
                            && let Some(symlink_info) = &node.symlink_info
                        {
                            #[cfg(unix)]
                            {
                                use std::os::unix::fs::symlink;
                                if symlink(&symlink_info.target_path, &path).is_ok() {
                                    node_restorer::try_restore_node_metadata(
                                        &node.metadata,
                                        true,
                                        &path,
                                        &meta_sender,
                                    );
                                }
                            }

                            #[cfg(not(unix))]
                            let _ = symlink_info;
                        }
                        sender(Event::Backup(BackupEvent::NodeProcessed {
                            path: path.clone(),
                            diff: NodeDiff::New,
                            size_hint: Some(node.metadata.size),
                        }));
                        return;
                    }

                    let blobs = match &node.blobs {
                        Some(b) => b,
                        None => {
                            sender(Event::Backup(BackupEvent::NodeProcessed {
                                path: path.clone(),
                                diff: NodeDiff::New,
                                size_hint: Some(node.metadata.size),
                            }));
                            return;
                        }
                    };

                    let mut file = match std::fs::File::create(&path) {
                        Ok(f) => f,
                        Err(e) => {
                            sender(Event::Backup(BackupEvent::Error(format!(
                                "failed to create file {}: {}",
                                path.display(),
                                e
                            ))));
                            sender(Event::Backup(BackupEvent::NodeProcessed {
                                path: path.clone(),
                                diff: NodeDiff::New,
                                size_hint: Some(node.metadata.size),
                            }));
                            return;
                        }
                    };

                    let mut success = true;
                    for blob_id in blobs {
                        let data = match loader.load_blob(blob_id).await {
                            Ok(d) => d,
                            Err(e) => {
                                sender(Event::Backup(BackupEvent::Error(format!(
                                    "failed to load blob {} for {}: {}",
                                    blob_id,
                                    path.display(),
                                    e
                                ))));
                                success = false;
                                break;
                            }
                        };

                        use std::io::Write;
                        if let Err(e) = file.write_all(&data) {
                            sender(Event::Backup(BackupEvent::Error(format!(
                                "failed to write to {}: {}",
                                path.display(),
                                e
                            ))));
                            success = false;
                            break;
                        }
                        sender(Event::Backup(
                            BackupEvent::BytesProcessed(data.len() as u64),
                        ));
                    }

                    drop(file);
                    if success {
                        node_restorer::try_restore_node_metadata(
                            &node.metadata,
                            false,
                            &path,
                            &meta_sender,
                        );
                    }

                    sender(Event::Backup(BackupEvent::NodeProcessed {
                        path: path.clone(),
                        diff: NodeDiff::New,
                        size_hint: Some(node.metadata.size),
                    }));
                }
            })
            .await;
    };

    let _ = futures::join!(walk_task, process_future);

    let mut directories: Vec<(PathBuf, Metadata)> = Vec::new();
    let mut dir_rx = dir_rx;
    while let Some((path, meta)) = dir_rx.recv().await {
        directories.push((path, meta));
    }

    directories.sort_unstable_by_key(|(p, _)| std::cmp::Reverse(p.as_os_str().len()));
    for (p, meta) in directories {
        node_restorer::try_restore_node_metadata(&meta, false, &p, &meta_sender);
    }

    Ok(())
}

fn make_meta_sender() -> EventSender {
    Arc::new(|event: Event| {
        if let Event::Restore(RestoreEvent::Warning(ref msg)) = event {
            ui::cli::warning!("{}", msg);
        } else if let Event::Restore(RestoreEvent::Error(ref msg)) = event {
            ui::cli::error!("{}", msg);
        }
    })
}

async fn spawn_background_scanner(
    paths: Vec<PathBuf>,
    exclude: Vec<PathBuf>,
    event_sender: EventSender,
    shutdown: Arc<AtomicBool>,
) {
    let filter = Arc::new(PathFilter::new(None, Some(exclude)));
    let sender_for_closure = event_sender.clone();

    let res = tokio::task::spawn_blocking(move || {
        use rayon::prelude::*;
        paths.into_par_iter().for_each(|path| {
            let scanner = BundleScanner {
                event_sender: sender_for_closure.clone(),
                filter: filter.clone(),
                shutdown: shutdown.clone(),
            };
            scanner.scan_recursive(&path);
        });
    })
    .await;

    if let Err(e) = res {
        event_sender(Event::Backup(BackupEvent::Error(format!(
            "background scanner panicked: {}",
            e
        ))));
    }

    event_sender(Event::Backup(BackupEvent::ScanFinished {
        total_items: 0,
        total_bytes: 0,
    }));
}

struct BundleScanner {
    event_sender: EventSender,
    filter: Arc<PathFilter>,
    shutdown: Arc<AtomicBool>,
}

impl BundleScanner {
    fn scan_recursive(&self, path: &std::path::Path) {
        if self.shutdown.load(std::sync::atomic::Ordering::Relaxed) {
            return;
        }
        if !self.filter.allow(path) {
            return;
        }

        if let Ok(node) = Node::from_path_sync(path, false) {
            (self.event_sender)(Event::Backup(BackupEvent::ScanProgress {
                items: 1,
                bytes: if node.is_file() {
                    node.metadata.size
                } else {
                    0
                },
            }));

            if node.is_dir()
                && let Ok(entries) = std::fs::read_dir(path)
            {
                use rayon::prelude::*;
                entries.par_bridge().for_each(|entry_res| {
                    if let Ok(entry) = entry_res {
                        self.scan_recursive(&entry.path());
                    }
                });
            }
        }
    }
}
