use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::AtomicBool;

use anyhow::{Context, Result, bail};
use clap::{ArgGroup, Args};
use colored::Colorize;
use futures::StreamExt;

#[cfg(all(feature = "fuse", unix))]
use crate::{commands::cleanup::CleanupHandler, fuse::fs::MapacheFS, utils::size};

use crate::{
    archiver::{SnapshotOptions, progress::SnapshotProgress},
    bundle::reader::BundleReader,
    bundle::writer::BundleWriter,
    fs::{node::Metadata, tree::Tree},
    mapache::{ID, traits::BlobLoader, traits::BlobSaver},
    ui::snapshot::SnapshotProgressReporter,
};

struct BundleRestoreReporterAdapter {
    inner: Arc<dyn SnapshotProgressReporter>,
}

impl crate::ui::restore::RestoreProgressReporter for BundleRestoreReporterAdapter {
    fn set_message(&self, _msg: String) {}
    fn resize_workload(&self, _num_expected_items: u64, _num_expected_bytes: u64) {}
    fn processed_item(&self, _path: &Path) {}
    fn processed_bytes(&self, _bytes: u64) {}
    fn error(&self, msg: &str) {
        self.inner.error(msg);
    }
    fn warning(&self, msg: &str) {
        self.inner.warning(msg);
    }
    fn error_count(&self) -> u64 {
        0
    }
    fn warning_count(&self) -> u64 {
        0
    }
    fn finalize(&self) {}
}

#[derive(Debug, Args)]
#[clap(
    about = "Create, extract or mount .mapache bundle files",
    group = ArgGroup::new("mode").required(true).args(&["bundle", "extract"]),
)]
pub struct CmdArgs {
    /// Bundle mode: create a new bundle from source paths
    #[arg(short, long, group = "mode")]
    pub bundle: bool,

    /// Extract mode: extract a bundle to a destination
    #[arg(short = 'x', long, group = "mode")]
    pub extract: bool,

    /// Mount mode: mount a bundle as a filesystem (FUSE)
    #[cfg(all(feature = "fuse", unix))]
    #[arg(short, long, group = "mode")]
    pub mount: bool,

    /// Input: source paths (-a), bundle file (-x), or bundle + mountpoint (-m)
    #[arg(required = true)]
    pub input: Vec<PathBuf>,

    /// Output: bundle file (-a) or destination directory (-x). Not used with -m.
    #[arg(short, long)]
    pub output: Option<PathBuf>,

    /// Glob patterns for paths to exclude (bundle mode only)
    #[arg(short = 'e', long)]
    pub exclude: Vec<PathBuf>,

    /// Compression level [fastest|fast|balanced|better|best|level:val] (bundle mode only)
    #[clap(long = "compression", value_parser = crate::commands::parse_compression_level, default_value_t = crate::commands::DEFAULT_COMPRESSION)]
    pub compression_level: crate::commands::Compression,

    /// Number of parallel readers
    #[clap(long, default_value_t = crate::mapache::defaults::DEFAULT_SNAPSHOT_READERS)]
    pub readers: usize,

    /// Create mountpoint if it does not exist (mount mode only, passes to mount -c)
    #[cfg(all(feature = "fuse", unix))]
    #[arg(short, long, default_value_t = false)]
    pub create: bool,

    /// Allow other users to access the mount (mount mode only)
    #[cfg(all(feature = "fuse", unix))]
    #[arg(long, default_value_t = false)]
    pub allow_other: bool,

    /// Display files but do not load contents (mount mode only)
    #[cfg(all(feature = "fuse", unix))]
    #[arg(long, default_value_t = false)]
    pub metadata_only: bool,

    /// Max size of internal data cache in MiB (mount mode only)
    #[cfg(all(feature = "fuse", unix))]
    #[arg(long = "cache-size-mib", default_value_t = 256.0)]
    pub data_cache_size_mib: f32,

    #[arg(skip)]
    pub internal_password: Option<String>,
}

#[cfg(all(feature = "fuse", unix))]
impl Default for CmdArgs {
    fn default() -> Self {
        Self {
            bundle: false,
            extract: false,
            mount: false,
            input: vec![],
            output: None,
            exclude: vec![],
            compression_level: crate::commands::Compression::Balanced,
            readers: crate::mapache::defaults::DEFAULT_SNAPSHOT_READERS,
            create: false,
            allow_other: false,
            metadata_only: false,
            data_cache_size_mib: 256.0,
            internal_password: None,
        }
    }
}

#[cfg(not(all(feature = "fuse", unix)))]
impl Default for CmdArgs {
    fn default() -> Self {
        Self {
            bundle: false,
            extract: false,
            input: vec![],
            output: None,
            exclude: vec![],
            compression_level: crate::commands::Compression::Balanced,
            readers: crate::mapache::defaults::DEFAULT_SNAPSHOT_READERS,
            internal_password: None,
        }
    }
}

pub async fn run(args: &CmdArgs) -> Result<()> {
    if args.bundle {
        run_create(args).await
    } else if args.extract {
        run_extract(args).await
    } else {
        run_mount(args).await
    }
}

async fn run_create(args: &CmdArgs) -> Result<()> {
    let output = args
        .output
        .as_ref()
        .context("-o is required for bundle mode")?;

    let password = match &args.internal_password {
        Some(p) => zeroize::Zeroizing::new(p.clone()),
        None => crate::ui::cli::request_new_password("Enter bundle password", "Confirm password")?,
    };

    let bundle_writer = Arc::new(BundleWriter::new(
        output,
        &password,
        args.compression_level.to_level(),
    )?);
    let shutdown_signal = Arc::new(AtomicBool::new(false));
    let progress = Arc::new(SnapshotProgress::new());

    let absolute_source_paths: Vec<PathBuf> = args
        .input
        .iter()
        .map(|p| p.canonicalize().unwrap_or(p.clone()))
        .collect();

    let snapshot_root_path = if absolute_source_paths.len() == 1 {
        let p = &absolute_source_paths[0];
        p.parent().unwrap_or(p).to_path_buf()
    } else {
        crate::fs::calculate_lcp(&absolute_source_paths, false)
    };

    let progress_reporter: Arc<dyn SnapshotProgressReporter> =
        Arc::new(crate::ui::bundle::cli::BundleCliProgressReporter::new(
            crate::ui::bundle::cli::BundleMode::Create,
            0,
            0,
            args.readers,
        ));

    let scanner_reporter = progress_reporter.clone();
    let scanner_paths = absolute_source_paths.clone();
    let scanner_exclude = args.exclude.clone();
    let scanner_shutdown = shutdown_signal.clone();
    let scanner_handle = tokio::spawn(async move {
        spawn_background_scanner(
            scanner_paths,
            scanner_exclude,
            scanner_reporter,
            scanner_shutdown,
        )
        .await;
    });

    crate::ui::cli::log!(
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

    let snapshot_options = SnapshotOptions {
        absolute_source_paths,
        snapshot_root_path: snapshot_root_path.clone(),
        exclude_paths: args.exclude.clone(),
        parent_snapshot: None,
        tags: Default::default(),
        description: Some(format!("Bundle of {:?}", args.input)),
        no_scan: false,
    };

    let fs_stream = crate::fs::tree::FSNodeStream::from_paths(
        snapshot_options.absolute_source_paths.clone(),
        snapshot_options.exclude_paths.clone(),
    )
    .await?;

    let (processed_tx, mut processed_rx) = tokio::sync::mpsc::channel(4096);

    let saver: Arc<dyn BlobSaver> = bundle_writer.clone();
    let process_shutdown = shutdown_signal.clone();
    let process_progress = progress.clone();
    let process_reporter = progress_reporter.clone();
    let process_readers = args.readers;

    let process_task = tokio::spawn(async move {
        fs_stream
            .for_each_concurrent(process_readers, |item| {
                let saver = saver.clone();
                let progress = process_progress.clone();
                let reporter = process_reporter.clone();
                let signal = process_shutdown.clone();
                let tx = processed_tx.clone();

                async move {
                    if signal.load(std::sync::atomic::Ordering::Relaxed) {
                        return;
                    }

                    let (path, stream_node_res) = match item {
                        Ok(v) => v,
                        Err(e) => {
                            reporter.error(&format!("Scan error: {}", e));
                            return;
                        }
                    };

                    let stream_node = match stream_node_res {
                        Ok(v) => v,
                        Err(e) => {
                            reporter.error(&format!("Node error: {}", e));
                            return;
                        }
                    };

                    if !stream_node.node.is_dir() {
                        reporter.processing_node(
                            &path,
                            crate::fs::tree::NodeDiff::New,
                            Some(stream_node.node.metadata.size),
                        );
                    }

                    let mut node = stream_node.node;
                    if node.is_file() {
                        let file_size = node.metadata.size;
                        let path_str = path.display().to_string();
                        let saver_clone = saver.clone();
                        let progress_clone = progress.clone();
                        let reporter_clone = reporter.clone();
                        let signal_clone = signal.clone();

                        let blobs_res = tokio::task::spawn_blocking(move || {
                            let file = std::fs::File::open(&path_str)?;
                            crate::archiver::processor::chunk_and_store_file(
                                saver_clone,
                                file,
                                file_size,
                                progress_clone,
                                reporter_clone,
                                signal_clone,
                            )
                        })
                        .await
                        .expect("Task panicked");

                        match blobs_res {
                            Ok(blobs) => node.blobs = Some(blobs),
                            Err(e) => {
                                reporter.error(&format!(
                                    "Error chunking {}: {}",
                                    path.display(),
                                    e
                                ));
                                return;
                            }
                        }
                    }

                    progress.processed_node();
                    reporter.processed_node(
                        &path,
                        crate::fs::tree::NodeDiff::New,
                        Some(node.metadata.size),
                    );

                    let _ = tx
                        .send((
                            path,
                            crate::fs::tree::StreamNode {
                                node,
                                num_children: stream_node.num_children,
                            },
                        ))
                        .await;
                }
            })
            .await;
    });

    let mut tree_serializer = crate::archiver::tree_serializer::TreeSerializer::new(
        bundle_writer.clone(),
        snapshot_root_path.clone(),
        &snapshot_options.absolute_source_paths,
    );

    while let Some((path_buf, stream_node)) = processed_rx.recv().await {
        tree_serializer
            .handle_processed_item((&path_buf, stream_node))
            .await?;
    }

    process_task.await?;

    let _ = scanner_handle.await;

    tree_serializer.finalize_root().await?;
    let root_tree_id = tree_serializer
        .root_tree()
        .context("Root tree ID not set")?;

    writer_finalize(bundle_writer.as_ref(), root_tree_id, output, &progress).await
}

async fn run_extract(args: &CmdArgs) -> Result<()> {
    if args.input.len() != 1 {
        bail!("Extract mode requires exactly one bundle file as input");
    }
    let bundle = &args.input[0];
    let destination = args.output.as_deref().unwrap_or(std::path::Path::new("."));

    let password = match &args.internal_password {
        Some(p) => zeroize::Zeroizing::new(p.clone()),
        None => crate::ui::cli::request_password("Enter bundle password")?,
    };

    let reader = BundleReader::open(bundle, &password)?;
    let root_tree_id = reader.trailer.root_tree;
    let loader = Arc::new(reader);

    crate::ui::cli::log!("{} Analyzing bundle...", "[1/2]".bold().cyan());
    let (total_items, total_bytes) = scan_bundle_tree(loader.clone(), &root_tree_id).await?;

    let progress_reporter = Arc::new(crate::ui::bundle::cli::BundleCliProgressReporter::new(
        crate::ui::bundle::cli::BundleMode::Extract,
        total_items as u64,
        total_bytes,
        args.readers,
    ));

    crate::ui::cli::log!(
        "{} Extracting {} to {}...",
        "[2/2]".bold().cyan(),
        bundle.display().to_string().bold(),
        destination.display().to_string().bold()
    );

    if !destination.exists() {
        std::fs::create_dir_all(destination)?;
    }

    extract_nodes_parallel(
        loader.clone(),
        &root_tree_id,
        destination,
        args.readers,
        progress_reporter.clone(),
    )
    .await?;

    progress_reporter.finalize();

    crate::ui::cli::log!("");
    crate::ui::cli::log!("{}", "Extraction Summary:".bold().cyan());

    let mut data_table = crate::ui::table::Table::new();
    data_table.add_row(vec![
        "Extracted items".to_string(),
        total_items.to_string().bold().white().to_string(),
    ]);
    data_table.add_row(vec![
        "Total size".to_string(),
        crate::utils::format_size_binary(total_bytes, 3)
            .bold()
            .white()
            .to_string(),
    ]);

    crate::ui::cli::log!("{}", data_table.render());
    crate::ui::cli::log!("{}", "Extraction completed successfully!".green().bold());

    Ok(())
}

#[cfg(all(feature = "fuse", unix))]
async fn run_mount(args: &CmdArgs) -> Result<()> {
    if args.input.len() != 2 {
        bail!("Mount mode requires: bundle.mapache <mountpoint>");
    }
    let bundle = &args.input[0];
    let mountpoint = &args.input[1];

    let actual_mountpoint = mountpoint.clone();
    let mut created_mountpoint = false;

    if !crate::fs::path_exists(&actual_mountpoint).await {
        if args.create {
            std::fs::create_dir_all(&actual_mountpoint).context("Could not create mount point")?;
            created_mountpoint = true;
        } else {
            bail!("Mountpoint doesn't exist. Use -c to create it automatically.");
        }
    } else if !actual_mountpoint.is_dir() {
        bail!("Mountpoint must be a directory");
    }

    let canonical_mountpoint = crate::fs::get_absolute_normalized_path(&actual_mountpoint)?;

    let password = match &args.internal_password {
        Some(p) => zeroize::Zeroizing::new(p.clone()),
        None => crate::ui::cli::request_password("Enter bundle password")?,
    };

    let reader = BundleReader::open(bundle, &password)?;
    let root_tree_id = reader.trailer.root_tree;
    let loader: Arc<dyn BlobLoader> = Arc::new(reader);

    let cleanup_handler = CleanupHandler::new()?;
    crate::ui::cli::log!(
        "Mounting bundle {} in {}",
        bundle.display().to_string().bold(),
        canonical_mountpoint.display()
    );

    let data_cache_size = (args.data_cache_size_mib * size::MiB as f32) as u64;
    let allow_other = args.allow_other;
    let metadata_only = args.metadata_only;
    let mp_clone = canonical_mountpoint.clone();

    run_mount_loop(&canonical_mountpoint, cleanup_handler, move |mp| {
        MapacheFS::mount_loader(
            loader,
            None,
            Some(root_tree_id),
            mp,
            crate::fuse::fs::MountOptions {
                allow_other,
                metadata_only,
                data_cache_size,
                created_time: chrono::Local::now(),
            },
        )
    })
    .await?;

    if created_mountpoint {
        let _ = std::fs::remove_dir_all(&mp_clone);
    }

    Ok(())
}

#[cfg(not(all(feature = "fuse", unix)))]
async fn run_mount(_args: &CmdArgs) -> Result<()> {
    bail!("Mount mode requires FUSE support on Unix systems. Compile with the 'fuse' feature.");
}

#[cfg(all(feature = "fuse", unix))]
async fn run_mount_loop<F>(
    mountpoint: &std::path::Path,
    cleanup_handler: CleanupHandler,
    mount_fn: F,
) -> Result<()>
where
    F: FnOnce(&std::path::Path) -> Result<()> + Send + 'static,
{
    crate::ui::cli::log!(
        "Press {} to finish or unmount the filesystem manually.",
        "Ctrl+C".bold()
    );

    let mp_clone = mountpoint.to_path_buf();
    let mount_res = tokio::task::spawn_blocking(move || mount_fn(&mp_clone));

    tokio::select! {
        res = mount_res => {
            res.context("Mount task panicked")??;
        }
        _ = async {
            loop {
                if cleanup_handler.is_interrupted() {
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            }
        } => {
            crate::ui::cli::log!("Interrupt received. Unmounting...");
            let _ = MapacheFS::<dyn BlobLoader>::unmount(mountpoint);
        }
    }
    Ok(())
}

async fn writer_finalize(
    writer: &BundleWriter,
    root_tree_id: ID,
    output_path: &PathBuf,
    progress: &SnapshotProgress,
) -> Result<()> {
    writer.finalize(root_tree_id)?;

    let final_size = std::fs::metadata(output_path)?.len();
    let summary = progress.summary();

    crate::ui::cli::log!("");
    crate::ui::cli::log!("{}", "Bundle Summary:".bold().cyan());

    let mut data_table = crate::ui::table::Table::new();
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
        crate::utils::format_size_binary(summary.processed_bytes, 3)
            .bold()
            .white()
            .to_string(),
    ]);
    data_table.add_row(vec![
        "Bundle size".to_string(),
        crate::utils::format_size_binary(final_size, 3)
            .bold()
            .green()
            .to_string(),
    ]);

    let ratio = if summary.processed_bytes > 0 {
        (final_size as f64 / summary.processed_bytes as f64) * 100.0
    } else {
        0.0
    };

    data_table.add_row(vec![
        "Compression ratio".to_string(),
        format!("{:.3}%", ratio).bold().yellow().to_string(),
    ]);

    crate::ui::cli::log!("{}", data_table.render());
    crate::ui::cli::log!("{}", "Bundle completed successfully!".green().bold());

    Ok(())
}

async fn scan_bundle_tree<L>(loader: Arc<L>, tree_id: &ID) -> Result<(usize, u64)>
where
    L: BlobLoader + ?Sized + 'static,
{
    let mut total_items = 0;
    let mut total_bytes = 0;
    let mut stack = vec![*tree_id];

    while let Some(current_id) = stack.pop() {
        let data = loader.load_blob(&current_id).await?;
        let tree: Tree = serde_json::from_slice(&data)?;

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
    reporter: Arc<dyn SnapshotProgressReporter>,
) -> Result<()>
where
    L: BlobLoader + ?Sized + 'static,
{
    let (tx, rx) = tokio::sync::mpsc::channel::<(PathBuf, crate::fs::node::Node)>(4096);
    let (dir_tx, dir_rx) = tokio::sync::mpsc::channel::<(PathBuf, Metadata)>(4096);

    let loader_clone = loader.clone();
    let dest_clone = destination.to_path_buf();
    let reporter_clone = reporter.clone();
    let root_id_val = *root_id;

    let walk_task = tokio::spawn(async move {
        let mut stack = vec![(dest_clone, root_id_val)];
        while let Some((current_dest, current_id)) = stack.pop() {
            let data = match loader_clone.load_blob(&current_id).await {
                Ok(d) => d,
                Err(e) => {
                    reporter_clone.error(&format!("Failed to load tree {}: {}", current_id, e));
                    continue;
                }
            };
            let tree: Tree = match serde_json::from_slice(&data) {
                Ok(t) => t,
                Err(e) => {
                    reporter_clone.error(&format!("Failed to parse tree {}: {}", current_id, e));
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
                    reporter_clone.error(&format!("Internal channel error: {}", e));
                    break;
                }
            }
        }
    });

    let meta_reporter: Arc<dyn crate::ui::restore::RestoreProgressReporter> =
        Arc::new(BundleRestoreReporterAdapter {
            inner: reporter.clone(),
        });

    let process_future = async {
        let stream = tokio_stream::wrappers::ReceiverStream::new(rx);
        stream
            .for_each_concurrent(workers, |(path, node)| {
                let loader = loader.clone();
                let reporter = reporter.clone();
                let meta_reporter = meta_reporter.clone();
                async move {
                    reporter.processing_node(
                        &path,
                        crate::fs::tree::NodeDiff::New,
                        Some(node.metadata.size),
                    );

                    if node.is_file() {
                        if let Some(blobs) = &node.blobs {
                            let file_res = std::fs::File::create(&path);
                            if let Ok(mut file) = file_res {
                                let mut success = true;
                                for blob_id in blobs {
                                    match loader.load_blob(blob_id).await {
                                        Ok(data) => {
                                            use std::io::Write;
                                            let data_len = data.len() as u64;
                                            if let Err(e) = file.write_all(&data) {
                                                reporter.error(&format!(
                                                    "Failed to write to {}: {}",
                                                    path.display(),
                                                    e
                                                ));
                                                success = false;
                                                break;
                                            }
                                            reporter.processed_bytes(data_len);
                                        }
                                        Err(e) => {
                                            reporter.error(&format!(
                                                "Failed to load blob {} for {}: {}",
                                                blob_id,
                                                path.display(),
                                                e
                                            ));
                                            success = false;
                                            break;
                                        }
                                    }
                                }
                                drop(file);
                                if success {
                                    crate::restorer::node_restorer::try_restore_node_metadata(
                                        &node.metadata,
                                        false,
                                        &path,
                                        meta_reporter.as_ref(),
                                    );
                                }
                            } else if let Err(e) = file_res {
                                reporter.error(&format!(
                                    "Failed to create file {}: {}",
                                    path.display(),
                                    e
                                ));
                            }
                        }
                    } else if node.is_symlink()
                        && let Some(symlink_info) = &node.symlink_info
                    {
                        #[cfg(unix)]
                        {
                            use std::os::unix::fs::symlink;
                            if symlink(&symlink_info.target_path, &path).is_ok() {
                                crate::restorer::node_restorer::try_restore_node_metadata(
                                    &node.metadata,
                                    true,
                                    &path,
                                    meta_reporter.as_ref(),
                                );
                            }
                        }

                        #[cfg(not(unix))]
                        let _ = symlink_info;
                    }

                    reporter.processed_node(
                        &path,
                        crate::fs::tree::NodeDiff::New,
                        Some(node.metadata.size),
                    );
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
        crate::restorer::node_restorer::try_restore_node_metadata(
            &meta,
            false,
            &p,
            meta_reporter.as_ref(),
        );
    }

    Ok(())
}

async fn spawn_background_scanner(
    paths: Vec<PathBuf>,
    exclude: Vec<PathBuf>,
    reporter: Arc<dyn SnapshotProgressReporter>,
    shutdown: Arc<AtomicBool>,
) {
    let filter = Arc::new(crate::fs::filter::PathFilter::new(None, Some(exclude)));
    let reporter_for_scan = reporter.clone();

    let res = tokio::task::spawn_blocking(move || {
        use rayon::prelude::*;
        paths.into_par_iter().for_each(|path| {
            scan_recursive(&path, &filter, &reporter_for_scan, &shutdown);
        });
    })
    .await;

    if let Err(e) = res {
        reporter.error(&format!("Background scanner panicked: {}", e));
    }

    reporter.scan_finished();
}

fn scan_recursive(
    path: &std::path::Path,
    filter: &Arc<crate::fs::filter::PathFilter>,
    reporter: &Arc<dyn SnapshotProgressReporter>,
    shutdown: &Arc<AtomicBool>,
) {
    if shutdown.load(std::sync::atomic::Ordering::Relaxed) {
        return;
    }
    if !filter.allow(path) {
        return;
    }

    if let Ok(node) = crate::fs::node::Node::from_path_sync(path) {
        reporter.add_expected_items(1);
        if node.is_file() {
            reporter.add_expected_bytes(node.metadata.size);
        }

        if node.is_dir()
            && let Ok(entries) = std::fs::read_dir(path)
        {
            use rayon::prelude::*;
            entries.par_bridge().for_each(|entry_res| {
                if let Ok(entry) = entry_res {
                    scan_recursive(&entry.path(), filter, reporter, shutdown);
                }
            });
        }
    }
}
