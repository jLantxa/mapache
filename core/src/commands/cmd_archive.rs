use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;

use anyhow::{Context, Result};
use clap::Args;
use colored::Colorize;
use futures::StreamExt;

use crate::{
    archive::writer::ArchiveWriter,
    archiver::{self, SnapshotOptions, progress::SnapshotProgress},
    mapache::traits::BlobSaver,
    ui::snapshot::SnapshotProgressReporter,
};

#[derive(Debug, Args)]
#[clap(about = "Archive files and directories into a single archive file")]
pub struct CmdArgs {
    /// Source path(s) to archive.
    #[arg(required = true)]
    pub source: Vec<PathBuf>,

    /// Output archive file (.mapache).
    #[arg(short, long, required = true)]
    pub output: PathBuf,

    /// Glob patterns for paths to exclude.
    #[arg(short = 'e', long)]
    pub exclude: Vec<PathBuf>,

    /// Compression level [fastest|fast|balanced|better|best|level:val]
    #[clap(long = "compression", value_parser = crate::commands::parse_compression_level, default_value_t = crate::commands::DEFAULT_COMPRESSION)]
    pub compression_level: crate::commands::Compression,

    /// Number of files to process in parallel.
    #[clap(long = "readers", default_value_t = num_cpus::get())]
    pub num_readers: usize,

    #[arg(skip)]
    pub internal_password: Option<String>,
}

pub async fn run(args: &CmdArgs) -> Result<()> {
    let password = match &args.internal_password {
        Some(p) => zeroize::Zeroizing::new(p.clone()),
        None => crate::ui::cli::request_new_password("Enter archive password", "Confirm password")?,
    };

    let archive_writer = Arc::new(ArchiveWriter::new(
        &args.output,
        &password,
        args.compression_level.to_level(),
    )?);
    let shutdown_signal = Arc::new(AtomicBool::new(false));
    let progress = Arc::new(SnapshotProgress::new());

    let absolute_source_paths: Vec<PathBuf> = args
        .source
        .iter()
        .map(|p| p.canonicalize().unwrap_or(p.clone()))
        .collect();

    let snapshot_root_path = if absolute_source_paths.len() == 1 {
        let p = &absolute_source_paths[0];
        if p.is_dir() {
            p.clone()
        } else {
            p.parent().unwrap_or(p).to_path_buf()
        }
    } else {
        crate::fs::calculate_lcp(&absolute_source_paths, false)
    };

    // Scan filesystem for totals to provide accurate progress bars
    crate::ui::cli::log!("{} Scanning filesystem...", "[1/2]".bold().cyan());
    let fs_stream = crate::fs::tree::FSNodeStream::from_paths(
        absolute_source_paths.clone(),
        args.exclude.clone(),
    )
    .await?;

    let mut total_items = 0;
    let mut total_bytes = 0;
    let mut nodes_to_process = Vec::new();

    let mut fs_stream_scan = fs_stream;
    while let Some(node_res) = fs_stream_scan.next().await {
        let (path, stream_node_res) = node_res?;
        let stream_node = stream_node_res?;
        total_items += 1;
        if stream_node.node.is_file() {
            total_bytes += stream_node.node.metadata.size;
        }
        nodes_to_process.push((path, stream_node));
    }

    // Setup reporter with real totals
    let progress_reporter = Arc::new(crate::ui::archive::cli::ArchiveCliProgressReporter::new(
        crate::ui::archive::cli::ArchiveMode::Archive,
        total_items as u64,
        total_bytes,
        args.num_readers,
    ));

    crate::ui::cli::log!(
        "{} Archiving {} to {}...",
        "[2/2]".bold().cyan(),
        args.source
            .iter()
            .map(|p| p.display().to_string())
            .collect::<Vec<_>>()
            .join(", ")
            .bold(),
        args.output.display().to_string().bold()
    );

    let snapshot_options = SnapshotOptions {
        absolute_source_paths,
        snapshot_root_path: snapshot_root_path.clone(),
        exclude_paths: args.exclude.clone(),
        parent_snapshot: None,
        tags: Default::default(),
        description: Some(format!("Archive of {:?}", args.source)),
        no_scan: false,
    };

    archive_process_nodes(
        archive_writer.clone(),
        snapshot_options,
        nodes_to_process,
        args.num_readers,
        progress.clone(),
        progress_reporter.clone(),
        shutdown_signal,
    )
    .await?;

    progress_reporter.finalize();

    // Get final archive size
    let final_size = std::fs::metadata(&args.output)?.len();
    let summary = progress.summary();

    crate::ui::cli::log!("");
    crate::ui::cli::log!("{}", "Archive Summary:".bold().cyan());

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
        "Archive size".to_string(),
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
    crate::ui::cli::log!("{}", "Archive completed successfully!".green().bold());

    Ok(())
}

async fn archive_process_nodes(
    writer: Arc<ArchiveWriter>,
    options: SnapshotOptions<'_>,
    nodes: Vec<(PathBuf, crate::fs::tree::StreamNode)>,
    num_readers: usize,
    progress: Arc<SnapshotProgress>,
    progress_reporter: Arc<dyn SnapshotProgressReporter>,
    shutdown_signal: Arc<AtomicBool>,
) -> Result<()> {
    // Setup channels
    let (processed_tx, mut processed_rx) = tokio::sync::mpsc::channel(4096);

    // Process items in parallel
    let saver: Arc<dyn BlobSaver> = writer.clone();
    let nodes_stream = futures::stream::iter(nodes);

    let progress_for_task = progress.clone();
    let reporter_for_task = progress_reporter.clone();

    let fs_task = tokio::spawn(async move {
        nodes_stream
            .for_each_concurrent(num_readers, |(path, stream_node)| {
                let saver = saver.clone();
                let progress = progress_for_task.clone();
                let reporter = reporter_for_task.clone();
                let signal = shutdown_signal.clone();
                let tx = processed_tx.clone();

                async move {
                    if signal.load(std::sync::atomic::Ordering::Relaxed) {
                        return;
                    }

                    // Only files get the "Active" spinner during processing
                    if !stream_node.node.is_dir() {
                        reporter.processing_node(
                            &path,
                            crate::fs::tree::NodeDiff::New,
                            Some(stream_node.node.metadata.size),
                        );
                    }

                    let mut node = stream_node.node;
                    if node.is_file() {
                        let path_clone = path.clone();
                        let saver_clone = saver.clone();
                        let progress_clone = progress.clone();
                        let reporter_clone = reporter.clone();
                        let signal_clone = signal.clone();
                        let node_clone = node.clone();

                        let blobs_res = tokio::task::spawn_blocking(move || {
                            let file = std::fs::File::open(&path_clone)?;
                            archiver::processor::chunk_and_store_file(
                                saver_clone,
                                file,
                                &node_clone,
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

                    // All elements signal completion to advance main counters
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
        drop(processed_tx);
    });

    // Serialize tree
    let mut tree_serializer = archiver::tree_serializer::TreeSerializer::new(
        writer.clone(),
        options.snapshot_root_path.clone(),
        &options.absolute_source_paths,
    );

    while let Some((path_buf, stream_node)) = processed_rx.recv().await {
        tree_serializer
            .handle_processed_item((&path_buf, stream_node))
            .await?;
    }

    fs_task.await?;

    tree_serializer.finalize_root().await?;
    let root_tree_id = tree_serializer
        .root_tree()
        .context("Root tree ID not set")?;

    // Finalize archive
    writer.finalize(root_tree_id)?;

    Ok(())
}
