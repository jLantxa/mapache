use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;

use anyhow::{Context, Result};
use clap::Args;
use colored::Colorize;
use futures::StreamExt;

use crate::{
    archive::writer::ArchiveWriter,
    archiver::{SnapshotOptions, progress::SnapshotProgress},
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

    // Setup reporter (totals will be filled by background scanner)
    let progress_reporter: Arc<dyn SnapshotProgressReporter> =
        Arc::new(crate::ui::archive::cli::ArchiveCliProgressReporter::new(
            crate::ui::archive::cli::ArchiveMode::Archive,
            0,
            0,
            args.num_readers,
        ));

    // Kick off background scanner for accurate progress estimation
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
        "{} Archiving {} to {}...",
        "[1/1]".bold().cyan(),
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

    // Stream filesystem nodes directly into the processing pipeline
    let fs_stream = crate::fs::tree::FSNodeStream::from_paths(
        snapshot_options.absolute_source_paths.clone(),
        snapshot_options.exclude_paths.clone(),
    )
    .await?;

    let (processed_tx, mut processed_rx) = tokio::sync::mpsc::channel(4096);

    let saver: Arc<dyn BlobSaver> = archive_writer.clone();
    let process_shutdown = shutdown_signal.clone();
    let process_progress = progress.clone();
    let process_reporter = progress_reporter.clone();
    let process_readers = args.num_readers;

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

    // Tree serializer (sequential)
    let mut tree_serializer = crate::archiver::tree_serializer::TreeSerializer::new(
        archive_writer.clone(),
        snapshot_root_path.clone(),
        &snapshot_options.absolute_source_paths,
    );

    while let Some((path_buf, stream_node)) = processed_rx.recv().await {
        tree_serializer
            .handle_processed_item((&path_buf, stream_node))
            .await?;
    }

    process_task.await?;

    // Wait for background scanner to finish
    let _ = scanner_handle.await;

    tree_serializer.finalize_root().await?;
    let root_tree_id = tree_serializer
        .root_tree()
        .context("Root tree ID not set")?;

    writer_finalize(
        archive_writer.as_ref(),
        root_tree_id,
        &args.output,
        &progress,
    )
    .await
}

async fn writer_finalize(
    writer: &ArchiveWriter,
    root_tree_id: crate::mapache::ID,
    output_path: &PathBuf,
    progress: &SnapshotProgress,
) -> Result<()> {
    writer.finalize(root_tree_id)?;

    let final_size = std::fs::metadata(output_path)?.len();
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

/// Fast background scanner that estimates total items and bytes using rayon.
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
