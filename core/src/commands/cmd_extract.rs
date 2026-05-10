use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::Result;
use clap::Args;
use colored::Colorize;
use futures::StreamExt;

use crate::{
    archive::reader::ArchiveReader, fs::tree::Tree, mapache::ID, mapache::traits::BlobLoader,
    ui::snapshot::SnapshotProgressReporter,
};

#[derive(Debug, Args)]
#[clap(about = "Extract an archive to a destination directory")]
pub struct CmdArgs {
    /// Archive file (.mapache).
    #[arg(required = true)]
    pub archive: PathBuf,

    /// Destination directory.
    #[arg(short, long, default_value = ".")]
    pub destination: PathBuf,

    /// Number of parallel extraction workers.
    #[clap(long = "workers", default_value_t = num_cpus::get())]
    pub workers: usize,

    #[arg(skip)]
    pub internal_password: Option<String>,
}

pub async fn run(args: &CmdArgs) -> Result<()> {
    let password = match &args.internal_password {
        Some(p) => zeroize::Zeroizing::new(p.clone()),
        None => crate::ui::cli::request_password("Enter archive password")?,
    };

    let reader = ArchiveReader::open(&args.archive, &password)?;
    let root_tree_id = reader.trailer.root_tree;
    let loader = Arc::new(reader);

    // Scan archive tree to calculate totals
    crate::ui::cli::log!("{} Analyzing archive...", "[1/2]".bold().cyan());
    let (total_items, total_bytes) = scan_archive_tree(loader.clone(), &root_tree_id).await?;

    // Setup progress reporter
    let progress_reporter = Arc::new(crate::ui::archive::cli::ArchiveCliProgressReporter::new(
        crate::ui::archive::cli::ArchiveMode::Extract,
        total_items as u64,
        total_bytes,
        args.workers,
    ));

    crate::ui::cli::log!(
        "{} Extracting {} to {}...",
        "[2/2]".bold().cyan(),
        args.archive.display().to_string().bold(),
        args.destination.display().to_string().bold()
    );

    if !args.destination.exists() {
        std::fs::create_dir_all(&args.destination)?;
    }

    // Perform parallel extraction
    extract_nodes_parallel(
        loader.clone(),
        &root_tree_id,
        &args.destination,
        args.workers,
        progress_reporter.clone(),
    )
    .await?;

    progress_reporter.finalize();

    // Final summary
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

async fn scan_archive_tree<L>(loader: Arc<L>, tree_id: &ID) -> Result<(usize, u64)>
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

    let stream = tokio_stream::wrappers::ReceiverStream::new(rx);
    stream
        .for_each_concurrent(workers, |(path, node)| {
            let loader = loader.clone();
            let reporter = reporter.clone();
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
                                        break;
                                    }
                                }
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
                        let _ = symlink(&symlink_info.target_path, &path);
                    }

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

    let _ = walk_task.await;
    Ok(())
}
