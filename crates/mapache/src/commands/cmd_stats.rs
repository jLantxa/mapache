use std::{
    io,
    path::{Path, PathBuf},
    sync::Arc,
};

use clap::Args;
use futures::StreamExt;
use indicatif::{ProgressBar, ProgressStyle};
use serde::Serialize;

use crate::{
    backend::{StorageBackend, new_backend_with_prompt},
    commands::{GlobalArgs, ToExitCode, cleanup::CleanupHandler, with_repository_lock},
    common::{ContentIdType, error::MapacheError, global::GlobalOpts},
    fs::{node::NodeType, tree::SerializedNodeStream},
    repository::{
        packer::Packer,
        repo::{MANIFEST_PATH, REPO_ECC_EXTENSION, Repository},
        snapshot::SnapshotStream,
        storage::SecureStorage,
    },
    ui::{self, SPINNER_TICK_CHARS, default_bar_draw_target},
    utils::{self, collections::IdSet},
};

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
            StatsError::Io(_) => 1,
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
    indexed_blobs: u64,
    indexed_raw_bytes: u64,
    indexed_encoded_bytes: u64,
}

#[derive(Serialize)]
struct SnapshotsOutput {
    count: usize,
    total_snapshot_bytes: u64,
    referenced_blobs: u64,
    referenced_data_blobs: u64,
    referenced_tree_blobs: u64,
    referenced_raw_bytes: u64,
    referenced_encoded_bytes: u64,
    referenced_raw_bytes_data: u64,
    referenced_encoded_bytes_data: u64,
    referenced_raw_bytes_tree: u64,
    referenced_encoded_bytes_tree: u64,
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

    async fn sum_sizes(
        spinner: &ProgressBar,
        backend: &dyn StorageBackend,
        label: &str,
        files: &[PathBuf],
    ) -> Result<u64, StatsError> {
        let mut total = 0u64;
        let len = files.len();
        for (i, path) in files.iter().enumerate() {
            spinner.set_message(format!("{label} {}/{}", i + 1, len));
            if let Some(sz) = backend.lstat(path).await?.size {
                total = total.saturating_add(sz);
            }
        }
        Ok(total)
    }

    // Packs
    let packs = repo.list_all_files(ContentIdType::Pack).await?;
    let num_packs = packs.len();
    let total_pack_size = sum_sizes(&spinner, backend.as_ref(), "packs", &packs).await?;

    // ECC sidecars
    spinner.set_message("ecc");
    let objects_path = repo.objects_path();
    let ecc_entries = backend.list_dir_recursive(objects_path).await?;
    let mut ecc_files: Vec<PathBuf> = Vec::new();
    for node in ecc_entries {
        let path = node.into_path();
        if path
            .extension()
            .map(|e| e == REPO_ECC_EXTENSION)
            .unwrap_or(false)
        {
            ecc_files.push(path);
        }
    }
    let num_ecc = ecc_files.len();
    let total_ecc_size = sum_sizes(&spinner, backend.as_ref(), "ecc", &ecc_files).await?;

    // Indices
    let indices = repo.list_all_files(ContentIdType::Index).await?;
    let num_indices = indices.len();
    let total_index_size = sum_sizes(&spinner, backend.as_ref(), "index", &indices).await?;

    // Snapshots
    let snaps = repo.list_all_files(ContentIdType::Snapshot).await?;
    let num_snapshots = snaps.len();
    let total_snapshot_size = sum_sizes(&spinner, backend.as_ref(), "snapshots", &snaps).await?;

    // Keys
    let keys = repo.list_all_files(ContentIdType::Key).await?;
    let num_keys = keys.len();
    let total_key_size = sum_sizes(&spinner, backend.as_ref(), "keys", &keys).await?;

    spinner.set_message("manifest");
    let manifest_size = repo
        .backend()
        .lstat(Path::new(MANIFEST_PATH))
        .await?
        .size
        .unwrap_or(0);

    let total_size = total_pack_size
        .saturating_add(total_ecc_size)
        .saturating_add(total_index_size)
        .saturating_add(total_snapshot_size)
        .saturating_add(total_key_size)
        .saturating_add(manifest_size);

    // Index-level summary
    let mut indexed_blobs = 0u64;
    let mut indexed_encoded = 0u64;
    let mut indexed_raw = 0u64;

    spinner.set_message("scanning index");
    let mut count = 0u64;
    repo.index().for_each_id(|_id, loc| {
        indexed_blobs += 1;
        indexed_encoded = indexed_encoded.saturating_add(loc.length as u64);
        indexed_raw = indexed_raw.saturating_add(loc.raw_length as u64);
        count += 1;
        if count.is_multiple_of(10000) {
            spinner.set_message(format!("scanning index: {count}"));
        }
    });

    // Snapshot-derived summary (index-only)
    let snap_stats = analyze_snapshots(repo.clone(), &spinner).await?;

    // If user requested full scan, parse pack footers to compute packed blob totals
    let mut pack_footer_blob_count: Option<usize> = None;
    let mut pack_footer_encoded_bytes: Option<u64> = None;
    let mut pack_footer_raw_bytes: Option<u64> = None;
    let mut pack_footer_dangling: Option<usize> = None;

    if args.full {
        let pack_ids = repo.list_packs().await?;
        let num_pack_ids = pack_ids.len();
        let mut total_descriptors_encoded = 0u64;
        let mut total_descriptors_raw = 0u64;
        let mut total_descriptors = 0usize;
        let mut dangling = 0usize;

        spinner.set_message(format!("parsing pack footers 0/{num_pack_ids}"));

        for (idx, pack_id) in pack_ids.iter().enumerate() {
            let descriptors = Packer::parse_pack_footer(
                repo.as_ref(),
                backend.as_ref(),
                secure_storage.as_ref(),
                pack_id,
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

            spinner.set_message(format!("parsing pack footers {}/{}", idx + 1, num_pack_ids));

            for d in descriptors.iter() {
                total_descriptors += 1;
                total_descriptors_encoded =
                    total_descriptors_encoded.saturating_add(d.length as u64);
                total_descriptors_raw = total_descriptors_raw.saturating_add(d.raw_length as u64);
                if !repo.index().contains(&d.id) {
                    dangling += 1;
                }
            }
        }

        pack_footer_blob_count = Some(total_descriptors);
        pack_footer_encoded_bytes = Some(total_descriptors_encoded);
        pack_footer_raw_bytes = Some(total_descriptors_raw);
        pack_footer_dangling = Some(dangling);
    }

    if !json_out {
        // Human readable output
        ui::cli::log!("Packs:");
        ui::cli::log!("\t{}", utils::format_count(num_packs, "pack", "packs"));
        ui::cli::log!(
            "\tTotal pack size: {}",
            utils::format_size_binary(total_pack_size, 3)
        );
        if num_ecc > 0 {
            ui::cli::log!(
                "\tECC sidecars: {} ({})",
                utils::format_count(num_ecc, "file", "files"),
                utils::format_size_binary(total_ecc_size, 3)
            );
        }
        if args.full {
            ui::cli::log!("\tPack footers parsed: yes");
            if let Some(cnt) = pack_footer_blob_count {
                ui::cli::log!(
                    "\tFooter blobs: {}",
                    utils::format_count(cnt, "blob", "blobs")
                );
            }
            if let Some(enc) = pack_footer_encoded_bytes {
                ui::cli::log!(
                    "\tFooter encoded bytes: {}",
                    utils::format_size_binary(enc, 3)
                );
            }
            if let Some(raw) = pack_footer_raw_bytes {
                ui::cli::log!("\tFooter raw bytes: {}", utils::format_size_binary(raw, 3));
            }
            if let Some(dang) = pack_footer_dangling {
                ui::cli::log!(
                    "\tFooter dangling blobs: {}",
                    utils::format_count(dang, "blob", "blobs")
                );
            }
        }
        ui::cli::log!();
        ui::cli::log!("Index:");
        ui::cli::log!(
            "\t{}",
            utils::format_count(num_indices, "index file", "index files")
        );
        ui::cli::log!(
            "\tTotal index size: {}",
            utils::format_size_binary(total_index_size, 3)
        );
        ui::cli::log!(
            "\tIndexed blobs: {}",
            utils::format_count(indexed_blobs as usize, "blob", "blobs")
        );
        ui::cli::log!(
            "\tIndexed raw size: {}",
            utils::format_size_binary(indexed_raw, 3)
        );
        ui::cli::log!(
            "\tIndexed encoded size: {}",
            utils::format_size_binary(indexed_encoded, 3)
        );
        ui::cli::log!();
        ui::cli::log!("Snapshots:");
        ui::cli::log!(
            "\t{}",
            utils::format_count(num_snapshots, "snapshot", "snapshots")
        );
        ui::cli::log!(
            "\tTotal snapshot size: {}",
            utils::format_size_binary(total_snapshot_size, 3)
        );
        ui::cli::log!();
        ui::cli::log!("Snapshot reference summary:");
        ui::cli::log!(
            "\tReferenced blobs: {} (data: {}, tree: {})",
            utils::format_count(snap_stats.num_referenced_blobs as usize, "blob", "blobs"),
            utils::format_count(
                snap_stats.num_referenced_data_blobs as usize,
                "blob",
                "blobs"
            ),
            utils::format_count(
                snap_stats.num_referenced_tree_blobs as usize,
                "blob",
                "blobs"
            ),
        );
        ui::cli::log!(
            "\tTotal raw referenced size: {}",
            utils::format_size_binary(snap_stats.total_raw_data_size, 3)
        );
        ui::cli::log!(
            "\tTotal encoded referenced size: {}",
            utils::format_size_binary(snap_stats.total_encoded_data_size, 3)
        );
        // Compression ratios = raw / encoded
        let ratio_total = if snap_stats.total_encoded_data_size == 0 {
            0.0
        } else {
            snap_stats.total_raw_data_size as f32 / snap_stats.total_encoded_data_size as f32
        };
        let ratio_data = if snap_stats.total_encoded_data_size_data == 0 {
            0.0
        } else {
            snap_stats.total_raw_data_size_data as f32
                / snap_stats.total_encoded_data_size_data as f32
        };
        let ratio_tree = if snap_stats.total_encoded_data_size_tree == 0 {
            0.0
        } else {
            snap_stats.total_raw_data_size_tree as f32
                / snap_stats.total_encoded_data_size_tree as f32
        };
        ui::cli::log!(
            "\tData (raw / encoded): {} / {}",
            utils::format_size_binary(snap_stats.total_raw_data_size_data, 3),
            utils::format_size_binary(snap_stats.total_encoded_data_size_data, 3)
        );
        ui::cli::log!(
            "\tTree (raw / encoded): {} / {}",
            utils::format_size_binary(snap_stats.total_raw_data_size_tree, 3),
            utils::format_size_binary(snap_stats.total_encoded_data_size_tree, 3)
        );
        ui::cli::log!(
            "\tTotal restorable size: {}",
            utils::format_size_binary(snap_stats.total_restorable_bytes, 3)
        );
        ui::cli::log!(
            "\tCompression ratio (raw/encoded): total: {:.2}x (data: {:.2}x, tree: {:.2}x)",
            ratio_total,
            ratio_data,
            ratio_tree
        );
        ui::cli::log!();
        ui::cli::log!("Keys:");
        ui::cli::log!("\t{}", utils::format_count(num_keys, "key", "keys"));
        ui::cli::log!(
            "\tTotal key size: {}",
            utils::format_size_binary(total_key_size, 3)
        );
        ui::cli::log!();
        ui::cli::log!(
            "Manifest size: {}",
            utils::format_size_binary(manifest_size, 3)
        );
        ui::cli::log!();
        ui::cli::log!(
            "Total repository size: {}",
            utils::format_size_binary(total_size, 3)
        );
    } else {
        // Output JSON if requested
        let out = StatsOutput {
            packs: PacksOutput {
                count: num_packs,
                total_bytes: total_pack_size,
                ecc_count: num_ecc,
                ecc_bytes: total_ecc_size,
                parsed_footer: args.full,
                footer_blob_count: pack_footer_blob_count,
                footer_encoded_bytes: pack_footer_encoded_bytes,
                footer_raw_bytes: pack_footer_raw_bytes,
                footer_dangling_blobs: pack_footer_dangling,
            },
            indices: IndicesOutput {
                count: num_indices,
                total_bytes: total_index_size,
                indexed_blobs,
                indexed_raw_bytes: indexed_raw,
                indexed_encoded_bytes: indexed_encoded,
            },
            snapshots: SnapshotsOutput {
                count: num_snapshots,
                total_snapshot_bytes: total_snapshot_size,
                referenced_blobs: snap_stats.num_referenced_blobs,
                referenced_data_blobs: snap_stats.num_referenced_data_blobs,
                referenced_tree_blobs: snap_stats.num_referenced_tree_blobs,
                referenced_raw_bytes: snap_stats.total_raw_data_size,
                referenced_encoded_bytes: snap_stats.total_encoded_data_size,
                referenced_raw_bytes_data: snap_stats.total_raw_data_size_data,
                referenced_encoded_bytes_data: snap_stats.total_encoded_data_size_data,
                referenced_raw_bytes_tree: snap_stats.total_raw_data_size_tree,
                referenced_encoded_bytes_tree: snap_stats.total_encoded_data_size_tree,
                compression_ratio_total: if snap_stats.total_encoded_data_size == 0 {
                    0.0
                } else {
                    snap_stats.total_raw_data_size as f32
                        / snap_stats.total_encoded_data_size as f32
                },
                compression_ratio_data: if snap_stats.total_encoded_data_size_data == 0 {
                    0.0
                } else {
                    snap_stats.total_raw_data_size_data as f32
                        / snap_stats.total_encoded_data_size_data as f32
                },
                compression_ratio_tree: if snap_stats.total_encoded_data_size_tree == 0 {
                    0.0
                } else {
                    snap_stats.total_raw_data_size_tree as f32
                        / snap_stats.total_encoded_data_size_tree as f32
                },
                total_restorable_bytes: snap_stats.total_restorable_bytes,
            },
            keys: KeysOutput {
                count: num_keys,
                total_bytes: total_key_size,
            },
            manifest_bytes: manifest_size,
            total_repo_bytes: total_size,
        };

        ui::json::emit_static("stats", &out);
        return Ok(());
    }

    Ok(())
}

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

async fn analyze_snapshots(
    repo: Arc<Repository>,
    spinner: &ProgressBar,
) -> Result<SnapshotAnalysis, StatsError> {
    let mut total_raw_data_size = 0u64;
    let mut total_encoded_data_size = 0u64;
    let mut num_referenced_blobs = 0u64; // total (data + tree)
    let mut num_referenced_data_blobs = 0u64;
    let mut num_referenced_tree_blobs = 0u64;
    let mut total_raw_data_size_data = 0u64;
    let mut total_encoded_data_size_data = 0u64;
    let mut total_raw_data_size_tree = 0u64;
    let mut total_encoded_data_size_tree = 0u64;
    let mut total_restorable_bytes = 0u64;
    let mut visited_blobs = IdSet::default();

    let snaps = repo.list_all_files(ContentIdType::Snapshot).await?;
    let num_snapshots = snaps.len();
    let index = repo.index();

    let mut snapshot_stream = SnapshotStream::new(repo.clone()).await?;

    let mut i = 0;
    while let Some(res) = snapshot_stream.next().await {
        let (_id, snapshot) = res?;
        spinner.set_message(format!(
            "Analyzing snapshots: {} / {}",
            i + 1,
            num_snapshots
        ));
        i += 1;
        // Accumulate restorable size (sum of snapshot raw bytes, not deduped)
        total_restorable_bytes = total_restorable_bytes.saturating_add(snapshot.size());

        // Count snapshot root tree blob
        if visited_blobs.insert(snapshot.tree)
            && let Some(locator) = index.get(&snapshot.tree).await
        {
            total_raw_data_size = total_raw_data_size.saturating_add(locator.raw_length as u64);
            total_encoded_data_size = total_encoded_data_size.saturating_add(locator.length as u64);
            total_raw_data_size_tree =
                total_raw_data_size_tree.saturating_add(locator.raw_length as u64);
            total_encoded_data_size_tree =
                total_encoded_data_size_tree.saturating_add(locator.length as u64);
            num_referenced_blobs = num_referenced_blobs.saturating_add(1);
            num_referenced_tree_blobs = num_referenced_tree_blobs.saturating_add(1);
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
            let stream_node = stream_node_res_outer?;
            let node = stream_node.node;

            // Count tree blob if present
            if let Some(tree_id) = &node.tree
                && visited_blobs.insert(*tree_id)
                && let Some(locator) = index.get(tree_id).await
            {
                total_raw_data_size = total_raw_data_size.saturating_add(locator.raw_length as u64);
                total_encoded_data_size =
                    total_encoded_data_size.saturating_add(locator.length as u64);
                total_raw_data_size_tree =
                    total_raw_data_size_tree.saturating_add(locator.raw_length as u64);
                total_encoded_data_size_tree =
                    total_encoded_data_size_tree.saturating_add(locator.length as u64);
                num_referenced_blobs = num_referenced_blobs.saturating_add(1);
                num_referenced_tree_blobs = num_referenced_tree_blobs.saturating_add(1);
            }

            // Count file/data blobs
            if let NodeType::File = node.node_type
                && let Some(blobs) = node.blobs
            {
                for blob_id in blobs {
                    if visited_blobs.insert(blob_id)
                        && let Some(locator) = index.get(&blob_id).await
                    {
                        total_raw_data_size =
                            total_raw_data_size.saturating_add(locator.raw_length as u64);
                        total_encoded_data_size =
                            total_encoded_data_size.saturating_add(locator.length as u64);
                        total_raw_data_size_data =
                            total_raw_data_size_data.saturating_add(locator.raw_length as u64);
                        total_encoded_data_size_data =
                            total_encoded_data_size_data.saturating_add(locator.length as u64);
                        num_referenced_blobs = num_referenced_blobs.saturating_add(1);
                        num_referenced_data_blobs = num_referenced_data_blobs.saturating_add(1);
                    }
                }
            }
        }
    }

    spinner.finish_and_clear();

    Ok(SnapshotAnalysis {
        total_raw_data_size,
        total_encoded_data_size,
        num_referenced_blobs,
        num_referenced_data_blobs,
        num_referenced_tree_blobs,
        total_raw_data_size_data,
        total_encoded_data_size_data,
        total_raw_data_size_tree,
        total_encoded_data_size_tree,
        total_restorable_bytes,
    })
}
