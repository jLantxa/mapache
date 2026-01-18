use std::{
    path::{Path, PathBuf},
    sync::Arc,
};

use anyhow::Result;
use clap::{Args, ValueEnum};
use indicatif::{ProgressBar, ProgressStyle};

use crate::{
    backend::{StorageBackend, new_backend_with_prompt},
    commands::{GlobalArgs, cleanup::CleanupHandler},
    fs::{node::NodeType, tree::SerializedNodeStream},
    mapache::{ContentIdType, global::GlobalOpts},
    repository::{
        repo::{MANIFEST_PATH, RepoConfig, Repository},
        snapshot::SnapshotStream,
    },
    ui::{self, SPINNER_TICK_CHARS, default_bar_draw_target},
    utils::{self, collections::IdSet, size},
};

#[derive(Debug, Clone, ValueEnum)]
pub enum Mode {
    Repository,
    Snapshots,
}

impl std::fmt::Display for Mode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Mode::Repository => write!(f, "repository"),
            Mode::Snapshots => write!(f, "snapshots"),
        }
    }
}

#[derive(Args, Debug)]
#[clap(about = "Display stats about the repository and its contents")]
pub struct CmdArgs {
    #[clap(long = "mode", value_parser, default_value_t = Mode::Repository)]
    pub mode: Mode,
}

pub fn run(global_args: &GlobalArgs, args: &CmdArgs) -> Result<()> {
    let auth = utils::get_auth_from_file(&global_args.auth_file)?;
    let backend = new_backend_with_prompt(global_args.backend_options(false))?;

    let config = RepoConfig {
        pack_size: (global_args.pack_size_mib * size::MiB as f32) as u64,
        use_cache: !global_args.no_cache,
        compression: global_args.compression_level,
    };
    let (repo, _, lock_handle) = Repository::try_open_with_lock(
        auth.as_ref(),
        global_args.key.as_ref(),
        backend.clone(),
        config,
        false,
        global_args.retry_lock_duration,
    )?;

    let lock_handle_clone = lock_handle.clone();
    let _cleanup_handler = CleanupHandler::new(move || {
        lock_handle_clone.write().unlock();
    })?;

    match args.mode {
        Mode::Repository => stats_repository(repo, backend),
        Mode::Snapshots => stats_snapshots(repo),
    }
}

fn stats_repository(repo: Arc<Repository>, backend: Arc<dyn StorageBackend>) -> Result<()> {
    let spinner = ProgressBar::new_spinner();
    spinner.set_draw_target(default_bar_draw_target());
    spinner.set_style(
        ProgressStyle::default_spinner()
            .template("{spinner:.cyan} Collecting stats... ({msg})")
            .unwrap()
            .tick_chars(SPINNER_TICK_CHARS),
    );
    spinner.enable_steady_tick(GlobalOpts::progress_refresh_interval());

    fn sum_sizes(
        spinner: &ProgressBar,
        backend: &dyn StorageBackend,
        label: &str,
        files: &[PathBuf],
    ) -> Result<u64> {
        let mut total = 0u64;
        let len = files.len();
        for (i, path) in files.iter().enumerate() {
            spinner.set_message(format!("{label} {}/{}", i + 1, len));
            if let Some(sz) = backend.lstat(path)?.size {
                total = total.saturating_add(sz);
            }
        }
        Ok(total)
    }

    // Packs
    let packs = repo.list_all_files(ContentIdType::Pack)?;
    let num_packs = packs.len();
    let total_pack_size = sum_sizes(&spinner, backend.as_ref(), "packs", &packs)?;

    // Indices
    let indices = repo.list_all_files(ContentIdType::Index)?;
    let num_indices = indices.len();
    let total_index_size = sum_sizes(&spinner, backend.as_ref(), "index", &indices)?;

    // Snapshots
    let snaps = repo.list_all_files(ContentIdType::Snapshot)?;
    let num_snapshots = snaps.len();
    let total_snapshot_size = sum_sizes(&spinner, backend.as_ref(), "snapshots", &snaps)?;

    // Keys
    let keys = repo.list_all_files(ContentIdType::Key)?;
    let num_keys = keys.len();
    let total_key_size = sum_sizes(&spinner, backend.as_ref(), "keys", &keys)?;

    // Manifest
    spinner.set_message("manifest");
    let manifest_size = repo
        .backend()
        .lstat(Path::new(MANIFEST_PATH))?
        .size
        .unwrap_or(0);

    let total_size = total_pack_size
        .saturating_add(total_index_size)
        .saturating_add(total_snapshot_size)
        .saturating_add(total_key_size)
        .saturating_add(manifest_size);

    spinner.finish_and_clear();

    ui::cli::log!("Packs:");
    ui::cli::log!("\t{}", utils::format_count(num_packs, "pack", "packs"));
    ui::cli::log!(
        "\tTotal pack size: {}",
        utils::format_size_binary(total_pack_size, 3)
    );
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

    Ok(())
}

fn stats_snapshots(repo: Arc<Repository>) -> Result<()> {
    let snapshot_stream = SnapshotStream::new(repo.clone())?;
    let num_snapshots = snapshot_stream.len();

    let mut error_counter = 0usize;
    let mut total_restore_size = 0u64;
    let mut total_raw_data_size = 0u64;
    let mut total_encoded_data_size = 0u64;
    let mut num_referenced_blobs = 0u64;
    let mut visited_blobs = IdSet::default();

    let spinner = ProgressBar::new_spinner();
    spinner.set_draw_target(default_bar_draw_target());
    spinner.set_style(
        ProgressStyle::default_spinner()
            .template("{spinner:.cyan} {msg}")
            .unwrap()
            .tick_chars(SPINNER_TICK_CHARS),
    );
    spinner.enable_steady_tick(GlobalOpts::progress_refresh_interval());

    // Hold index read-lock once
    let index = repo.index();

    for (i, (_id, snapshot)) in snapshot_stream.enumerate() {
        spinner.set_message(format!(
            "Analyzing snapshots: {} / {}",
            i + 1,
            num_snapshots
        ));
        total_restore_size = total_restore_size.saturating_add(snapshot.size());

        let stream = SerializedNodeStream::new(
            repo.clone(),
            Some(snapshot.tree),
            PathBuf::new(),
            None,
            None,
        )?;

        for (_path, stream_node) in stream.flatten() {
            let node = stream_node.node;
            if let NodeType::File = node.node_type
                && let Some(blobs) = node.blobs
            {
                for blob_id in blobs {
                    // Single op membership check
                    if visited_blobs.insert(blob_id) {
                        if let Some(locator) = index.get(&blob_id) {
                            total_raw_data_size =
                                total_raw_data_size.saturating_add(locator.raw_length as u64);
                            total_encoded_data_size =
                                total_encoded_data_size.saturating_add(locator.length as u64);
                            num_referenced_blobs = num_referenced_blobs.saturating_add(1);
                        } else {
                            error_counter = error_counter.saturating_add(1);
                        }
                    }
                }
            }
        }
    }

    spinner.finish_and_clear();

    ui::cli::log!(
        "{}",
        utils::format_count(num_snapshots, "snapshot", "snapshots")
    );
    ui::cli::log!(
        "\t{}",
        utils::format_count(
            num_referenced_blobs as usize,
            "referenced blob",
            "referenced blobs"
        )
    );
    ui::cli::log!(
        "\tRestore size:       {:>12}",
        utils::format_size_binary(total_restore_size, 3)
    );
    ui::cli::log!(
        "\tTotal raw size:     {:>12}",
        utils::format_size_binary(total_raw_data_size, 3)
    );
    ui::cli::log!(
        "\tTotal encoded size: {:>12}",
        utils::format_size_binary(total_encoded_data_size, 3)
    );

    let ratio = if total_encoded_data_size == 0 {
        0.0
    } else {
        total_raw_data_size as f32 / total_encoded_data_size as f32
    };
    ui::cli::log!("\tCompression ratio: {:.2}x", ratio);

    if error_counter > 0 {
        ui::cli::log!();
        ui::cli::warning!("Found {} blobs not indexed", error_counter);
    }

    Ok(())
}
