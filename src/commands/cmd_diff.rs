use std::path::PathBuf;

use anyhow::Result;
use clap::Args;
use colored::Colorize;

use crate::{
    backend::{BackendOptions, new_backend_with_prompt},
    commands::{GlobalArgs, cleanup::CleanupHandler},
    fs::tree::{NodeDiff, NodeDiffStreamer, SerializedNodeStreamer},
    mapache::{ContentIdType, defaults::SHORT_SNAPSHOT_ID_LEN},
    repository::{
        repo::{RepoConfig, Repository},
        snapshot::DiffCounts,
    },
    ui::{
        self,
        table::{Alignment, Table},
    },
    utils::{self, format_size_binary, size},
};

#[derive(Args, Debug)]
#[clap(about = "Show differences between snapshots")]
pub struct CmdArgs {
    #[arg(value_parser)]
    pub source_snapshot_id: String,

    #[arg(value_parser)]
    pub target_snapshot_id: String,

    /// A list of paths to include.
    #[clap(long)]
    pub include: Option<Vec<PathBuf>>,

    /// A list of paths to exclude.
    #[clap(long)]
    pub exclude: Option<Vec<PathBuf>>,
}

pub fn run(global_args: &GlobalArgs, args: &CmdArgs) -> Result<()> {
    let auth = utils::get_auth_from_file(&global_args.auth_file)?;
    let backend = new_backend_with_prompt(BackendOptions {
        repo_path: global_args.repo.clone(),
        ssh_pubkey: global_args.ssh_pubkey.clone(),
        ssh_privatekey: global_args.ssh_privatekey.clone(),
        dry_backend: false,
    })?;

    let config = RepoConfig {
        pack_size: (global_args.pack_size_mib * size::MiB as f32) as u64,
        use_cache: !global_args.no_cache,
    };
    let (repo, _, lock_handle) = Repository::try_open_with_lock(
        auth.as_ref(),
        global_args.key.as_ref(),
        backend,
        config,
        false,
    )?;

    let lock_handle_clone = lock_handle.clone();
    let _cleanup_handler = CleanupHandler::new(move || {
        lock_handle_clone.write().unlock();
    })?;

    // Load snapshots
    let (source_id, _) = repo.find(ContentIdType::Snapshot, &args.source_snapshot_id)?;
    let (target_id, _) = repo.find(ContentIdType::Snapshot, &args.target_snapshot_id)?;
    let source_snapshot = repo.load_snapshot(&source_id, None)?;
    let target_snapshot = repo.load_snapshot(&target_id, None)?;

    let source_node_streamer = SerializedNodeStreamer::new(
        repo.clone(),
        Some(source_snapshot.tree),
        PathBuf::new(),
        args.include.clone(),
        args.exclude.clone(),
    )?;
    let target_node_streamer = SerializedNodeStreamer::new(
        repo.clone(),
        Some(target_snapshot.tree),
        PathBuf::new(),
        args.include.clone(),
        args.exclude.clone(),
    )?;
    let diff_streamer = NodeDiffStreamer::new(source_node_streamer, target_node_streamer);

    ui::cli::log!(
        "Finding diffs {}..{}\n",
        source_id
            .to_short_hex(SHORT_SNAPSHOT_ID_LEN)
            .bold()
            .yellow(),
        target_id.to_short_hex(SHORT_SNAPSHOT_ID_LEN).bold().green()
    );

    let mut counts = DiffCounts::default();
    for (path, source, target, diff_type) in diff_streamer.flatten() {
        match &diff_type {
            NodeDiff::New => {
                let target_node = &target
                    .as_ref()
                    .expect("Target node (new) should not be None")
                    .node;

                let new_symbol = "+".bold().green().to_string();

                let path_str = if target_node.is_dir() {
                    format!("{}", path.to_string_lossy().blue().bold())
                } else {
                    path.display().to_string()
                };

                ui::cli::log!("{}  {}", new_symbol, path_str);

                counts.increment(target_node.is_dir(), &diff_type);
            }
            NodeDiff::Deleted => {
                let source_node = &source
                    .as_ref()
                    .expect("Source node (deleted) should not be None")
                    .node;

                let deleted_symbol = "-".bold().red().to_string();

                let path_str = if source_node.is_dir() {
                    format!("{}", path.to_string_lossy().blue().bold())
                } else {
                    path.display().to_string()
                };

                ui::cli::log!("{}  {}", deleted_symbol, path_str);

                counts.increment(source_node.is_dir(), &diff_type);
            }
            NodeDiff::Changed => {
                let target_node = &target
                    .as_ref()
                    .expect("Target node (changed) should not be None")
                    .node;
                let source_node = &source
                    .as_ref()
                    .expect("Source node (changed) should not be None")
                    .node;

                let content_changed = target_node.blobs != source_node.blobs;
                let node_type_changed = source_node.node_type != target_node.node_type;

                let path_str = if source_node.is_dir() {
                    format!("{}", path.to_string_lossy().blue().bold())
                } else {
                    path.display().to_string()
                };

                let symbol = if node_type_changed {
                    "T".bold().purple().to_string()
                } else if !content_changed {
                    "m".bold().cyan().to_string()
                } else {
                    "M".bold().yellow().to_string()
                };

                ui::cli::log!("{}  {}", symbol, path_str);

                counts.increment(target_node.is_dir(), &diff_type);
            }
            NodeDiff::Unchanged => {
                let target_node = &target
                    .as_ref()
                    .expect("Target node (unchanged) should not be None")
                    .node;
                counts.increment(target_node.is_dir(), &diff_type);
                let source_node = &source
                    .as_ref()
                    .expect("Source node (changed) should not be None")
                    .node;

                let content_changed = target_node.blobs != source_node.blobs;
                let path_str = if target_node.is_dir() {
                    format!("{}", path.to_string_lossy().blue().bold())
                } else {
                    path.display().to_string()
                };

                if content_changed {
                    ui::cli::log!(
                        "{}  {}",
                        "?".bold().white().on_red().to_string(),
                        path.display()
                    );
                } else {
                    ui::cli::verbose_1!("{}  {}", "U".bold().to_string(), path_str);
                }
            }
        }
    }

    ui::cli::log!();

    let mut changes_table = Table::new_with_alignments(vec![
        Alignment::Left,
        Alignment::Right,
        Alignment::Right,
        Alignment::Right,
        Alignment::Right,
    ]);
    changes_table.set_headers(vec![
        "".to_string(),
        "new".bold().green().to_string(),
        "changed".bold().yellow().to_string(),
        "deleted".bold().red().to_string(),
        "unchanged".bold().to_string(),
    ]);

    changes_table.add_row(vec![
        "Files".bold().to_string(),
        counts.new_files.to_string(),
        counts.changed_files.to_string(),
        counts.deleted_files.to_string(),
        counts.unchanged_files.to_string(),
    ]);
    changes_table.add_row(vec![
        "Dirs".bold().to_string(),
        counts.new_dirs.to_string(),
        counts.changed_dirs.to_string(),
        counts.deleted_dirs.to_string(),
        counts.unchanged_dirs.to_string(),
    ]);
    ui::cli::log!("{}", changes_table.render());

    let mut summary_table =
        Table::new_with_alignments(vec![Alignment::Left, Alignment::Right, Alignment::Right]);
    summary_table.set_headers(vec![
        String::new(),
        source_id
            .to_short_hex(SHORT_SNAPSHOT_ID_LEN)
            .bold()
            .yellow()
            .to_string(),
        target_id
            .to_short_hex(SHORT_SNAPSHOT_ID_LEN)
            .bold()
            .green()
            .to_string(),
    ]);
    summary_table.add_row(vec![
        "Size".to_string(),
        format_size_binary(source_snapshot.size(), 3),
        format_size_binary(target_snapshot.size(), 3),
    ]);

    ui::cli::log!("{}", summary_table.render());

    Ok(())
}
