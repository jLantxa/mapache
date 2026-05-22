use std::path::PathBuf;

use anyhow::Result;
use clap::Args;
use colored::Colorize;
use futures::StreamExt;

use crate::{
    backend::new_backend_with_prompt,
    commands::{GlobalArgs, cleanup::CleanupHandler, with_repository_lock},
    fs::tree::{NodeDiff, NodeDiffStream, SerializedNodeStream},
    mapache::{ContentIdType, ID, defaults::SHORT_SNAPSHOT_ID_LEN},
    repository::snapshot::DiffCounts,
    ui::{
        self,
        cli::table::{Alignment, Table},
    },
    utils::format_size_binary,
};

#[derive(Args, Debug)]
#[clap(about = "Show differences between snapshots")]
pub struct CmdArgs {
    #[arg(value_parser)]
    pub source_snapshot_id: String,

    #[arg(value_parser)]
    pub target_snapshot_id: String,
}

pub async fn run(global_args: &GlobalArgs, args: &CmdArgs) -> Result<()> {
    with_repository_lock(
        global_args.auth_file.as_ref(),
        global_args.key.as_ref(),
        new_backend_with_prompt(global_args.backend_options(false)).await?,
        global_args.to_repo_config(),
        false,
        global_args.retry_lock_duration,
        |repo, _, lock_handle| async move {
            let cleanup_handler = CleanupHandler::new()?;
            cleanup_handler.add_lock(lock_handle.clone());

            repo.reload_master_index().await?;

            let (src_id, _) = repo
                .find(ContentIdType::Snapshot, &args.source_snapshot_id)
                .await?;
            let (tgt_id, _) = repo
                .find(ContentIdType::Snapshot, &args.target_snapshot_id)
                .await?;
            let src_snap = repo.load_snapshot(&src_id, None).await?;
            let tgt_snap = repo.load_snapshot(&tgt_id, None).await?;

            let mut stream = NodeDiffStream::new(
                SerializedNodeStream::new(
                    repo.clone(),
                    Some(src_snap.tree),
                    PathBuf::new(),
                    None,
                    None,
                )
                .await?,
                SerializedNodeStream::new(
                    repo.clone(),
                    Some(tgt_snap.tree),
                    PathBuf::new(),
                    None,
                    None,
                )
                .await?,
            );

            ui::cli::log!(
                "Finding diffs {}..{}\n",
                src_id.to_short_hex(SHORT_SNAPSHOT_ID_LEN).bold().yellow(),
                tgt_id.to_short_hex(SHORT_SNAPSHOT_ID_LEN).bold().green()
            );

            let mut counts = DiffCounts::default();
            while let Some(res) = stream.next().await {
                let (path, source_res, target_res, diff_type) = res?;

                let source = source_res.transpose()?;
                let target = target_res.transpose()?;

                let node = target
                    .as_ref()
                    .or(source.as_ref())
                    .map(|sn| &sn.node)
                    .expect("Node missing in diff result");
                let path_display = if node.is_dir() {
                    path.to_string_lossy().blue().bold()
                } else {
                    path.display().to_string().normal()
                };

                let symbol = match &diff_type {
                    NodeDiff::New => "+".bold().green(),
                    NodeDiff::Deleted => "-".bold().red(),
                    NodeDiff::Changed => {
                        let s_node = &source
                            .as_ref()
                            .expect("Source node missing in Changed diff")
                            .node;
                        let t_node = &target
                            .as_ref()
                            .expect("Target node missing in Changed diff")
                            .node;
                        if s_node.node_type != t_node.node_type {
                            "T".bold().purple()
                        } else if s_node.blobs != t_node.blobs {
                            "M".bold().yellow()
                        } else {
                            "m".bold().cyan()
                        }
                    }
                    NodeDiff::Unchanged => "U".bold().white(),
                };

                if diff_type != NodeDiff::Unchanged {
                    ui::cli::log!("{}  {}", symbol, path_display);
                } else {
                    let s_node = &source
                        .as_ref()
                        .expect("Source node missing in Unchanged diff")
                        .node;
                    let t_node = &target
                        .as_ref()
                        .expect("Target node missing in Unchanged diff")
                        .node;
                    if s_node.blobs != t_node.blobs {
                        ui::cli::log!("{}  {}", "?".bold().white().on_red(), path.display());
                    }
                }

                counts.increment(node.is_dir(), &diff_type);
            }

            ui::cli::log!("\n{}", render_changes_table(&counts));
            ui::cli::log!(
                "{}",
                render_summary_table(&src_id, &tgt_id, src_snap.size(), tgt_snap.size())
            );

            Ok(())
        },
    )
    .await
}

fn render_changes_table(counts: &DiffCounts) -> String {
    let mut table = Table::new_with_alignments(vec![
        Alignment::Left,
        Alignment::Right,
        Alignment::Right,
        Alignment::Right,
        Alignment::Right,
    ]);
    table.set_headers(vec![
        "".to_string(),
        "new".bold().green().to_string(),
        "changed".bold().yellow().to_string(),
        "deleted".bold().red().to_string(),
        "unchanged".to_string(),
    ]);
    table.add_row(vec![
        "Files".bold().to_string(),
        counts.new_files.to_string(),
        counts.changed_files.to_string(),
        counts.deleted_files.to_string(),
        counts.unchanged_files.to_string(),
    ]);
    table.add_row(vec![
        "Dirs".bold().to_string(),
        counts.new_dirs.to_string(),
        counts.changed_dirs.to_string(),
        counts.deleted_dirs.to_string(),
        counts.unchanged_dirs.to_string(),
    ]);
    table.render()
}

fn render_summary_table(
    source_id: &ID,
    target_id: &ID,
    source_size: u64,
    target_size: u64,
) -> String {
    let mut table =
        Table::new_with_alignments(vec![Alignment::Left, Alignment::Right, Alignment::Right]);

    table.set_headers(vec![
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

    table.add_row(vec![
        "Size".to_string(),
        format_size_binary(source_size, 3),
        format_size_binary(target_size, 3),
    ]);

    table.render()
}
