use anyhow::Result;
use clap::Args;
use futures::StreamExt;
use serde::Serialize;

use crate::{
    backend::new_backend_with_prompt,
    commands::{GlobalArgs, cleanup::CleanupHandler, with_repository_lock},
    fs::tree::{NodeDiff, create_diff_stream},
    mapache::{ContentIdType, ID, defaults::SHORT_SNAPSHOT_ID_LEN},
    repository::snapshot::DiffCounts,
    ui::{
        self,
        cli::{
            color::Colorize,
            table::{Alignment, Table},
        },
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

            let mut stream = create_diff_stream(repo.clone(), src_snap.tree, tgt_snap.tree).await?;

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

                let node = match target.as_ref().or(source.as_ref()).map(|sn| &sn.node) {
                    Some(n) => n,
                    None => {
                        tracing::warn!(target: "diff", "Both source and target missing for {}", path.display());
                        continue;
                    }
                };
                let path_display = if node.is_dir() {
                    path.to_string_lossy().blue().bold()
                } else {
                    path.display().to_string().normal()
                };

                let symbol = match &diff_type {
                    NodeDiff::New => "+".bold().green(),
                    NodeDiff::Deleted => "-".bold().red(),
                    NodeDiff::Changed => {
                        let (s_node, t_node) = match (source.as_ref(), target.as_ref()) {
                            (Some(s), Some(t)) => (&s.node, &t.node),
                            _ => {
                                tracing::warn!(target: "diff", "Source or target node missing in Changed diff for {}", path.display());
                                continue;
                            }
                        };
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
                } else if let (Some(s_node), Some(t_node)) =
                    (source.as_ref().map(|sn| &sn.node), target.as_ref().map(|sn| &sn.node))
                    && s_node.blobs != t_node.blobs
                {
                    ui::cli::log!("{}  {}", "?".bold().white().on_red(), path.display());
                }

                if global_args.json {
                    #[derive(Serialize)]
                    struct DiffEntryMsg<'a> {
                        path: String,
                        diff_type: &'a str,
                        is_dir: bool,
                    }
                    ui::json::emit_static("diff", &DiffEntryMsg {
                        path: path.to_string_lossy().to_string(),
                        diff_type: match &diff_type {
                            NodeDiff::New => "new",
                            NodeDiff::Deleted => "deleted",
                            NodeDiff::Changed => "changed",
                            NodeDiff::Unchanged => "unchanged",
                        },
                        is_dir: node.is_dir(),
                    });
                }

                counts.increment(node.is_dir(), &diff_type);
            }

            ui::cli::log!("\n{}", render_changes_table(&counts));
            ui::cli::log!(
                "{}",
                render_summary_table(&src_id, &tgt_id, src_snap.size(), tgt_snap.size())
            );

            if global_args.json {
                #[derive(Serialize)]
                struct DiffSummaryMsg {
                    new_files: u64,
                    changed_files: u64,
                    deleted_files: u64,
                    unchanged_files: u64,
                    new_dirs: u64,
                    changed_dirs: u64,
                    deleted_dirs: u64,
                    unchanged_dirs: u64,
                }
                ui::json::emit_static("diff_summary", &DiffSummaryMsg {
                    new_files: counts.new_files,
                    changed_files: counts.changed_files,
                    deleted_files: counts.deleted_files,
                    unchanged_files: counts.unchanged_files,
                    new_dirs: counts.new_dirs,
                    changed_dirs: counts.changed_dirs,
                    deleted_dirs: counts.deleted_dirs,
                    unchanged_dirs: counts.unchanged_dirs,
                });
            }

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
