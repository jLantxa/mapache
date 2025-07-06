// mapache is an incremental backup tool
// Copyright (C) 2025  Javier Lancha Vázquez <javier.lancha@gmail.com>
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program.  If not, see <https://www.gnu.org/licenses/>.

use std::path::PathBuf;
use std::sync::Arc;

use anyhow::Result;
use clap::Args;
use colored::Colorize;

use crate::{
    backend::new_backend_with_prompt,
    commands::GlobalArgs,
    global::{FileType, defaults::SHORT_SNAPSHOT_ID_LEN},
    repository::{
        self, RepositoryBackend,
        streamers::{NodeDiff, NodeDiffStreamer, SerializedNodeStreamer},
    },
    ui::{
        self,
        table::{Alignment, Table},
    },
    utils::{self, format_size},
};

#[derive(Args, Debug)]
pub struct CmdArgs {
    #[arg(value_parser)]
    pub source_snapshot_id: String,

    #[arg(value_parser)]
    pub target_snapshot_id: String,
}

pub fn run(global_args: &GlobalArgs, args: &CmdArgs) -> Result<()> {
    let pass = utils::get_password_from_file(&global_args.password_file)?;
    let backend = new_backend_with_prompt(global_args)?;
    let repo: Arc<dyn RepositoryBackend> =
        repository::try_open(pass, global_args.key.as_ref(), backend)?;

    // Load snapshots
    let (source_id, _) = repo.find(FileType::Snapshot, &args.source_snapshot_id)?;
    let (target_id, _) = repo.find(FileType::Snapshot, &args.target_snapshot_id)?;
    let source_snapshot = repo.load_snapshot(&source_id)?;
    let target_snapshot = repo.load_snapshot(&target_id)?;

    let source_node_streamer = SerializedNodeStreamer::new(
        repo.clone(),
        Some(source_snapshot.tree.clone()),
        PathBuf::new(),
        None,
        None,
    )?;
    let target_node_streamer = SerializedNodeStreamer::new(
        repo.clone(),
        Some(target_snapshot.tree.clone()),
        PathBuf::new(),
        None,
        None,
    )?;
    let diff_streamer = NodeDiffStreamer::new(source_node_streamer, target_node_streamer);

    ui::cli::log!(
        "Calculating diffs {}..{}\n",
        source_id
            .to_short_hex(SHORT_SNAPSHOT_ID_LEN)
            .bold()
            .yellow(),
        target_id.to_short_hex(SHORT_SNAPSHOT_ID_LEN).bold().green()
    );

    let mut new_files = 0;
    let mut deleted_files = 0;
    let mut changed_files = 0;
    let mut new_dirs = 0;
    let mut deleted_dirs = 0;
    let mut changed_dirs = 0;
    let mut unchanged_files = 0;
    let mut unchanged_dirs = 0;
    for (path, source, target, diff_type) in diff_streamer.flatten() {
        match diff_type {
            NodeDiff::New => {
                ui::cli::log!("{} {}", "+".bold().green(), path.display());
                if target
                    .expect("Target node (new) should not be None")
                    .node
                    .is_dir()
                {
                    new_dirs += 1;
                } else {
                    new_files += 1;
                }
            }
            NodeDiff::Deleted => {
                ui::cli::log!("{} {}", "-".bold().red(), path.display());
                if source
                    .expect("Source node (deleted) should not be None")
                    .node
                    .is_dir()
                {
                    deleted_dirs += 1;
                } else {
                    deleted_files += 1;
                }
            }
            NodeDiff::Changed => {
                ui::cli::log!("{} {}", "M".bold().yellow(), path.display());
                if target
                    .expect("Target node (changed) should not be None")
                    .node
                    .is_dir()
                {
                    changed_dirs += 1;
                } else {
                    changed_files += 1;
                }
            }
            NodeDiff::Unchanged => {
                if target
                    .expect("Target node (changed) should not be None")
                    .node
                    .is_dir()
                {
                    unchanged_dirs += 1;
                } else {
                    unchanged_files += 1;
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
        new_files.to_string(),
        changed_files.to_string(),
        deleted_files.to_string(),
        unchanged_files.to_string(),
    ]);
    changes_table.add_row(vec![
        "Dirs".bold().to_string(),
        new_dirs.to_string(),
        changed_dirs.to_string(),
        deleted_dirs.to_string(),
        unchanged_dirs.to_string(),
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
        format_size(source_snapshot.size(), 3),
        format_size(target_snapshot.size(), 3),
    ]);

    ui::cli::log!("{}", summary_table.render());

    Ok(())
}
