// [backup] is an incremental backup tool
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

use std::sync::Arc;

use anyhow::Result;
use clap::Args;
use colored::Colorize;

use crate::{
    backend::new_backend_with_prompt,
    backup::{self, SnapshotId},
    cli,
    repository::{self, RepositoryBackend, snapshot::Snapshot, storage::SecureStorage},
    ui::table::{Alignment, Table},
    utils,
};

use super::GlobalArgs;

#[derive(Args, Debug)]
pub struct CmdArgs {
    /// Show a compact list of snapshots
    #[arg(short, long)]
    pub compact: bool,
}

pub fn run(global: &GlobalArgs, args: &CmdArgs) -> Result<()> {
    let backend = new_backend_with_prompt(&global.repo)?;
    let repo_password = cli::request_repo_password();

    let key = repository::retrieve_key(repo_password, backend.clone())?;
    let secure_storage = Arc::new(
        SecureStorage::build()
            .with_key(key)
            .with_compression(zstd::DEFAULT_COMPRESSION_LEVEL),
    );

    let repo: Arc<dyn RepositoryBackend> = Arc::from(repository::open(backend, secure_storage)?);

    let snapshots = repo.load_all_snapshots_sorted()?;

    println!();
    if args.compact {
        log_compact(&snapshots);
    } else {
        log(&snapshots);
    }

    println!("{} snapshots", snapshots.len());

    Ok(())
}

fn log(snapshots: &Vec<(SnapshotId, Snapshot)>) {
    let mut peekable_snapshots = snapshots.iter().peekable();

    while let Some((id, snapshot)) = peekable_snapshots.next() {
        println!("{}", id.bold().yellow());
        println!(
            "{} {}",
            "Date:".bold(),
            snapshot.timestamp.format("%Y-%m-%d %H:%M:%S %Z")
        );
        println!("{} {}", "Size:".bold(), utils::format_size(snapshot.size));
        println!("{} {}", "Root:".bold(), &snapshot.root.display());

        println!();
        println!("{}", "Paths:".bold());
        for path in &snapshot.paths {
            println!("{}", path.display());
        }

        if let Some(description) = &snapshot.description {
            println!();
            println!("{}", "Description:".bold().cyan());
            println!("{}", description);
        }

        if peekable_snapshots.peek().is_some() {
            println!();
        }
    }

    println!();
}

fn log_compact(snapshots: &Vec<(SnapshotId, Snapshot)>) {
    let mut peekable_snapshots = snapshots.iter().peekable();

    let id_len = backup::defaults::SHORT_ID_LENGTH;

    let mut table =
        Table::new_with_alignments(vec![Alignment::Left, Alignment::Center, Alignment::Right]);

    table.set_headers(vec![
        "ID".bold().to_string(),
        "Date".bold().to_string(),
        "Size".bold().to_string(),
    ]);

    while let Some((id, snapshot)) = peekable_snapshots.next() {
        table.add_row(vec![
            id[0..id_len].bold().yellow().to_string(),
            snapshot
                .timestamp
                .format("%Y-%m-%d %H:%M:%S %Z")
                .to_string(),
            utils::format_size(snapshot.size),
        ]);
    }

    table.print();
}
