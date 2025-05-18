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
    cli,
    repository::{self, RepositoryBackend, SnapshotId, snapshot::Snapshot, storage::SecureStorage},
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
    println!();
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
}

fn log_compact(snapshots: &Vec<(SnapshotId, Snapshot)>) {
    const ABBR_ID_LEN: usize = 12;

    let mut peekable_snapshots = snapshots.iter().peekable();

    println!("{0: <ABBR_ID_LEN$}  {1: <26}", "ID".bold(), "Date".bold());
    print_separator('-', ABBR_ID_LEN + 2 + 26);
    while let Some((id, snapshot)) = peekable_snapshots.next() {
        println!(
            "{0: <ABBR_ID_LEN$}  {1: <26}",
            &id[0..ABBR_ID_LEN].bold().yellow(),
            snapshot.timestamp.format("%Y-%m-%d %H:%M:%S %Z")
        );
    }
}

fn print_separator(character: char, count: usize) {
    let repeated_string: String = std::iter::repeat(character).take(count).collect();
    println!("{}", repeated_string);
}
