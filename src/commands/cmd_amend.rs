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

use std::sync::Arc;

use anyhow::{Result, bail};
use clap::{ArgGroup, Args};
use colored::Colorize;

use crate::{
    backend::new_backend_with_prompt,
    commands::{GlobalArgs, UseSnapshot, find_use_snapshot},
    global::{FileType, SaveID, defaults::SHORT_SNAPSHOT_ID_LEN},
    repository::{self, RepositoryBackend},
    ui, utils,
};

#[derive(Args, Debug)]
#[clap(group = ArgGroup::new("tags_group").multiple(false))]
#[clap(group = ArgGroup::new("description_group").multiple(false))]
pub struct CmdArgs {
    /// The ID of the snapshot to restore, or 'latest' to restore the most recent snapshot saved.
    #[arg(value_parser = clap::value_parser!(UseSnapshot), default_value_t=UseSnapshot::Latest)]
    pub snapshot: UseSnapshot,

    /// Tags (comma-separated)
    #[clap(long, value_delimiter = ',', value_parser, group = "tags_group")]
    pub tags: Option<Vec<String>>,

    /// Clear tags
    #[clap(long, value_parser, group = "tags_group")]
    pub clear_tags: bool,

    /// Snapshot description
    #[clap(long, value_parser, group = "description_group")]
    pub description: Option<String>,

    /// Clear description
    #[clap(long, value_parser, group = "description_group")]
    pub clear_description: bool,
}

pub fn run(global_args: &GlobalArgs, args: &CmdArgs) -> Result<()> {
    let pass = utils::get_password_from_file(&global_args.password_file)?;
    let backend = new_backend_with_prompt(global_args)?;

    let repo: Arc<dyn RepositoryBackend> =
        repository::try_open(pass, global_args.key.as_ref(), backend)?;

    let (orig_snapshot_id, mut snapshot) = match find_use_snapshot(repo.clone(), &args.snapshot) {
        Ok(Some((id, snap))) => (id, snap),
        Ok(None) | Err(_) => bail!("Snapshot not found"),
    };

    if args.description.is_some() {
        snapshot.description = args.description.clone();
    } else if args.clear_description {
        snapshot.description = None;
    }

    if let Some(tags) = &args.tags {
        snapshot.tags = tags.clone();
    } else if args.clear_tags {
        snapshot.tags = Vec::new();
    }

    // Save the amended snapshot and delete the old snapshot file
    let (new_id, _raw, _encoded) = repo.save_file(
        FileType::Snapshot,
        serde_json::to_string(&snapshot)?.as_bytes(),
        SaveID::CalculateID,
    )?;
    repo.delete_file(FileType::Snapshot, &orig_snapshot_id)?;

    ui::cli::log!(
        "Snapshot {} amended.\nNew snapshot ID {}",
        orig_snapshot_id
            .to_short_hex(SHORT_SNAPSHOT_ID_LEN)
            .bold()
            .yellow(),
        new_id.to_short_hex(SHORT_SNAPSHOT_ID_LEN).bold().green()
    );

    Ok(())
}
