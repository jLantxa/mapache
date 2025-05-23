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

use crate::backend::new_backend_with_prompt;
use crate::backup::SnapshotId;
use crate::repository::storage::SecureStorage;
use crate::{cli, repository};

use super::GlobalArgs;

#[derive(Args, Debug)]
pub struct CmdArgs {
    /// The ID of the snapshot to restore
    #[clap(value_parser)]
    pub snapshot: SnapshotId,
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

    let repo = repository::open(backend, secure_storage)?;

    repo.remove_snapshot(&args.snapshot)?;

    Ok(())
}
