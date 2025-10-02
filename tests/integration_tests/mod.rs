// mapache is a secure, de-duplicating, incremental backup tool.
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

use std::{path::PathBuf, sync::Arc};

use anyhow::{Context, Result};

use mapache::{
    backend::localfs::LocalFS,
    repository::repo::{Auth, Repository},
};

mod test_cmd_amend;
mod test_cmd_clean;
mod test_cmd_init;
mod test_cmd_restore;
mod test_cmd_snapshot;
mod test_cmd_sync;

#[cfg(all(feature = "fuse", unix))]
mod test_cmd_mount;

const BACKUP_DATA_PATH: &str = "backup_data.tar.xz";

fn init_repo(auth: &Auth, repo_path: PathBuf) -> Result<()> {
    let backend = Arc::new(LocalFS::new(repo_path));
    Repository::init(Some(auth), None, backend).with_context(|| "Failed to init repo")
}
