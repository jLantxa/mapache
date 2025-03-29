/*
 * [backup] is an incremental backup tool
 * Copyright (C) 2025  Javier Lancha Vázquez <javier.lancha@gmail.com>
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
*/

use std::path::{Path, PathBuf};

use anyhow::{bail, Context, Result};
use colored::Colorize;

pub struct Repository {
    path: PathBuf,
}

impl Repository {
    pub fn init(repo_path: &Path) -> Result<Self> {
        if repo_path.exists() {
            bail!(format!(
                "Could not initialize a repository because a directory already exists in \'{}\'",
                repo_path.to_string_lossy()
            ));
        }

        Self::init_repo_structure(repo_path)?;

        Ok(Self {
            path: repo_path.to_owned(),
        })
    }

    pub fn open(repo_path: &Path) -> Result<Self> {
        if !repo_path.exists() {
            bail!(
                "Could not open a repository. \'{}\' doesn't exist",
                repo_path.to_string_lossy()
            );
        } else if !repo_path.is_dir() {
            bail!(
                "Could not open a repository. \'{}\' is not a directory",
                repo_path.to_string_lossy()
            );
        }

        Ok(Self {
            path: repo_path.to_owned(),
        })
    }

    fn init_repo_structure(repo_path: &Path) -> Result<()> {
        std::fs::create_dir_all(repo_path).with_context(|| "Could not create root directory");

        // Data
        let data_path = repo_path.join("data");
        std::fs::create_dir(&data_path)?;
        for n in 0x00..=0xff {
            std::fs::create_dir(&data_path.join(format!("{:02x}", n)))?;
        }

        // Meta
        let meta_path = repo_path.join("meta");
        std::fs::create_dir(meta_path)?;

        Ok(())
    }
}
