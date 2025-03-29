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

use anyhow::{Context, Result, bail};

use super::{config::Config, meta};

pub struct Repository {
    root_path: PathBuf,
    data_path: PathBuf,
    snapshot_path: PathBuf,
    tree_path: PathBuf,

    config: Config,
}

impl Repository {
    fn new(root_path: &Path, config: Config) -> Self {
        Self {
            root_path: root_path.to_owned(),
            data_path: root_path.join("data").to_owned(),
            snapshot_path: root_path.join("snapshot").to_owned(),
            tree_path: root_path.join("tree").to_owned(),

            config: config,
        }
    }

    /// Create and initialize a new repository
    pub fn init(repo_path: &Path) -> Result<Self> {
        if repo_path.exists() {
            bail!(format!(
                "Could not initialize a repository because a directory already exists in \'{}\'",
                repo_path.to_string_lossy()
            ));
        }

        let repo = Self::new(repo_path, Config::default());

        repo.init_structure()
            .with_context(|| "Could not initialize repository structure")?;

        repo.persist_config()?;

        Ok(repo)
    }

    /// Open an existing repository from a directory
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

        let loaded_config = Self::load_config(repo_path)?;

        Ok(Self::new(repo_path, loaded_config))
    }

    pub fn get_config(&self) -> &Config {
        &self.config
    }

    pub fn set_config(&mut self, config: &Config) {
        self.config = config.clone();
    }

    /**
     * Create the repository structure.
     * This includes the data subdirectories, meta, etc.
     */
    fn init_structure(&self) -> Result<()> {
        std::fs::create_dir_all(&self.root_path)
            .with_context(|| "Could not create root directory")?;

        std::fs::create_dir(&self.data_path)?;
        for n in 0x00..=0xff {
            std::fs::create_dir(&self.data_path.join(format!("{:02x}", n)))?;
        }

        std::fs::create_dir(&self.snapshot_path)?;
        std::fs::create_dir(&self.tree_path)?;

        Ok(())
    }

    /// Load config
    fn load_config(repo_path: &Path) -> Result<Config> {
        let config_path = repo_path.join("config");
        let config = meta::load_json(&config_path)?;
        Ok(config)
    }

    fn persist_config(&self) -> Result<()> {
        meta::save_json(&self.config, self.root_path.join("config"))
            .with_context(|| "Could not persist config file")
    }
}
