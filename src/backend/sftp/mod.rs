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

pub mod sftp_pool;

use std::{
    io::{Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
    sync::Arc,
};

use anyhow::{Context, Result, bail};
use sftp_pool::{SftpConnectionPool, SftpSessionClient};
use ssh2::RenameFlags;

use super::StorageBackend;

pub struct SftpBackend {
    repo_path: PathBuf,
    pool: SftpConnectionPool,
}

impl SftpBackend {
    pub fn new(
        repo_path: PathBuf,
        username: String,
        host: String,
        port: u16,
        password: String,
    ) -> Result<Self> {
        let addr = format!("{}:{}", host, port);

        const MAX_SFTP_CONNECTIONS: usize = 5;
        const CONNECTION_TIMEOUT_SECONDS: u64 = 5;
        const CONNECTION_IDLE_SECONDS: u64 = 10;

        let pool = SftpConnectionPool::new(
            addr,
            username,
            password,
            MAX_SFTP_CONNECTIONS,
            CONNECTION_TIMEOUT_SECONDS,
            CONNECTION_IDLE_SECONDS,
        );

        Ok(Self { repo_path, pool })
    }

    #[inline]
    fn full_path(&self, path: &Path) -> PathBuf {
        self.repo_path.join(path)
    }

    fn exists_exact(&self, path: &Path, sftp_client: &Arc<SftpSessionClient>) -> bool {
        match sftp_client.sftp().lstat(&path) {
            Ok(_) => true,
            Err(_) => false,
        }
    }

    fn create_dir_exact(&self, path: &Path, sftp_client: &Arc<SftpSessionClient>) -> Result<()> {
        let sftp = sftp_client.sftp();

        let stats = sftp.lstat(path);
        if let Ok(stats) = stats {
            if !stats.is_dir() {
                bail!(format!(
                    "Failed to create directory {:?}\' in sftp backend. Path exists, but it is not a directory.",
                    path
                ))
            } else {
                Ok(())
            }
        } else {
            sftp.mkdir(path, 0o755)
                .with_context(|| format!("Failed to create directory {:?}\' in sftp backend", path))
        }
    }

    fn create_dir_all_exact(&self, path: &Path) -> Result<()> {
        let sftp_client = Arc::new(self.pool.get()?);
        let sftp = sftp_client.sftp();

        if self.exists_exact(path, &sftp_client) {
            let metadata = sftp
                .stat(path)
                .with_context(|| format!("Failed to get metadata for path: {:?}", path))?;
            if metadata.is_dir() {
                return Ok(());
            } else {
                return Err(anyhow::anyhow!(
                    "Path {:?} exists but is not a directory",
                    path
                ));
            }
        }

        if let Some(parent) = path.parent() {
            if parent != Path::new("") {
                self.create_dir_all_exact(parent)?;
            }
        }

        sftp.mkdir(path, 0o755)
            .with_context(|| format!("Failed to create directory {:?}\' in sftp backend", path))
    }
}

impl StorageBackend for SftpBackend {
    fn create(&self) -> Result<()> {
        self.create_dir_all_exact(&self.repo_path)
    }

    fn root_exists(&self) -> bool {
        match self.pool.get() {
            Ok(sftp_client) => self.exists_exact(&self.repo_path, &Arc::new(sftp_client)),
            Err(_) => false,
        }
    }

    fn read(&self, path: &Path) -> Result<Vec<u8>> {
        let sftp_client = self.pool.get()?;
        let sftp = sftp_client.sftp();

        let full_path = self.full_path(path);
        let mut file = sftp.open(full_path).with_context(|| {
            format!(
                "Failed to open file {:?}\' in sftp backend for reading",
                path
            )
        })?;
        let mut contents = Vec::new();
        file.read_to_end(&mut contents)
            .with_context(|| format!("Failed to read file {:?}\' in sftp backend", path))?;
        Ok(contents)
    }

    fn read_seek(&self, path: &Path, offset: u64, length: u64) -> Result<Vec<u8>> {
        let sftp_client = self.pool.get()?;
        let sftp = sftp_client.sftp();

        let full_path = self.full_path(path);
        let mut file = sftp.open(full_path).with_context(|| {
            format!(
                "Failed to open file {:?}\' in sftp backend for ranged reading",
                path
            )
        })?;
        let _ = file.seek(SeekFrom::Start(offset));
        let mut contents = Vec::with_capacity(length as usize);
        file.read_exact(&mut contents)
            .with_context(|| format!("Failed to range read file {:?}\' in sftp backend", path))?;
        Ok(contents)
    }

    fn write(&self, path: &Path, contents: &[u8]) -> Result<()> {
        let sftp_client = self.pool.get()?;
        let sftp = sftp_client.sftp();

        let full_path = self.full_path(path);
        let mut file = sftp
            .create(&full_path)
            .context(format!("Failed to create file for writing: {:?}", path))?;
        file.write_all(contents)
            .context(format!("Failed to write to file: {:?}", path))?;
        Ok(())
    }

    fn rename(&self, from: &Path, to: &Path) -> Result<()> {
        let sftp_client = self.pool.get()?;
        let sftp = sftp_client.sftp();

        let full_path_from = self.full_path(from);
        let full_path_from_to = self.full_path(to);
        sftp.rename(
            &full_path_from,
            &full_path_from_to,
            Some(RenameFlags::all()),
        )
        .with_context(|| {
            format!(
                "Failed to rename {:?}\' to {:?}\' in sftp backend",
                from, to
            )
        })
    }

    fn remove_file(&self, file_path: &Path) -> Result<()> {
        let sftp_client = self.pool.get()?;
        let sftp = sftp_client.sftp();

        let full_path = self.full_path(file_path);
        sftp.unlink(&full_path)
            .with_context(|| format!("Failed to remove file {:?}\' in sftp backend", file_path))
    }

    #[inline]
    fn create_dir(&self, path: &Path) -> Result<()> {
        let sftp_client = Arc::new(self.pool.get()?);

        let full_path = self.full_path(path);
        self.create_dir_exact(&full_path, &sftp_client)
    }

    #[inline]
    fn create_dir_all(&self, path: &Path) -> Result<()> {
        let full_path = self.full_path(path);
        self.create_dir_all_exact(&full_path)
    }

    fn read_dir(&self, path: &Path) -> Result<Vec<PathBuf>> {
        let sftp_client = self.pool.get()?;
        let sftp = sftp_client.sftp();

        let full_path = self.full_path(path);

        let entries = sftp
            .readdir(full_path)
            .with_context(|| format!("Could not list directory {:?}\' in sftp backend", path))?;

        Ok(entries
            .iter()
            .map(|(path, _meta)| path.strip_prefix(&self.repo_path).unwrap().to_path_buf())
            .collect())
    }

    fn remove_dir(&self, path: &Path) -> Result<()> {
        let sftp_client = self.pool.get()?;
        let sftp = sftp_client.sftp();

        let full_path = self.full_path(path);
        sftp.rmdir(&full_path)
            .with_context(|| format!("Failed to remove dir {:?}\' in sftp backend", path))
    }

    fn remove_dir_all(&self, _path: &Path) -> Result<()> {
        todo!()
    }

    fn exists(&self, path: &Path) -> bool {
        match self.pool.get() {
            Ok(sftp_client) => {
                let full_path = self.full_path(path);
                self.exists_exact(&full_path, &Arc::new(sftp_client))
            }
            Err(_) => false,
        }
    }

    fn is_file(&self, path: &Path) -> bool {
        match self.pool.get() {
            Ok(sftp_client) => {
                let full_path = self.full_path(path);
                let sftp = sftp_client.sftp();
                let stat_res = sftp.lstat(&full_path);

                match stat_res {
                    Ok(stat) => stat.is_file(),
                    Err(_) => false,
                }
            }
            Err(_) => false,
        }
    }

    fn is_dir(&self, path: &Path) -> bool {
        match self.pool.get() {
            Ok(sftp_client) => {
                let full_path = self.full_path(path);
                let sftp = sftp_client.sftp();
                let stat_res = sftp.lstat(&full_path);

                match stat_res {
                    Ok(stat) => stat.is_dir(),
                    Err(_) => false,
                }
            }
            Err(_) => false,
        }
    }
}
