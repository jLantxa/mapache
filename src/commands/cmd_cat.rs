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

use std::str::FromStr;

use anyhow::{Context, Result, bail};
use clap::Args;

use crate::{
    backend::{BackendOptions, new_backend_with_prompt},
    commands::{GlobalArgs, cleanup::CleanupHandler},
    fs::tree::Tree,
    mapache::{BlobType, FileType},
    repository::{
        lock::Lock,
        repo::{RepoConfig, Repository},
    },
    ui,
    utils::{self, size},
};

#[derive(Args, Debug)]
#[clap(about = "Print repository objects")]
pub struct CmdArgs {
    /// Object to print:
    /// [manifest|snapshot:ID|pack:ID|blob:ID|tree:ID|index:ID|key:ID|lock:ID].
    /// Blob and tree types don't accept prefixes.
    #[arg(value_parser)]
    pub object: Object,
}

#[derive(Debug, Clone)]
pub enum Object {
    Manifest,
    Pack(String),
    Blob(String),
    Tree(String),
    Index(String),
    Key(String),
    Snapshot(String),
    Lock(String),
}

pub fn run(global_args: &GlobalArgs, args: &CmdArgs) -> Result<()> {
    let auth = utils::get_auth_from_file(&global_args.auth_file)?;
    let backend = new_backend_with_prompt(BackendOptions {
        repo_path: global_args.repo.clone(),
        ssh_pubkey: global_args.ssh_pubkey.clone(),
        ssh_privatekey: global_args.ssh_privatekey.clone(),
        dry_backend: false,
    })?;

    let config = RepoConfig {
        pack_size: (global_args.pack_size_mib * size::MiB as f32) as u64,
    };
    let (repo, _, lock_handle) = Repository::try_open_with_lock(
        auth.as_ref(),
        global_args.key.as_ref(),
        backend,
        config,
        false,
    )?;

    let lock_handle_clone = lock_handle.clone();
    let _cleanup_handler = CleanupHandler::new(move || {
        lock_handle_clone.write().unlock();
    })?;

    match &args.object {
        Object::Manifest => {
            let manifest = repo
                .load_manifest()
                .with_context(|| "Failed to load manifest")?;
            ui::cli::log!("{}", serde_json::to_string_pretty(&manifest)?);
            Ok(())
        }
        Object::Pack(prefix) => {
            let (id, _) = repo.find(FileType::Pack, prefix)?;
            let object = repo
                .load_object(&id)
                .with_context(|| "Failed to load object")?;
            ui::cli::log!("{}", serde_json::to_string_pretty(&object)?);
            Ok(())
        }
        Object::Tree(prefix) => {
            let index = repo.index();
            let index_guard = index.read();
            let id = match index_guard.search_prefix(prefix)? {
                Some(val) => val,
                None => bail!("No tree blobs found with prefix {prefix}"),
            };
            let tree = repo
                .load_blob(id, BlobType::Tree)
                .with_context(|| "Failed to load tree blob")?;
            let tree: Tree = serde_json::from_slice(&tree)?;
            ui::cli::log!("{}", serde_json::to_string_pretty(&tree)?);
            Ok(())
        }
        Object::Blob(prefix) => {
            let index = repo.index();
            let index_guard = index.read();
            let id = match index_guard.search_prefix(prefix)? {
                Some(val) => val,
                None => bail!("No blobs found with prefix {prefix}"),
            };
            let blob = repo
                .load_blob(id, BlobType::Data)
                .with_context(|| "Failed to load blob")?;
            ui::cli::log!("{}", String::from_utf8(blob)?);
            Ok(())
        }
        Object::Index(prefix) => {
            let (id, _) = repo.find(FileType::Index, prefix)?;
            let index = repo
                .load_index(&id)
                .with_context(|| "Failed to load index")?;
            ui::cli::log!("{}", serde_json::to_string_pretty(&index)?);
            Ok(())
        }
        Object::Key(prefix) => {
            let (id, _) = repo.find(FileType::Key, prefix)?;
            let key = repo.load_key(&id).with_context(|| "Failed to load key")?;
            ui::cli::log!("{}", serde_json::to_string_pretty(&key)?);
            Ok(())
        }
        Object::Snapshot(prefix) => {
            let (id, _) = repo.find(FileType::Snapshot, prefix)?;
            let snapshot = repo
                .load_snapshot(&id)
                .with_context(|| "Failed to load snapshot")?;
            ui::cli::log!("{}", serde_json::to_string_pretty(&snapshot)?);
            Ok(())
        }
        Object::Lock(prefix) => {
            let (id, _) = repo.find(FileType::Lock, prefix)?;
            let lock = repo.load_file(FileType::Lock, &id)?;
            let lock: Lock = serde_json::from_slice(&lock)?;
            ui::cli::log!("{}", serde_json::to_string_pretty(&lock)?);
            Ok(())
        }
    }
}

impl FromStr for Object {
    type Err = String; // Or a more specific error type

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let parts: Vec<&str> = s.split(':').collect();
        match parts[0] {
            "manifest" => Ok(Object::Manifest),
            "pack" => {
                if parts.len() == 2 {
                    Ok(Object::Pack(parts[1].to_string()))
                } else {
                    Err("Pack object requires an ID, e.g., 'pack:some_id'".to_string())
                }
            }
            "tree" => {
                if parts.len() == 2 {
                    Ok(Object::Tree(parts[1].to_string()))
                } else {
                    Err("Tree object requires an ID, e.g., 'tree:some_id'".to_string())
                }
            }
            "blob" => {
                if parts.len() == 2 {
                    Ok(Object::Blob(parts[1].to_string()))
                } else {
                    Err("Blob object requires an ID, e.g., 'blob:some_id'".to_string())
                }
            }
            "index" => {
                if parts.len() == 2 {
                    Ok(Object::Index(parts[1].to_string()))
                } else {
                    Err("Index object requires an ID, e.g., 'index:some_id'".to_string())
                }
            }
            "key" => {
                if parts.len() == 2 {
                    Ok(Object::Key(parts[1].to_string()))
                } else {
                    Err("Key object requires an ID, e.g., 'key:some_id'".to_string())
                }
            }
            "snapshot" => {
                if parts.len() == 2 {
                    Ok(Object::Snapshot(parts[1].to_string()))
                } else {
                    Err("Snapshot object requires an ID, e.g., 'snapshot:some_id'".to_string())
                }
            }
            "lock" => {
                if parts.len() == 2 {
                    Ok(Object::Lock(parts[1].to_string()))
                } else {
                    Err("Lock object requires an ID, e.g., 'lock:some_id'".to_string())
                }
            }
            _ => Err(format!("Unknown object type: {}", parts[0])),
        }
    }
}
