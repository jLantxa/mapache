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

use std::str::FromStr;
use std::sync::Arc;

use anyhow::{Context, Result, bail};
use clap::Args;

use crate::backend::new_backend_with_prompt;
use crate::global::{ID, ID_LENGTH};
use crate::repository::tree::Tree;
use crate::repository::{self, RepositoryBackend};
use crate::utils;

use super::GlobalArgs;

#[derive(Args, Debug)]
#[clap(about = "Print repository objects")]
pub struct CmdArgs {
    /// Object to print:
    /// [manifest|snapshot:ID|pack:ID|blob:ID|tree:ID|index:ID|key:ID].
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
}

pub fn run(global_args: &GlobalArgs, args: &CmdArgs) -> Result<()> {
    let pass = utils::get_password_from_file(&global_args.password_file)?;
    let backend = new_backend_with_prompt(global_args)?;
    let repo: Arc<dyn RepositoryBackend> =
        repository::try_open(pass, global_args.key.as_ref(), backend)?;

    match &args.object {
        Object::Manifest => {
            let manifest = repo
                .load_manifest()
                .with_context(|| "Failed to load manifest")?;
            println!("{}", serde_json::to_string_pretty(&manifest)?);
            Ok(())
        }
        Object::Pack(prefix) => {
            let (id, _) = repo.find(crate::global::FileType::Object, prefix)?;
            let object = repo
                .load_object(&id)
                .with_context(|| "Failed to load object")?;
            println!("{}", serde_json::to_string_pretty(&object)?);
            Ok(())
        }
        Object::Tree(prefix) => {
            if prefix.len() != 2 * ID_LENGTH {
                bail!("Tree search not supported with prefix. Use the whole ID string.");
            }
            let id = ID::from_hex(prefix)?;
            let tree = repo.load_blob(&id).with_context(|| "Failed to load blob")?;
            let tree: Tree = serde_json::from_slice(&tree)?;
            println!("{}", serde_json::to_string_pretty(&tree)?);
            Ok(())
        }
        Object::Blob(prefix) => {
            if prefix.len() != 2 * ID_LENGTH {
                bail!("Blob search not supported with prefix. Use the whole ID string.");
            }

            let id = ID::from_hex(prefix)?;
            let blob = repo.load_blob(&id).with_context(|| "Failed to load blob")?;
            println!("{}", String::from_utf8(blob)?);
            Ok(())
        }
        Object::Index(prefix) => {
            let (id, _) = repo.find(crate::global::FileType::Index, prefix)?;
            let index = repo
                .load_index(&id)
                .with_context(|| "Failed to load index")?;
            println!("{}", serde_json::to_string_pretty(&index)?);
            Ok(())
        }
        Object::Key(prefix) => {
            let (id, _) = repo.find(crate::global::FileType::Key, prefix)?;
            let key = repo.load_key(&id).with_context(|| "Failed to load key")?;
            println!("{}", serde_json::to_string_pretty(&key)?);
            Ok(())
        }
        Object::Snapshot(prefix) => {
            let (id, _) = repo.find(crate::global::FileType::Snapshot, prefix)?;
            let snapshot = repo
                .load_snapshot(&id)
                .with_context(|| "Failed to load snapshot")?;
            println!("{}", serde_json::to_string_pretty(&snapshot)?);
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
            _ => Err(format!("Unknown object type: {}", parts[0])),
        }
    }
}
