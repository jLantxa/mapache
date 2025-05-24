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

use std::str::FromStr;
use std::sync::Arc;

use anyhow::{Context, Result};
use clap::Args;

use crate::backend::new_backend_with_prompt;
use crate::cli::{self};
use crate::global::ID;
use crate::repository::storage::SecureStorage;
use crate::repository::tree::Tree;
use crate::repository::{self};

use super::GlobalArgs;

#[derive(Args, Debug)]
pub struct CmdArgs {
    /// Object to print
    #[arg(value_parser)]
    pub object: Object,
}

#[derive(Debug, Clone)]
pub enum Object {
    Config,
    Pack(String),
    Blob(String),
    Tree(String),
    Index(String),
    Key(String),
    Snapshot(String),
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

    let repo = repository::open(backend, secure_storage.clone())?;

    match &args.object {
        Object::Config => {
            let config = repo
                .load_config()
                .with_context(|| "Failed to load config")?;
            println!("{}", serde_json::to_string_pretty(&config)?);
            Ok(())
        }
        Object::Pack(hex) => {
            let id = &ID::from_hex(hex)?;
            let object = repo
                .load_object(&id)
                .with_context(|| "Failed to load object")?;
            println!("{}", serde_json::to_string_pretty(&object)?);
            Ok(())
        }
        Object::Tree(hex) => {
            let id = &ID::from_hex(hex)?;
            let tree = repo.load_blob(&id).with_context(|| "Failed to load blob")?;
            let tree: Tree = serde_json::from_slice(&tree)?;
            println!("{}", serde_json::to_string_pretty(&tree)?);
            Ok(())
        }
        Object::Blob(hex) => {
            let id = &ID::from_hex(hex)?;
            let blob = repo.load_blob(&id).with_context(|| "Failed to load blob")?;
            println!("{}", String::from_utf8(blob)?);
            Ok(())
        }
        Object::Index(hex) => {
            let id = &ID::from_hex(hex)?;
            let index = repo
                .load_index(&id)
                .with_context(|| "Failed to load index")?;
            println!("{}", serde_json::to_string_pretty(&index)?);
            Ok(())
        }
        Object::Key(hex) => {
            let id = &ID::from_hex(hex)?;
            let key = repo.load_key(&id).with_context(|| "Failed to load key")?;
            println!("{}", serde_json::to_string_pretty(&key)?);
            Ok(())
        }
        Object::Snapshot(hex) => {
            let id = &ID::from_hex(hex)?;
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
            "config" => Ok(Object::Config),
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
