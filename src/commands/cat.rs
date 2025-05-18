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

use std::path::PathBuf;

use anyhow::{Context, Result};
use clap::{ArgGroup, Args};

use crate::backend::new_backend_with_prompt;
use crate::cli::{self};
use crate::repository::storage::SecureStorage;
use crate::repository::{self};

use super::GlobalArgs;

#[derive(Args, Debug)]
#[clap(group = ArgGroup::new("decode_mode").multiple(false))]
pub struct CmdArgs {
    /// Path to the repository file
    #[clap(value_parser)]
    path: String,

    /// Decode the object (decrypt + decompress) before printing
    #[arg(long, default_value_t = false, group = "decode_mode")]
    pub decode: bool,

    /// Use this flag if the object is a key file
    #[arg(long, default_value_t = false, group = "decode_mode")]
    pub decompress: bool,
}

pub fn run(global: &GlobalArgs, args: &CmdArgs) -> Result<()> {
    let backend = new_backend_with_prompt(&global.repo)?;
    let repo_password = cli::request_repo_password();

    let key = repository::retrieve_key(repo_password, backend.clone())?;
    let secure_storage = SecureStorage::build()
        .with_key(key)
        .with_compression(zstd::DEFAULT_COMPRESSION_LEVEL);

    let object_path = PathBuf::from(args.path.clone());
    let mut object = backend.read(&object_path)?;

    // Decompress or decode
    if args.decompress {
        object = SecureStorage::decompress(&object).with_context(
            || "Could not decompress the object. Make sure the object is not encrypted.",
        )?;
    } else if args.decode {
        object = secure_storage.decode(&object).with_context(
            || "Could not decode the object. Make sure the object is really encoded.",
        )?;
    }

    println!("{}", String::from_utf8_lossy(&object));

    Ok(())
}
