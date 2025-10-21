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

use std::path::PathBuf;

use anyhow::{Context, Result};
use clap::{Args, Subcommand};
use colored::Colorize;

use crate::{
    backend::{BackendOptions, new_backend_with_prompt},
    commands::GlobalArgs,
    mapache::ID,
    repository::{
        keys::{KeyFileStreamer, KeyManager},
        repo::{Auth, KEYS_DIR},
        storage::SecureStorage,
    },
    ui::{
        self,
        cli::{request_auth, request_new_auth},
        table::Table,
    },
    utils::{self},
};

#[derive(Args, Debug, Clone)]
pub struct AddArgs {
    /// Optional path to save the new Keyfile
    #[clap(long = "path", value_parser)]
    output_keyfile_path: Option<PathBuf>,
}

#[derive(Args, Debug, Clone)]
pub struct DeleteArgs {
    #[clap(value_parser)]
    id: String,
}

#[derive(Args, Debug, Clone)]
pub struct PasswordChangeArgs {}

#[derive(Subcommand, Debug, Clone)]
pub enum KeySubcommand {
    /// List all existing keys
    List,

    /// Add a new key
    Add(AddArgs),

    /// Delete a key
    Delete(DeleteArgs),

    /// Change the password for a user's key
    ChangePassword(PasswordChangeArgs),
}

#[derive(Args, Debug)]
#[clap(about = "Create and manage keys")]
pub struct CmdArgs {
    #[command(subcommand)]
    subcommand: KeySubcommand,
}

pub fn run(global_args: &GlobalArgs, args: &CmdArgs) -> Result<()> {
    println!();
    match &args.subcommand {
        KeySubcommand::List => run_list(global_args),
        KeySubcommand::Add(args) => run_add(global_args, args),
        KeySubcommand::Delete(args) => run_delete(global_args, args),
        KeySubcommand::ChangePassword(args) => run_password_change(global_args, args),
    }
}

fn run_list(global_args: &GlobalArgs) -> Result<()> {
    let backend = new_backend_with_prompt(BackendOptions {
        repo_path: global_args.repo.clone(),
        ssh_pubkey: global_args.ssh_pubkey.clone(),
        ssh_privatekey: global_args.ssh_privatekey.clone(),
        dry_backend: false,
    })?;

    let mut table = Table::new();
    table.set_headers(vec![
        "Username ▼".bold().yellow().to_string(),
        "Key ID".bold().yellow().to_string(),
        "Created".bold().yellow().to_string(),
    ]);

    let keyfile_streamer = KeyFileStreamer::new(backend.clone())?;
    for (id, keyfile) in keyfile_streamer.flatten() {
        table.add_row(vec![
            keyfile.username,
            id.to_short_hex(6),
            utils::pretty_print_timestamp(&keyfile.created),
        ]);
    }

    println!("{}", table.render());

    Ok(())
}

fn run_add(global_args: &GlobalArgs, args: &AddArgs) -> Result<()> {
    let auth = request_auth();
    let backend = new_backend_with_prompt(BackendOptions {
        repo_path: global_args.repo.clone(),
        ssh_pubkey: global_args.ssh_pubkey.clone(),
        ssh_privatekey: global_args.ssh_privatekey.clone(),
        dry_backend: false,
    })?;

    let key_manager = KeyManager::new(backend.clone());
    let (_key_id, master_key) = key_manager.retrieve_master_key(&auth, global_args.key.as_ref())?;

    ui::cli::log!("\nCreating new user key...");
    let new_auth = request_new_auth();
    let new_key_file = KeyManager::generate_key_file(&new_auth, master_key)
        .with_context(|| "Could not generate key")?;

    let ss = SecureStorage::build().with_compression(zstd::DEFAULT_COMPRESSION_LEVEL);

    let new_keyfile_json = serde_json::to_string_pretty(&new_key_file)?;
    let new_keyfile_json = ss.compress(new_keyfile_json.as_bytes())?;
    let new_keyfile_id = ID::from_content(&new_keyfile_json);

    match &args.output_keyfile_path {
        Some(path) => {
            std::fs::write(path, &new_keyfile_json)?;
        }
        None => {
            let path = PathBuf::from(KEYS_DIR).join(new_keyfile_id.to_hex());
            backend.write(&path, &new_keyfile_json)?;
        }
    }

    Ok(())
}

fn run_delete(global_args: &GlobalArgs, args: &DeleteArgs) -> Result<()> {
    let backend = new_backend_with_prompt(BackendOptions {
        repo_path: global_args.repo.clone(),
        ssh_pubkey: global_args.ssh_pubkey.clone(),
        ssh_privatekey: global_args.ssh_privatekey.clone(),
        dry_backend: false,
    })?;
    let key_manager = KeyManager::new(backend.clone());
    let (_id, path) = key_manager.find_id_with_prefix(&args.id)?;
    backend.remove_file(&path)
}

fn run_password_change(global_args: &GlobalArgs, _args: &PasswordChangeArgs) -> Result<()> {
    let auth = request_auth();
    let backend = new_backend_with_prompt(BackendOptions {
        repo_path: global_args.repo.clone(),
        ssh_pubkey: global_args.ssh_pubkey.clone(),
        ssh_privatekey: global_args.ssh_privatekey.clone(),
        dry_backend: false,
    })?;

    let key_manager = KeyManager::new(backend.clone());
    let (old_id, old_keyfile) = key_manager
        .load_keyfile_with_username(&auth.username)?
        .with_context(|| format!("No keyfile found for username {}", &auth.username))?;
    let master_key = KeyManager::decode_master_key(&auth.password, &old_keyfile)?;

    let new_auth = Auth {
        username: auth.username,
        password: ui::cli::request_new_password("Enter the new password", "Confirm password"),
    };

    let new_keyfile = KeyManager::generate_key_file(&new_auth, master_key)?;
    key_manager.save_keyfile(&new_keyfile)?;
    key_manager.delete_keyfile_with_id(&old_id)?;

    Ok(())
}
