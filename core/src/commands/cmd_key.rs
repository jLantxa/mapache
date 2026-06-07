use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use clap::{Args, Subcommand};
use colored::Colorize;
use futures::StreamExt;

use crate::{
    backend::{Handle, WriteContents, new_backend_with_prompt},
    commands::GlobalArgs,
    mapache::{ContentIdType, ID, defaults::DEFAULT_COMPRESSION},
    repository::{
        keys::{KeyFileStream, KeyManager},
        repo::{Auth, KEYS_DIR},
        storage::SecureStorage,
    },
    ui::{
        self,
        cli::{request_auth, request_new_auth, table::Table},
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

#[derive(Args, Debug, Clone)]
pub struct ExportArgs {
    /// ID (or prefix) of the key to export
    #[clap(value_parser)]
    id: String,

    /// Path to save the exported key file
    #[clap(short, long = "output", default_value = "keyfile")]
    output_path: PathBuf,
}

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

    /// Export a key file from the repository and save it locally
    Export(ExportArgs),
}

#[derive(Args, Debug, Clone)]
#[clap(about = "Create and manage keys")]
pub struct CmdArgs {
    #[command(subcommand)]
    pub subcommand: KeySubcommand,
}

pub async fn run(global_args: &GlobalArgs, args: &CmdArgs) -> Result<()> {
    match &args.subcommand {
        KeySubcommand::List => run_list(global_args).await,
        KeySubcommand::Add(args) => run_add(global_args, args).await,
        KeySubcommand::Delete(args) => run_delete(global_args, args).await,
        KeySubcommand::ChangePassword(args) => run_password_change(global_args, args).await,
        KeySubcommand::Export(args) => run_export(global_args, args).await,
    }
}

async fn run_list(global_args: &GlobalArgs) -> Result<()> {
    let backend = new_backend_with_prompt(global_args.backend_options(false)).await?;

    let mut table = Table::new();
    table.set_headers(vec![
        "Username ▼".bold().yellow().to_string(),
        "Key ID".bold().yellow().to_string(),
        "Created".bold().yellow().to_string(),
    ]);

    let mut keyfile_stream = KeyFileStream::new(backend.clone()).await?;
    while let Some(res) = keyfile_stream.next().await {
        let (id, keyfile) = res?;
        table.add_row(vec![
            keyfile.username,
            id.to_short_hex(6),
            utils::pretty_print_timestamp(&keyfile.created, None),
        ]);
    }

    ui::cli::log!("{}", table.render());

    Ok(())
}

async fn run_add(global_args: &GlobalArgs, args: &AddArgs) -> Result<()> {
    let auth = request_auth()?;
    let backend = new_backend_with_prompt(global_args.backend_options(false)).await?;

    let key_manager = KeyManager::new(backend.clone());
    let (_key_id, master_key) = key_manager
        .retrieve_master_key(&auth, global_args.key.as_ref())
        .await?;

    ui::cli::log!("\nCreating new user key...");
    let new_auth = request_new_auth()?;
    let new_key_file =
        KeyManager::generate_key_file(&new_auth, &master_key).context("Could not generate key")?;

    let ss = SecureStorage::new().with_compression(DEFAULT_COMPRESSION.to_level());

    let new_keyfile_json = serde_json::to_string_pretty(&new_key_file)?;
    let new_keyfile_json = ss.compress(new_keyfile_json.as_bytes())?;
    let new_keyfile_id = ID::from_content(&new_keyfile_json);

    match &args.output_keyfile_path {
        Some(path) => {
            std::fs::write(path, &new_keyfile_json)?;
        }
        None => {
            let path = Path::new(KEYS_DIR).join(new_keyfile_id.to_hex());
            let handle = Handle::new_with_hint(&path, ContentIdType::Key, true);
            backend
                .write(&handle, WriteContents::Owned(new_keyfile_json))
                .await?;
        }
    }

    Ok(())
}

async fn run_delete(global_args: &GlobalArgs, args: &DeleteArgs) -> Result<()> {
    tracing::info!(target: "key", "Starting key delete command (id={})", args.id);
    let backend = new_backend_with_prompt(global_args.backend_options(false)).await?;
    let key_manager = KeyManager::new(backend.clone());
    let (id, path) = key_manager.find_id_with_prefix(&args.id).await?;
    tracing::info!(target: "key", "Deleting key file {}", id.to_short_hex(8));
    backend.remove(&path).await
}

async fn run_password_change(global_args: &GlobalArgs, _args: &PasswordChangeArgs) -> Result<()> {
    let auth = request_auth()?;
    let backend = new_backend_with_prompt(global_args.backend_options(false)).await?;

    let key_manager = KeyManager::new(backend.clone());
    let (old_id, old_keyfile) = key_manager
        .load_keyfile_with_username(&auth.username)
        .await?
        .with_context(|| format!("No keyfile found for username {}", &auth.username))?;
    let master_key = KeyManager::decode_master_key(&auth.password, &old_keyfile)?;

    let new_auth = Auth {
        username: auth.username.clone(),
        password: ui::cli::request_new_password("Enter the new password", "Confirm password")?,
    };

    let new_keyfile = KeyManager::generate_key_file(&new_auth, &master_key)?;
    tracing::info!(target: "key", "Saving updated key file for user {}", auth.username);
    key_manager.save_keyfile(&new_keyfile).await?;
    tracing::info!(target: "key", "Deleting old key file {}", old_id.to_short_hex(8));
    key_manager.delete_keyfile_with_id(&old_id).await?;

    Ok(())
}

async fn run_export(global_args: &GlobalArgs, args: &ExportArgs) -> Result<()> {
    let backend = new_backend_with_prompt(global_args.backend_options(false)).await?;
    let key_manager = KeyManager::new(backend.clone());

    let (id, _path) = key_manager.find_id_with_prefix(&args.id).await?;
    let raw_keyfile = key_manager.load_raw_keyfile(&id).await?;

    std::fs::write(&args.output_path, &raw_keyfile)?;

    ui::cli::log!(
        "Exported key {} to {}",
        id.to_short_hex(8).italic(),
        args.output_path.display().to_string().bold()
    );

    Ok(())
}
