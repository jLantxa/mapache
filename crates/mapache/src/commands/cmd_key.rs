use std::{
    io,
    path::{Path, PathBuf},
};

use clap::{Args, Subcommand};
use futures::StreamExt;

use crate::{
    backend::{Handle, WriteContents, new_backend_with_prompt},
    commands::{GlobalArgs, ToExitCode},
    common::{ContentIdType, ID, defaults::DEFAULT_COMPRESSION, error::MapacheError},
    repository::{
        keys::{KeyFileStream, KeyManager},
        repo::{Auth, KEYS_DIR, Repository},
        storage::SecureStorage,
    },
    ui::{
        self,
        cli::{color::Colorize, request_auth, request_new_auth, table::Table},
    },
    utils::{self},
};

#[derive(Debug, thiserror::Error)]
pub enum KeyError {
    #[error("failed to open repository: {0}")]
    RepoOpenFail(String),
    #[error(transparent)]
    Io(#[from] io::Error),
    #[error(transparent)]
    Repo(#[from] MapacheError),
}

impl ToExitCode for KeyError {
    fn to_exit_code(&self) -> i32 {
        match self {
            KeyError::RepoOpenFail(_) => 10,
            KeyError::Io(_) => 4,
            KeyError::Repo(_) => 1,
        }
    }
}

#[derive(Args, Debug, Clone)]
pub struct AddArgs {
    /// Optional path to save the new Keyfile
    #[clap(long = "path", value_parser)]
    output_keyfile_path: Option<PathBuf>,

    /// Benchmark and tune Argon2id parameters for this hardware
    #[clap(long)]
    pub calibrate_kdf: bool,
}

#[derive(Args, Debug, Clone)]
pub struct DeleteArgs {
    #[clap(value_parser)]
    id: String,

    /// Skip the confirmation prompt
    #[clap(long)]
    pub yes: bool,
}

#[derive(Args, Debug, Clone)]
pub struct PasswordChangeArgs {
    /// Benchmark and tune Argon2id parameters for this hardware
    #[clap(long)]
    pub calibrate_kdf: bool,
}

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

pub async fn run(global_args: &GlobalArgs, args: &CmdArgs) -> Result<(), KeyError> {
    match &args.subcommand {
        KeySubcommand::List => run_list(global_args).await,
        KeySubcommand::Add(args) => run_add(global_args, args).await,
        KeySubcommand::Delete(args) => run_delete(global_args, args).await,
        KeySubcommand::ChangePassword(args) => run_password_change(global_args, args).await,
        KeySubcommand::Export(args) => run_export(global_args, args).await,
    }
}

async fn run_list(global_args: &GlobalArgs) -> Result<(), KeyError> {
    let backend = new_backend_with_prompt(global_args.backend_options(false))
        .await
        .map_err(|e| {
            KeyError::RepoOpenFail(format!("failed to initialize backend: {}", e.inner()))
        })?;

    let mut table = Table::new();
    table.set_headers(vec![
        "Username ▼".bold().yellow().to_string(),
        "Key ID".bold().yellow().to_string(),
        "Created".bold().yellow().to_string(),
    ]);

    let mut keyfile_stream = KeyFileStream::new(backend.clone())
        .await
        .map_err(|e| KeyError::RepoOpenFail(format!("failed to open key stream: {}", e.inner())))?;
    while let Some(res) = keyfile_stream.next().await {
        let (id, keyfile) = res.map_err(|e| {
            KeyError::RepoOpenFail(format!("failed to read key file: {}", e.inner()))
        })?;
        table.add_row(vec![
            keyfile.username,
            id.to_short_hex(6),
            utils::pretty_print_timestamp(&keyfile.created, None),
        ]);
    }

    ui::cli::log!("{}", table.render());

    Ok(())
}

async fn run_add(global_args: &GlobalArgs, args: &AddArgs) -> Result<(), KeyError> {
    let auth = request_auth()
        .map_err(|e| KeyError::RepoOpenFail(format!("authentication failed: {}", e.inner())))?;
    let backend = new_backend_with_prompt(global_args.backend_options(false))
        .await
        .map_err(|e| {
            KeyError::RepoOpenFail(format!("failed to initialize backend: {}", e.inner()))
        })?;

    let key_manager = KeyManager::new(backend.clone());
    let (_key_id, master_key) = key_manager
        .retrieve_master_key(&auth, global_args.key.as_ref())
        .await
        .map_err(|e| {
            KeyError::RepoOpenFail(format!("failed to retrieve master key: {}", e.inner()))
        })?;

    let repo_version = Repository::load_manifest_version(&master_key, backend.clone())
        .await
        .map_err(|e| {
            KeyError::RepoOpenFail(format!("failed to load repository manifest: {}", e.inner()))
        })?;

    ui::cli::log!("\nCreating new user key...");
    let new_auth = request_new_auth()
        .map_err(|e| KeyError::RepoOpenFail(format!("failed to get new auth: {}", e.inner())))?;
    let new_key_file =
        KeyManager::generate_key_file(&new_auth, &master_key, repo_version, args.calibrate_kdf)
            .map_err(|e| {
                KeyError::RepoOpenFail(format!("could not generate key: {}", e.inner()))
            })?;

    let ss = SecureStorage::new().with_compression(DEFAULT_COMPRESSION.to_level());

    let new_keyfile_json = serde_json::to_string_pretty(&new_key_file)
        .map_err(MapacheError::Serialization)
        .map_err(KeyError::Repo)?;
    let new_keyfile_json = ss
        .compress(new_keyfile_json.as_bytes())
        .map_err(|e| KeyError::RepoOpenFail(format!("compression failed: {}", e.inner())))?;
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
                .await
                .map_err(|e| {
                    KeyError::RepoOpenFail(format!("failed to write key file: {}", e.inner()))
                })?;
        }
    }

    Ok(())
}

async fn run_delete(global_args: &GlobalArgs, args: &DeleteArgs) -> Result<(), KeyError> {
    tracing::info!(target: "key", "Starting key delete command (id={})", args.id);
    let backend = new_backend_with_prompt(global_args.backend_options(false))
        .await
        .map_err(|e| {
            KeyError::RepoOpenFail(format!("failed to initialize backend: {}", e.inner()))
        })?;
    let key_manager = KeyManager::new(backend.clone());
    let (id, path) = key_manager
        .find_id_with_prefix(&args.id)
        .await
        .map_err(|e| KeyError::RepoOpenFail(format!("failed to find key: {}", e.inner())))?;

    if !args.yes
        && !confirm_prompt(&format!(
            "Delete key {}? This cannot be undone. [y/N] ",
            id.to_short_hex(8)
        ))?
    {
        ui::cli::log!("Aborted.");
        return Ok(());
    }

    tracing::info!(target: "key", "Deleting key file {}", id.to_short_hex(8));
    backend
        .remove(&path)
        .await
        .map_err(|e| KeyError::RepoOpenFail(format!("failed to remove key file: {}", e.inner())))?;
    Ok(())
}

async fn run_password_change(
    global_args: &GlobalArgs,
    args: &PasswordChangeArgs,
) -> Result<(), KeyError> {
    let auth = request_auth()
        .map_err(|e| KeyError::RepoOpenFail(format!("authentication failed: {}", e.inner())))?;
    let backend = new_backend_with_prompt(global_args.backend_options(false))
        .await
        .map_err(|e| {
            KeyError::RepoOpenFail(format!("failed to initialize backend: {}", e.inner()))
        })?;

    let key_manager = KeyManager::new(backend.clone());
    let (old_id, old_keyfile) = key_manager
        .load_keyfile_with_username(&auth.username)
        .await
        .map_err(|e| KeyError::RepoOpenFail(format!("failed to load keyfile: {}", e.inner())))?
        .ok_or_else(|| {
            KeyError::RepoOpenFail(format!("no keyfile found for username {}", auth.username))
        })?;
    let master_key = KeyManager::decode_master_key(&auth.password, &old_keyfile).map_err(|e| {
        KeyError::RepoOpenFail(format!("failed to decode master key: {}", e.inner()))
    })?;

    let repo_version = Repository::load_manifest_version(&master_key, backend.clone())
        .await
        .map_err(|e| {
            KeyError::RepoOpenFail(format!("failed to load repository manifest: {}", e.inner()))
        })?;

    let new_auth = Auth {
        username: auth.username.clone(),
        password: ui::cli::request_new_password("Enter the new password", "Confirm password")
            .map_err(|e| {
                KeyError::RepoOpenFail(format!("password prompt failed: {}", e.inner()))
            })?,
    };

    let new_keyfile =
        KeyManager::generate_key_file(&new_auth, &master_key, repo_version, args.calibrate_kdf)
            .map_err(|e| {
                KeyError::RepoOpenFail(format!("failed to generate key file: {}", e.inner()))
            })?;
    tracing::info!(target: "key", "Saving updated key file for user {}", auth.username);
    key_manager
        .save_keyfile(&new_keyfile, &new_auth.username)
        .await
        .map_err(|e| KeyError::RepoOpenFail(format!("failed to save key file: {}", e.inner())))?;
    tracing::info!(target: "key", "Deleting old key file {}", old_id.to_short_hex(8));
    key_manager
        .delete_keyfile_with_id(&old_id)
        .await
        .map_err(|e| {
            KeyError::RepoOpenFail(format!("failed to delete old key file: {}", e.inner()))
        })?;

    Ok(())
}

async fn run_export(global_args: &GlobalArgs, args: &ExportArgs) -> Result<(), KeyError> {
    let backend = new_backend_with_prompt(global_args.backend_options(false))
        .await
        .map_err(|e| {
            KeyError::RepoOpenFail(format!("failed to initialize backend: {}", e.inner()))
        })?;
    let key_manager = KeyManager::new(backend.clone());

    let (id, _path) = key_manager
        .find_id_with_prefix(&args.id)
        .await
        .map_err(|e| KeyError::RepoOpenFail(format!("failed to find key: {}", e.inner())))?;
    let raw_keyfile = key_manager
        .load_raw_keyfile(&id)
        .await
        .map_err(|e| KeyError::RepoOpenFail(format!("failed to load key file: {e}")))?;

    std::fs::write(&args.output_path, &raw_keyfile)?;

    ui::cli::log!(
        "Exported key {} to {}",
        id.to_short_hex(8).italic(),
        args.output_path.display().to_string().bold()
    );

    Ok(())
}

/// Prompts the user for a yes/no confirmation. Anything other than "y"/"yes"
/// (case-insensitive) is treated as a "no".
fn confirm_prompt(prompt: &str) -> io::Result<bool> {
    use std::io::Write;
    let mut stdout = io::stdout();
    stdout.write_all(prompt.as_bytes())?;
    stdout.flush()?;
    let mut line = String::new();
    io::stdin().read_line(&mut line)?;
    let answer = line.trim().to_ascii_lowercase();
    Ok(matches!(answer.as_str(), "y" | "yes"))
}
