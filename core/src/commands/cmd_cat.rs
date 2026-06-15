use std::str::FromStr;

use anyhow::{Context, Result, bail};
use clap::Args;

use crate::{
    backend::new_backend_with_prompt,
    commands::{GlobalArgs, cleanup::CleanupHandler, with_repository_lock},
    fs::tree::Tree,
    mapache::ContentIdType,
    ui,
};

#[derive(Args, Debug, Clone)]
#[clap(about = "Print repository objects")]
pub struct CmdArgs {
    /// Object to print:
    /// [manifest|snapshot:ID|pack:ID|blob:ID|tree:ID|index:ID|key:ID|lock:ID].
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

pub async fn run(global_args: &GlobalArgs, args: &CmdArgs) -> Result<()> {
    with_repository_lock(
        global_args.auth_file.as_ref(),
        global_args.key.as_ref(),
        new_backend_with_prompt(global_args.backend_options(false)).await?,
        global_args.to_repo_config(),
        false,
        global_args.retry_lock_duration,
        global_args.no_lock,
        |repo, _, lock_handle| async move {
            let cleanup_handler = CleanupHandler::new()?;
            cleanup_handler.add_lock(lock_handle);

            repo.reload_master_index().await?;

            match &args.object {
                Object::Manifest => {
                    let manifest = repo.manifest();
                    ui::cli::log!("{}", serde_json::to_string_pretty(&manifest)?);
                    Ok(())
                }
                Object::Pack(prefix) => {
                    let (id, _) = repo.find(ContentIdType::Pack, prefix).await?;
                    let object = repo.load_pack(&id).await.context("Failed to load object")?;
                    ui::cli::log!("{}", serde_json::to_string_pretty(&object)?);
                    Ok(())
                }
                Object::Tree(prefix) => {
                    let index = repo.index();
                    let id = match index.search_prefix(prefix).await? {
                        Some(val) => val,
                        None => bail!("No tree blobs found with prefix {prefix}"),
                    };
                    let tree = repo
                        .load_blob(&id)
                        .await
                        .context("Failed to load tree blob")?;
                    let tree: Tree = serde_json::from_slice(&tree)?;
                    ui::cli::log!("{}", serde_json::to_string_pretty(&tree)?);
                    Ok(())
                }
                Object::Blob(prefix) => {
                    let index = repo.index();
                    let id = match index.search_prefix(prefix).await? {
                        Some(val) => val,
                        None => bail!("No blobs found with prefix {prefix}"),
                    };
                    let blob = repo.load_blob(&id).await.context("Failed to load blob")?;
                    ui::cli::log!("{}", String::from_utf8(blob)?);
                    Ok(())
                }
                Object::Index(prefix) => {
                    let (id, _) = repo.find(ContentIdType::Index, prefix).await?;
                    let index = repo.load_index(&id).await.context("Failed to load index")?;
                    ui::cli::log!("{}", serde_json::to_string_pretty(&index)?);
                    Ok(())
                }
                Object::Key(prefix) => {
                    let (id, _) = repo.find(ContentIdType::Key, prefix).await?;
                    let key = repo.load_key(&id).await.context("Failed to load key")?;
                    ui::cli::log!("{}", serde_json::to_string_pretty(&key)?);
                    Ok(())
                }
                Object::Snapshot(prefix) => {
                    let (id, _) = repo.find(ContentIdType::Snapshot, prefix).await?;
                    let snapshot = repo
                        .load_snapshot(&id, None)
                        .await
                        .context("Failed to load snapshot")?;
                    ui::cli::log!("{}", serde_json::to_string_pretty(&snapshot)?);
                    Ok(())
                }
                Object::Lock(prefix) => {
                    let (id, _) = repo.find(ContentIdType::Lock, prefix).await?;
                    let lock = repo.load_lock(&id).await.context("Failed to load lock")?;
                    ui::cli::log!("{}", serde_json::to_string_pretty(&lock)?);
                    Ok(())
                }
            }
        },
    )
    .await
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
