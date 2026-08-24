use std::{io, str::FromStr};

use clap::Args;

use crate::{
    backend::new_backend_with_prompt,
    commands::{GlobalArgs, ToExitCode, cleanup::CleanupHandler, with_repository_lock},
    common::{ContentIdType, error::MapacheError},
    fs::tree::Tree,
    ui,
};

#[derive(Debug, thiserror::Error)]
pub enum CatError {
    #[error("object not found: {0}")]
    ObjectNotFound(String),
    #[error(transparent)]
    Repo(#[from] MapacheError),
    #[error(transparent)]
    Io(#[from] io::Error),
}

impl ToExitCode for CatError {
    fn to_exit_code(&self) -> i32 {
        match self {
            CatError::ObjectNotFound(_) => 20,
            CatError::Repo(_) => 1,
            CatError::Io(_) => 1,
        }
    }
}

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

pub async fn run(global_args: &GlobalArgs, args: &CmdArgs) -> Result<(), CatError> {
    with_repository_lock(
        global_args.auth_file.as_ref(),
        global_args.key.as_ref(),
        new_backend_with_prompt(global_args.backend_options(false))
            .await
            .map_err(CatError::Repo)?,
        global_args.to_repo_config(),
        false,
        global_args.retry_lock_duration,
        global_args.no_lock,
        |repo, _, lock_handle| async move {
            let cleanup_handler = CleanupHandler::new();
            cleanup_handler.add_lock(lock_handle);

            repo.reload_master_index().await?;

            match &args.object {
                Object::Manifest => {
                    let manifest = repo.manifest();
                    ui::cli::log!(
                        "{}",
                        serde_json::to_string_pretty(&manifest)
                            .map_err(MapacheError::Serialization)?
                    );
                    Ok(())
                }
                Object::Pack(prefix) => {
                    let (id, _) = repo.find(ContentIdType::Pack, prefix).await?;
                    let object = repo.load_pack(&id).await.map_err(|e| {
                        CatError::ObjectNotFound(format!("failed to load object: {}", e.inner()))
                    })?;
                    ui::cli::log!(
                        "{}",
                        serde_json::to_string_pretty(&object)
                            .map_err(MapacheError::Serialization)?
                    );
                    Ok(())
                }
                Object::Tree(prefix) => {
                    let index = repo.index();
                    let id = match index.search_prefix(prefix).await? {
                        Some(val) => val,
                        None => {
                            return Err(CatError::ObjectNotFound(format!(
                                "no tree blobs found with prefix {prefix}"
                            )));
                        }
                    };
                    let tree_data = repo.load_blob(&id).await.map_err(|e| {
                        CatError::ObjectNotFound(format!("failed to load tree blob: {}", e.inner()))
                    })?;
                    let tree: Tree = if repo.repo_version() >= 2 {
                        Tree::from_binary(&tree_data)
                    } else {
                        Tree::from_json(&tree_data)
                    }
                    .map_err(|e| {
                        CatError::ObjectNotFound(format!("failed to deserialize tree: {e}"))
                    })?;
                    ui::cli::log!(
                        "{}",
                        serde_json::to_string_pretty(&tree).map_err(MapacheError::Serialization)?
                    );
                    Ok(())
                }
                Object::Blob(prefix) => {
                    let index = repo.index();
                    let id = match index.search_prefix(prefix).await? {
                        Some(val) => val,
                        None => {
                            return Err(CatError::ObjectNotFound(format!(
                                "no blobs found with prefix {prefix}"
                            )));
                        }
                    };
                    let blob = repo.load_blob(&id).await.map_err(|e| {
                        CatError::ObjectNotFound(format!("failed to load blob: {}", e.inner()))
                    })?;
                    ui::cli::log!(
                        "{}",
                        String::from_utf8(blob).map_err(|e| CatError::Io(io::Error::new(
                            io::ErrorKind::InvalidData,
                            e
                        )))?
                    );
                    Ok(())
                }
                Object::Index(prefix) => {
                    let (id, _) = repo.find(ContentIdType::Index, prefix).await?;
                    let index = repo.load_index(&id).await.map_err(|e| {
                        CatError::ObjectNotFound(format!("failed to load index: {}", e.inner()))
                    })?;
                    ui::cli::log!(
                        "{}",
                        serde_json::to_string_pretty(&index)
                            .map_err(MapacheError::Serialization)?
                    );
                    Ok(())
                }
                Object::Key(prefix) => {
                    let (id, _) = repo.find(ContentIdType::Key, prefix).await?;
                    let key = repo.load_key(&id).await.map_err(|e| {
                        CatError::ObjectNotFound(format!("failed to load key: {}", e.inner()))
                    })?;
                    ui::cli::log!(
                        "{}",
                        serde_json::to_string_pretty(&key).map_err(MapacheError::Serialization)?
                    );
                    Ok(())
                }
                Object::Snapshot(prefix) => {
                    let (id, _) = repo.find(ContentIdType::Snapshot, prefix).await?;
                    let snapshot = repo.load_snapshot(&id, None).await.map_err(|e| {
                        CatError::ObjectNotFound(format!("failed to load snapshot: {}", e.inner()))
                    })?;
                    ui::cli::log!(
                        "{}",
                        serde_json::to_string_pretty(&snapshot)
                            .map_err(MapacheError::Serialization)?
                    );
                    Ok(())
                }
                Object::Lock(prefix) => {
                    let (id, _) = repo.find(ContentIdType::Lock, prefix).await?;
                    let lock = repo.load_lock(&id).await.map_err(|e| {
                        CatError::ObjectNotFound(format!("failed to load lock: {}", e.inner()))
                    })?;
                    ui::cli::log!(
                        "{}",
                        serde_json::to_string_pretty(&lock).map_err(MapacheError::Serialization)?
                    );
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
                    Err("pack object requires an ID, e.g., 'pack:some_id'".to_string())
                }
            }
            "tree" => {
                if parts.len() == 2 {
                    Ok(Object::Tree(parts[1].to_string()))
                } else {
                    Err("tree object requires an ID, e.g., 'tree:some_id'".to_string())
                }
            }
            "blob" => {
                if parts.len() == 2 {
                    Ok(Object::Blob(parts[1].to_string()))
                } else {
                    Err("blob object requires an ID, e.g., 'blob:some_id'".to_string())
                }
            }
            "index" => {
                if parts.len() == 2 {
                    Ok(Object::Index(parts[1].to_string()))
                } else {
                    Err("index object requires an ID, e.g., 'index:some_id'".to_string())
                }
            }
            "key" => {
                if parts.len() == 2 {
                    Ok(Object::Key(parts[1].to_string()))
                } else {
                    Err("key object requires an ID, e.g., 'key:some_id'".to_string())
                }
            }
            "snapshot" => {
                if parts.len() == 2 {
                    Ok(Object::Snapshot(parts[1].to_string()))
                } else {
                    Err("snapshot object requires an ID, e.g., 'snapshot:some_id'".to_string())
                }
            }
            "lock" => {
                if parts.len() == 2 {
                    Ok(Object::Lock(parts[1].to_string()))
                } else {
                    Err("lock object requires an ID, e.g., 'lock:some_id'".to_string())
                }
            }
            _ => Err(format!("unknown object type: {}", parts[0])),
        }
    }
}
