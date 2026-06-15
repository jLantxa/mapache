use std::path::{Path, PathBuf};

use anyhow::{Context, Result, anyhow};
use clap::Args;
use serde::Serialize;

use crate::{
    backend::new_backend_with_prompt,
    commands::{
        self, GlobalArgs, ToExitCode, UseSnapshot, cleanup::CleanupHandler, fail,
        find_use_snapshot, with_repository_lock,
    },
    fs::{
        node::{Metadata, Node, NodeType, node_to_string},
        tree::{Tree, find_serialized_node},
    },
    mapache::ID,
    repository::repo::Repository,
    ui::{self, cli::color::Colorize},
};

#[derive(Debug, Clone, Copy)]
pub enum LsError {
    RepoOpenFail = 10,
    SnapshotNotFound = 20,
    PathNotFound = 21,
    LsFailed = 30,
}

impl ToExitCode for LsError {
    fn to_exit_code(&self) -> i32 {
        *self as i32
    }
}

#[derive(Args, Debug)]
#[clap(about = "List nodes in the repository")]
pub struct CmdArgs {
    /// Snapshot ID (prefix) or 'latest' for the most recent snapshots.
    #[clap(value_parser, default_value_t = UseSnapshot::Latest)]
    pub snapshot: UseSnapshot,

    /// Path
    #[clap(long, value_parser)]
    pub path: Option<PathBuf>,

    /// Use a long listing format
    #[clap(short = 'l', long, value_parser)]
    pub long: bool,

    /// Print sizes with units in a human readable format (1 byte, 1.24 KiB, etc.)
    #[clap(short = 'H', long, value_parser)]
    pub human_readable: bool,

    /// List subdirectories recursively
    #[clap(short = 'R', long, value_parser)]
    pub recursive: bool,
}

#[derive(Serialize)]
struct LsEntry {
    path: PathBuf,
    node: Node,
}

#[derive(Serialize)]
struct LsOutput {
    entries: Vec<LsEntry>,
}

pub async fn run(global_args: &GlobalArgs, args: &CmdArgs) -> Result<()> {
    with_repository_lock(
        global_args.auth_file.as_ref(),
        global_args.key.as_ref(),
        new_backend_with_prompt(global_args.backend_options(false))
            .await
            .map_err(|e| {
                fail(
                    format!("Failed to initialize backend: {}", e),
                    LsError::LsFailed,
                )
            })?,
        global_args.to_repo_config(),
        false,
        global_args.retry_lock_duration,
        global_args.no_lock,
        |repo, _secure_storage, lock_handle| async move {
            let cleanup_handler = CleanupHandler::new().map_err(|e| {
                fail(
                    format!("Failed to initialize cleanup handler: {}", e),
                    LsError::LsFailed,
                )
            })?;
            cleanup_handler.add_lock(lock_handle);

            repo.reload_master_index().await.map_err(|e| {
                fail(
                    format!("Failed to reload master index: {}", e),
                    LsError::LsFailed,
                )
            })?;

            let (_snapshot_id, snapshot) =
                match find_use_snapshot(repo.clone(), &args.snapshot).await {
                    Ok(Some(pair)) => pair,
                    _ => {
                        return Err(fail("Snapshot not found", LsError::SnapshotNotFound));
                    }
                };

            let node = if let Some(p) = &args.path {
                find_serialized_node(repo.as_ref(), &snapshot.tree, p)
                    .await
                    .map_err(|e| fail(format!("Error finding path: {}", e), LsError::LsFailed))?
                    .with_context(|| format!("'{}' does not exist in snapshot", p.display()))
                    .map_err(|e| fail(e.to_string(), LsError::PathNotFound))?
            } else {
                Node::new_root(&snapshot.tree)
            };

            ls(
                &args.path.clone().unwrap_or_default(),
                &node,
                &repo,
                args,
                global_args.json,
            )
            .await
            .map_err(|e| fail(format!("Listing failed: {}", e), LsError::LsFailed))?;

            Ok(())
        },
    )
    .await
    .map_err(|e| {
        if e.is::<commands::error::MapacheError>() {
            e
        } else {
            fail(
                format!("Failed to open repository: {}", e),
                LsError::RepoOpenFail,
            )
        }
    })
}

/// List the contents of a node.
async fn ls(
    path: &Path,
    node: &Node,
    repo: &Repository,
    args: &CmdArgs,
    json_out: bool,
) -> Result<()> {
    if json_out {
        return ls_json(path, node, repo, args).await;
    }

    if !node.is_dir() {
        ui::cli::log!(
            "{}",
            node_to_string(node, None, args.long, args.human_readable)
        );
        return Ok(());
    }

    if args.recursive {
        ui::cli::log!("{}:", path.display());
    }

    ls_recursive(path, node, repo, args).await
}

/// JSON output for ls
async fn ls_json(path: &Path, node: &Node, repo: &Repository, args: &CmdArgs) -> Result<()> {
    if !node.is_dir() {
        ui::json::emit_static(
            "ls",
            &LsOutput {
                entries: vec![LsEntry {
                    path: path.to_path_buf(),
                    node: node.clone(),
                }],
            },
        );
        return Ok(());
    }

    if args.recursive {
        let mut entries = Vec::new();
        ls_recursive_json(path, node, repo, &mut entries).await?;
        ui::json::emit_static("ls", &LsOutput { entries });
    } else {
        let tree_id = node
            .tree
            .as_ref()
            .ok_or_else(|| anyhow!("Directory node missing tree ID: {}", node.name))?;
        let mut tree = Tree::load_from_repo(repo, tree_id).await?;
        tree.nodes.sort_unstable_by(|a, b| a.name.cmp(&b.name));
        let entries: Vec<LsEntry> = tree
            .nodes
            .iter()
            .map(|n| LsEntry {
                path: path.join(&n.name),
                node: n.clone(),
            })
            .collect();
        ui::json::emit_static("ls", &LsOutput { entries });
    }

    Ok(())
}

/// List a snapshot tree.
async fn ls_recursive(path: &Path, node: &Node, repo: &Repository, args: &CmdArgs) -> Result<()> {
    let mut stack: Vec<(PathBuf, Node)> = Vec::new();

    if node.is_dir() && args.recursive {
        stack.push((path.to_path_buf(), node.clone()));
    } else if node.is_dir() {
        let tree_id = node
            .tree
            .as_ref()
            .ok_or_else(|| anyhow!("Directory node missing tree ID: {}", node.name))?;
        let mut tree = Tree::load_from_repo(repo, tree_id).await?;
        tree.nodes.sort_unstable_by(|a, b| a.name.cmp(&b.name));
        print_tree(&tree, args);
        return Ok(());
    }

    while let Some((parent_path, node)) = stack.pop() {
        let tree_id = node
            .tree
            .as_ref()
            .ok_or_else(|| anyhow!("Directory node missing tree ID: {}", node.name))?;
        let current_path = parent_path.join(&node.name);

        let mut tree = Tree::load_from_repo(repo, tree_id).await?;

        if args.recursive {
            ui::cli::log!();
            ui::cli::log!("{}:", current_path.to_string_lossy().bold().underline());
        }

        tree.nodes.sort_unstable_by(|a, b| a.name.cmp(&b.name));
        print_tree(&tree, args);

        for node in tree.nodes.into_iter().rev() {
            if node.is_dir() {
                stack.push((current_path.clone(), node));
            }
        }
    }

    Ok(())
}

/// Recursive listing for JSON output
async fn ls_recursive_json(
    path: &Path,
    node: &Node,
    repo: &Repository,
    entries: &mut Vec<LsEntry>,
) -> Result<()> {
    let mut stack: Vec<(PathBuf, Node)> = Vec::new();
    stack.push((path.to_path_buf(), node.clone()));

    while let Some((parent_path, node)) = stack.pop() {
        let tree_id = node
            .tree
            .as_ref()
            .ok_or_else(|| anyhow!("Directory node missing tree ID: {}", node.name))?;
        let current_path = parent_path.join(&node.name);

        let mut tree = Tree::load_from_repo(repo, tree_id).await?;
        tree.nodes.sort_unstable_by(|a, b| a.name.cmp(&b.name));

        for n in &tree.nodes {
            entries.push(LsEntry {
                path: current_path.join(&n.name),
                node: n.clone(),
            });
        }

        for n in tree.nodes.into_iter().rev() {
            if n.is_dir() {
                stack.push((current_path.clone(), n));
            }
        }
    }

    Ok(())
}

/// Helper function to print a tree's nodes
fn print_tree(tree: &Tree, args: &CmdArgs) {
    for node in &tree.nodes {
        ui::cli::log!(
            "{}",
            node_to_string(node, None, args.long, args.human_readable)
        )
    }
}

impl Node {
    pub fn new_root(tree_id: &ID) -> Self {
        Self {
            name: String::new(),
            node_type: NodeType::Directory,
            metadata: Metadata::default(),
            blobs: None,
            tree: Some(*tree_id),
            symlink_info: None,
        }
    }
}
