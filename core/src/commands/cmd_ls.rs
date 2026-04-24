use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use clap::Args;
use colored::Colorize;

use crate::{
    backend::new_backend_with_prompt,
    commands::{
        GlobalArgs, UseSnapshot, cleanup::CleanupHandler, find_use_snapshot, with_repository_lock,
    },
    fs::{
        node::{Metadata, Node, NodeType, node_to_string},
        tree::{Tree, find_serialized_node},
    },
    mapache::ID,
    repository::repo::Repository,
    ui,
};

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

pub async fn run(global_args: &GlobalArgs, args: &CmdArgs) -> Result<()> {
    with_repository_lock(
        global_args.auth_file.as_ref(),
        global_args.key.as_ref(),
        new_backend_with_prompt(global_args.backend_options(false)).await?,
        global_args.to_repo_config(),
        false,
        global_args.retry_lock_duration,
        |repo, _secure_storage, lock_handle| async move {
            let cleanup_handler = CleanupHandler::new()?;
            cleanup_handler.add_lock(lock_handle.clone());

            repo.reload_master_index().await?;

            let (_snapshot_id, snapshot) = find_use_snapshot(repo.clone(), &args.snapshot)
                .await?
                .context("Snapshot not found")?;

            let node = if let Some(p) = &args.path {
                find_serialized_node(repo.as_ref(), &snapshot.tree, p)
                    .await?
                    .with_context(|| format!("'{}' does not exist in snapshot", p.display()))?
            } else {
                Node::new_root(&snapshot.tree)
            };

            ls(&args.path.clone().unwrap_or_default(), &node, &repo, args).await?;

            Ok(())
        },
    )
    .await
}

/// List the contents of a node.
async fn ls(path: &Path, node: &Node, repo: &Repository, args: &CmdArgs) -> Result<()> {
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

/// List a snapshot tree.
async fn ls_recursive(path: &Path, node: &Node, repo: &Repository, args: &CmdArgs) -> Result<()> {
    let mut stack: Vec<(PathBuf, Node)> = Vec::new();

    if node.is_dir() && args.recursive {
        stack.push((path.to_path_buf(), node.clone()));
    } else if node.is_dir() {
        let mut tree = Tree::load_from_repo(repo, node.tree.as_ref().unwrap()).await?;
        tree.nodes.sort_unstable_by(|a, b| a.name.cmp(&b.name));
        print_tree(&tree, args);
        return Ok(());
    }

    while let Some((parent_path, node)) = stack.pop() {
        let tree_id = node.tree.as_ref().unwrap();
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
