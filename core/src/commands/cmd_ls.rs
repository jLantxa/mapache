use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use clap::Args;

use crate::{
    backend::new_backend_with_prompt,
    commands::{GlobalArgs, UseSnapshot, cleanup::CleanupHandler, find_use_snapshot},
    fs::{
        node::{Metadata, Node, NodeType, node_to_string},
        tree::{Tree, find_serialized_node},
    },
    mapache::ID,
    repository::repo::{RepoConfig, Repository},
    ui,
    utils::{self, size},
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

pub fn run(global_args: &GlobalArgs, args: &CmdArgs) -> Result<()> {
    let auth = utils::get_auth_from_file(&global_args.auth_file)?;
    let backend = new_backend_with_prompt(global_args.backend_options(false))?;

    let config = RepoConfig {
        pack_size: (global_args.pack_size_mib * size::MiB as f32) as u64,
        use_cache: !global_args.no_cache,
    };
    let (repo, _, lock_handle) = Repository::try_open_with_lock(
        auth.as_ref(),
        global_args.key.as_ref(),
        backend,
        config,
        false,
    )?;

    let lock_handle_clone = lock_handle.clone();
    let _cleanup_handler = CleanupHandler::new(move || {
        lock_handle_clone.write().unlock();
    })?;

    let (_snapshot_id, snapshot) =
        find_use_snapshot(repo.clone(), &args.snapshot)?.context("Snapshot not found")?;

    let node = if let Some(p) = &args.path {
        find_serialized_node(repo.as_ref(), &snapshot.tree, p)?
            .with_context(|| format!("'{}' does not exist in snapshot", p.display()))?
    } else {
        Node::new_root(&snapshot.tree)
    };

    ls(&args.path.clone().unwrap_or_default(), &node, &repo, args)?;

    Ok(())
}

/// List the contents of a node.
fn ls(path: &Path, node: &Node, repo: &Repository, args: &CmdArgs) -> Result<()> {
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

    ls_recursive(path, node, repo, args)
}

/// List a snapshot tree.
fn ls_recursive(path: &Path, node: &Node, repo: &Repository, args: &CmdArgs) -> Result<()> {
    let mut stack: Vec<(PathBuf, Node)> = Vec::new();

    if node.is_dir() && args.recursive {
        stack.push((path.to_path_buf(), node.clone()));
    } else if node.is_dir() {
        let mut tree = Tree::load_from_repo(repo, node.tree.as_ref().unwrap())?;
        tree.nodes.sort_unstable_by(|a, b| a.name.cmp(&b.name));
        print_tree(&tree, args);
        return Ok(());
    }

    while let Some((parent_path, node)) = stack.pop() {
        let tree_id = node.tree.as_ref().unwrap();
        let current_path = parent_path.join(&node.name);

        let mut tree = Tree::load_from_repo(repo, tree_id)?;

        if args.recursive {
            ui::cli::log!();
            ui::cli::log!("{}:", current_path.display());
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
