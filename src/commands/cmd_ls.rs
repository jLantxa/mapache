// mapache is an incremental backup tool
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

use anyhow::{Result, bail};
use clap::Args;
use colored::Colorize;

use crate::{
    backend::new_backend_with_prompt,
    commands::{GlobalArgs, UseSnapshot, find_use_snapshot},
    repository::{
        self, RepositoryBackend,
        streamers::find_serialized_node,
        tree::{Metadata, Node, NodeType, Tree},
    },
    utils,
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
    let pass = utils::get_password_from_file(&global_args.password_file)?;
    let backend = new_backend_with_prompt(global_args, false)?;
    let (repo, _) = repository::try_open(pass, global_args.key.as_ref(), backend)?;

    let (_snapshot_id, snapshot) = {
        match find_use_snapshot(repo.clone(), &args.snapshot) {
            Ok(Some((id, snap))) => (id, snap),
            Ok(None) | Err(_) => bail!("Snapshot not found"),
        }
    };

    let node: Node = match &args.path {
        Some(p) => match find_serialized_node(repo.as_ref(), &snapshot.tree, p)? {
            Some(n) => n,
            None => bail!("{} does not exist in snapshot", p.display()),
        },
        // Create a dummy node for the snapshot root, as this tree has no node referencing it.
        None => Node {
            name: String::from("/"),
            node_type: NodeType::Directory,
            metadata: Metadata::default(),
            blobs: None,
            tree: Some(snapshot.tree.clone()),
            symlink_info: None,
        },
    };

    ls(
        args.path.clone().unwrap_or_default(),
        node,
        repo.as_ref(),
        args,
    )
}

/// List the contents of a node.
fn ls(path: PathBuf, node: Node, repo: &dyn RepositoryBackend, args: &CmdArgs) -> Result<()> {
    if !node.is_dir() {
        println!("{}", node_to_string(&node, args.long, args.human_readable));
    } else {
        if args.recursive {
            println!("{}:", path.display());
        }

        let tree_id = node.tree;
        if let Some(id) = tree_id {
            let tree = Tree::load_from_repo(repo, &id)?;
            ls_tree(path, tree, repo, args)?;
        }
    }

    Ok(())
}

/// List a snapshot tree.
fn ls_tree(
    path: PathBuf, // 'path' here is the initial path
    mut tree: Tree,
    repo: &dyn RepositoryBackend,
    args: &CmdArgs,
) -> Result<()> {
    tree.nodes.sort_by_key(|node| node.name.to_lowercase());
    for node in &tree.nodes {
        println!("{}", node_to_string(node, args.long, args.human_readable))
    }

    if args.recursive {
        let mut stack: Vec<(PathBuf, Node)> = Vec::new();
        for node in tree.nodes.into_iter().rev() {
            if node.tree.is_some() {
                stack.push((path.clone(), node));
            }
        }

        while let Some((parent_path, node)) = stack.pop() {
            if let Some(tree_id) = node.tree {
                let current_path = parent_path.join(&node.name);

                let mut tree = Tree::load_from_repo(repo, &tree_id)?;
                tree.nodes.sort_by_key(|node| node.name.to_lowercase());

                println!();

                if args.recursive {
                    println!("{}:", current_path.display());
                }
                for node in &tree.nodes {
                    println!("{}", node_to_string(node, args.long, args.human_readable))
                }
                for node in tree.nodes.into_iter().rev() {
                    if node.tree.is_some() {
                        stack.push((current_path.clone(), node));
                    }
                }
            }
        }
    }

    Ok(())
}

/// Prints the relevant metadata of a node as a single line, similar to the Unix ls command.
fn node_to_string(node: &Node, long: bool, human_readable: bool) -> String {
    let node_name_str = get_colorized_node_name(node);

    if long {
        let size_str = match human_readable {
            true => utils::format_size(node.metadata.size, 3),
            false => node.metadata.size.to_string(),
        };

        const NA: &str = "_";

        format!(
            "{:10} {:3} {:7}  {:7}  {:>14}  {:12}  {}",
            node.metadata.mode.map_or(NA.to_string(), |mode| {
                utils::mode_to_permissions_string(mode)
            }),
            node.metadata
                .nlink
                .map_or(NA.to_string(), |nlink| nlink.to_string()),
            node.metadata
                .owner_uid
                .map_or(NA.to_string(), |uid| uid.to_string()),
            node.metadata
                .owner_gid
                .map_or(NA.to_string(), |gid| gid.to_string()),
            size_str,
            node.metadata.modified_time.map_or(NA.to_string(), |mtime| {
                utils::pretty_print_system_time(mtime, None).unwrap_or(String::from("Error"))
            }),
            node_name_str
        )
    } else {
        node_name_str.to_string()
    }
}

/// Returns a colorized node name.
/// This function follows the color code convention of ls, but it is not comprehensive.
fn get_colorized_node_name(node: &Node) -> String {
    if node.is_dir() {
        format!("{}", node.name.bold().blue())
    } else if node.is_symlink() {
        match &node.symlink_info {
            None => format!("{}", node.name.cyan()),
            Some(symlink_info) => format!(
                "{} -> {}",
                node.name.cyan(),
                symlink_info.target_path.display()
            ),
        }
    } else if node.is_block_device() || node.is_char_device() {
        format!("{}", node.name.yellow().on_black())
    } else {
        node.name.clone()
    }
}
