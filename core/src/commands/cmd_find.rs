use std::{
    path::{Path, PathBuf},
    sync::Arc,
};

use anyhow::Result;
use clap::Args;

use crate::{
    backend::{BackendOptions, new_backend_with_prompt},
    commands::{GlobalArgs, cleanup::CleanupHandler},
    fs::{
        node::{Node, node_to_string},
        tree::SerializedNodeStream,
    },
    repository::{
        repo::{RepoConfig, Repository},
        snapshot::{Snapshot, SnapshotStream},
    },
    ui,
    utils::{self, size},
};

#[derive(Args, Debug)]
#[clap(about = "Find files and directories in the repository")]
pub struct CmdArgs {
    /// Path
    #[arg()]
    pub path: PathBuf,
}

pub fn run(global_args: &GlobalArgs, args: &CmdArgs) -> Result<()> {
    let auth = utils::get_auth_from_file(&global_args.auth_file)?;
    let backend = new_backend_with_prompt(BackendOptions {
        repo_path: global_args.repo.clone(),
        ssh_pubkey: global_args.ssh_pubkey.clone(),
        ssh_privatekey: global_args.ssh_privatekey.clone(),
        dry_backend: false,
    })?;

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

    let snapshot_stream = SnapshotStream::new(repo.clone())?;
    for (snapshot_id, snapshot) in snapshot_stream {
        let found_nodes = find_in_snapshot(repo.clone(), &snapshot, &args.path)?;

        if !found_nodes.is_empty() {
            ui::cli::log!("Found in snapshot {}", snapshot_id.to_hex());
            for (path, node) in found_nodes {
                ui::cli::log!("{}", node_to_string(&node, Some(&path), true, true));
            }

            ui::cli::log!();
        }
    }

    Ok(())
}

fn find_in_snapshot(
    repo: Arc<Repository>,
    snapshot: &Snapshot,
    path: &Path,
) -> Result<Vec<(PathBuf, Node)>> {
    let root_tree_id = snapshot.tree;
    let stream = SerializedNodeStream::new(repo, Some(root_tree_id), PathBuf::new(), None, None)?;

    Ok(stream
        .flatten()
        .filter(|(node_path, _stream_node)| node_path.ends_with(path))
        .map(|(path, snode)| (path, snode.node))
        .collect())
}
