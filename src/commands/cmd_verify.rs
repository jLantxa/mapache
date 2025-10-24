use std::{collections::BTreeSet, path::PathBuf, sync::Arc, time::Instant};

use anyhow::{Result, bail};
use clap::Args;
use colored::Colorize;
use indicatif::{MultiProgress, ProgressBar, ProgressState, ProgressStyle};
use parking_lot::Mutex;
use rayon::iter::{IntoParallelRefIterator, ParallelIterator};

use crate::{
    backend::{BackendOptions, Handle, StorageBackend, new_backend_with_prompt},
    commands::{GlobalArgs, cleanup::CleanupHandler},
    fs::{node::NodeType, tree::SerializedNodeStreamer},
    mapache::{
        FileType, ID,
        defaults::{MAX_PATH_DISPLAY_LEN, SHORT_SNAPSHOT_ID_LEN},
        global::GlobalOpts,
    },
    repository::{
        repo::{RepoConfig, Repository},
        snapshot::SnapshotStreamer,
        verify::{verify_blob, verify_pack, verify_snapshot_links},
    },
    ui::{self, SPINNER_TICK_CHARS, default_bar_draw_target},
    utils::{self, size},
};

#[derive(Args, Debug)]
#[clap(
    about = "Verify the integrity of the data stored in the repository",
    long_about = "Verify the integrity of the data stored in the repository, ensuring that all data\
                  associated to a any active snapshots are valid and reachable. This guarantees\
                  that any active snapshot can be restored."
)]
pub struct CmdArgs {
    /// Simulate a restore, reading and checking actual data from the repository.
    #[clap(
        short = 's',
        long = "simulate-restore",
        value_parser,
        default_value_t = false
    )]
    pub simulate_restore: bool,

    /// Read all packs and discover unreferenced blobs
    #[clap(short = 'a', long = "all-packs", value_parser, default_value_t = false)]
    pub all_packs: bool,
}

pub fn run(global_args: &GlobalArgs, args: &CmdArgs) -> Result<()> {
    let auth = utils::get_auth_from_file(&global_args.auth_file)?;

    let backend_options = BackendOptions {
        repo_path: global_args.repo.clone(),
        ssh_pubkey: global_args.ssh_pubkey.clone(),
        ssh_privatekey: global_args.ssh_privatekey.clone(),
        dry_backend: false,
        cached: false,
    };
    let backend_arc = new_backend_with_prompt(backend_options)?;

    let config = RepoConfig {
        pack_size: (global_args.pack_size_mib * size::MiB as f32) as u64,
        use_cache: false,
    };
    let (repo_arc, secure_storage, lock_handle) = Repository::try_open_with_lock(
        auth.as_ref(),
        global_args.key.as_ref(),
        backend_arc.clone(),
        config,
        false,
    )?;

    let lock_handle_clone = lock_handle.clone();
    let _cleanup_handler = CleanupHandler::new(move || {
        lock_handle_clone.write().unlock();
    })?;

    let start = Instant::now();

    let snapshot_streamer = SnapshotStreamer::new(repo_arc.clone())?;

    let mut visited_blobs = BTreeSet::new();

    if args.all_packs {
        let packs = repo_arc.list_objects()?;

        let style = ProgressStyle::default_bar()
            .template(
                "[{custom_elapsed}] [{bar:20.cyan/white}] Reading packs: {pos} / {len}  [ETA: {custom_eta}]",
            )
            .unwrap()
            .progress_chars("=> ")
            .with_key(
                "custom_elapsed",
                move |state: &ProgressState, w: &mut dyn std::fmt::Write| {
                    let elapsed = state.elapsed();
                    let custom_elapsed = utils::pretty_print_duration(elapsed);
                    let _ = w.write_str(&custom_elapsed);
                },
            )
            .with_key(
                "custom_eta",
                move |state: &ProgressState, w: &mut dyn std::fmt::Write| {
                    let eta = state.eta();
                    let custom_eta = utils::pretty_print_duration(eta);
                    let _ = w.write_str(&custom_eta);
                },
            );

        let bar = ProgressBar::new(packs.len() as u64);
        bar.set_draw_target(default_bar_draw_target());
        bar.set_style(style);

        let visited_blobs_mutex = Mutex::new(visited_blobs);
        let repo_ref = repo_arc.clone();
        let backend_ref = backend_arc.clone();
        let secure_storage_ref = secure_storage.clone();

        let num_dangling_blobs: usize = packs
            .par_iter()
            .map(|pack_id| {
                let mut visited_guard = visited_blobs_mutex.lock();

                let verify_res = verify_pack(
                    repo_ref.as_ref(),
                    backend_ref.as_ref(),
                    secure_storage_ref.as_ref(),
                    pack_id,
                    &mut visited_guard,
                );

                drop(visited_guard); // Release lock early

                bar.inc(1);

                verify_res.unwrap_or_else(|e| {
                    ui::cli::error!("Error verifying pack {}: {}", pack_id.to_short_hex(8), e);
                    0 // Return 0 dangling on error
                })
            })
            .sum();

        visited_blobs = visited_blobs_mutex.into_inner();

        bar.finish_and_clear();
        ui::cli::log!(
            "Verified {} blobs from {} packs",
            visited_blobs.len(),
            packs.len()
        );
        if num_dangling_blobs > 0 {
            ui::cli::log!("Found {} unreferenced blobs", num_dangling_blobs);
        }

        ui::cli::log!();
    }

    let num_snapshots = snapshot_streamer.len();
    let mut ok_counter = 0;
    let mut error_counter = 0;

    for (i, (snapshot_id, _snapshot)) in snapshot_streamer.enumerate() {
        ui::cli::log!(
            "Verifying snapshot {}  ({} / {})",
            snapshot_id
                .to_short_hex(SHORT_SNAPSHOT_ID_LEN)
                .bold()
                .yellow(),
            i + 1,
            num_snapshots
        );

        let res = if args.simulate_restore {
            verify_snapshot(
                repo_arc.clone(),
                backend_arc.clone(),
                &snapshot_id,
                &mut visited_blobs,
            )
        } else {
            verify_snapshot_links(repo_arc.clone(), &snapshot_id)
        };

        match res {
            Ok(_) => {
                ui::cli::log!("{}", "[OK]".bold().green());
                ok_counter += 1;
            }
            Err(e) => {
                ui::cli::log!("{} {}", "[ERROR]".bold().red(), e.to_string());
                error_counter += 1
            }
        }
    }

    ui::cli::log!();
    ui::cli::log!(
        "{} verified",
        utils::format_count(num_snapshots, "snapshot", "snapshots"),
    );
    if ok_counter > 0 {
        ui::cli::log!("{} {}", ok_counter, "[OK]".bold().green());
    }
    if error_counter > 0 {
        ui::cli::log!("{} {}", error_counter, "[ERROR]".bold().red());
    }

    ui::cli::log!();
    ui::cli::log!(
        "Finished in {}",
        utils::pretty_print_duration(start.elapsed())
    );

    Ok(())
}

/// Verify the checksum and contents of a snapshot with a known ID in the repository.
/// This function will verify the checksum of the Snapshot object and the contents of all blobs
/// referenced by it. It is a simulation of a restore.
pub fn verify_snapshot(
    repo: Arc<Repository>,
    backend: Arc<dyn StorageBackend>,
    snapshot_id: &ID,
    visited_blobs: &mut BTreeSet<ID>,
) -> Result<()> {
    let snapshot_path = repo.get_path(FileType::Snapshot, snapshot_id);
    let snapshot_data = backend.read(&Handle::new(&snapshot_path), 0, 0)?;
    let checksum = utils::calculate_hash(snapshot_data);
    if checksum != snapshot_id.0[..] {
        bail!("Invalid snapshot checksum");
    }

    let snapshot = repo.load_snapshot(snapshot_id)?;
    let tree_id = snapshot.tree;
    let streamer =
        SerializedNodeStreamer::new(repo.clone(), Some(tree_id), PathBuf::new(), None, None)?;

    let mp = MultiProgress::with_draw_target(default_bar_draw_target());
    let bar = mp.add(ProgressBar::new(snapshot.size()));
    let bar_style = ProgressStyle::default_bar()
        .template("[{percent} %] [{bar:20.cyan/white}] [{custom_elapsed}]  {processed_bytes_formated}  [ETA: {custom_eta}]")
        .unwrap()
        .progress_chars("=> ")
        .with_key(
            "custom_elapsed",
            move |state: &ProgressState, w: &mut dyn std::fmt::Write| {
                let elapsed = state.elapsed();
                let custom_elapsed = utils::pretty_print_duration(elapsed);
                let _ = w.write_str(&custom_elapsed);
            },
        )
        .with_key(
            "processed_bytes_formated",
            move |state: &ProgressState, w: &mut dyn std::fmt::Write| {
                let s = format!(
                    "{} / {}",
                    utils::format_size(state.pos(), 3),
                    utils::format_size(state.len().unwrap(), 3)
                );
                let _ = w.write_str(&s);
            },
        )
        .with_key(
            "custom_eta",
            move |state: &ProgressState, w: &mut dyn std::fmt::Write| {
                let eta = state.eta();
                let custom_eta = utils::pretty_print_duration(eta);
                let _ = w.write_str(&custom_eta);
            },
        );

    bar.set_style(bar_style);

    let spinner = mp.add(ProgressBar::new_spinner());
    spinner.set_style(
        ProgressStyle::default_spinner()
            .template("{spinner:.cyan} {msg}")
            .unwrap()
            .tick_chars(SPINNER_TICK_CHARS),
    );
    spinner.enable_steady_tick(GlobalOpts::progress_refresh_interval());

    bar.set_position(0);

    let index = repo.index();
    let index_guard = index.read();

    for (path, stream_node) in streamer.flatten() {
        spinner.set_message(utils::abbreviate_path(&path, MAX_PATH_DISPLAY_LEN));

        let node = stream_node.node;
        match node.node_type {
            NodeType::File => {
                if let Some(blobs) = node.blobs {
                    for blob in blobs {
                        if !visited_blobs.contains(&blob) {
                            match verify_blob(repo.as_ref(), &blob) {
                                Ok((raw_length, _encoded_length)) => {
                                    visited_blobs.insert(blob);
                                    bar.inc(raw_length);
                                }
                                Err(_) => {
                                    let _ = mp.clear();
                                    bail!("Snapshot has corrupt blobs");
                                }
                            }
                        } else {
                            let (_, _, _, raw_length, _) = index_guard
                                .get(&blob)
                                .expect("We visited this blob, so it should be indexed");
                            bar.inc(raw_length as u64);
                        }
                    }
                }
            }
            NodeType::Symlink
            | NodeType::Directory
            | NodeType::BlockDevice
            | NodeType::CharDevice
            | NodeType::Fifo
            | NodeType::Socket => (),
        }
    }

    let _ = mp.clear();

    Ok(())
}
