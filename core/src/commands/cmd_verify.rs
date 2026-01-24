use std::time::Instant;

use anyhow::{Result, bail};
use clap::Args;
use colored::Colorize;
use indicatif::{ProgressBar, ProgressState, ProgressStyle};
use rayon::iter::{IntoParallelRefIterator, ParallelIterator};

use crate::mapache::defaults::SHORT_SNAPSHOT_ID_LEN;
use crate::{
    backend::new_backend_with_prompt,
    commands::{GlobalArgs, cleanup::CleanupHandler},
    repository::{
        repo::{RepoConfig, Repository},
        snapshot::SnapshotStream,
        verify::{verify_pack, verify_snapshot_refs},
    },
    ui::{self, default_bar_draw_target},
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
    /// Read all packs and discover unreferenced blobs
    #[clap(long = "read-packs", value_parser, default_value_t = false)]
    pub read_packs: bool,
}

pub fn run(global_args: &GlobalArgs, args: &CmdArgs) -> Result<()> {
    let auth = utils::get_auth_from_file(&global_args.auth_file)?;

    let backend_options = global_args.backend_options(false);
    let backend_arc = new_backend_with_prompt(backend_options)?;

    let config = RepoConfig {
        pack_size: (global_args.pack_size_mib * size::MiB as f32) as u64,
        use_cache: !global_args.no_cache,
        compression: global_args.compression_level,
    };
    let (repo_arc, secure_storage, lock_handle) = Repository::try_open_with_lock(
        auth.as_ref(),
        global_args.key.as_ref(),
        backend_arc.clone(),
        config,
        false,
        global_args.retry_lock_duration,
    )?;

    let lock_handle_clone = lock_handle.clone();
    let _cleanup_handler = CleanupHandler::new(move || {
        lock_handle_clone.write().unlock();
    })?;

    let start = Instant::now();

    let snapshot_stream = SnapshotStream::new(repo_arc.clone())?;

    if args.read_packs {
        let packs = repo_arc.list_packs()?;

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

        let repo_ref = repo_arc.clone();
        let backend_ref = backend_arc.clone();
        let secure_storage_ref = secure_storage.clone();

        let num_dangling_blobs: usize = packs
            .par_iter()
            .map(|pack_id| {
                let verify_res = verify_pack(
                    repo_ref.as_ref(),
                    backend_ref.as_ref(),
                    secure_storage_ref.as_ref(),
                    &pack_id,
                );

                bar.inc(1);

                verify_res.unwrap_or_else(|e| {
                    ui::cli::error!("Error verifying pack {}: {}", pack_id.to_short_hex(8), e);
                    0 // Return 0 dangling on error
                })
            })
            .sum();

        bar.finish_and_clear();

        if num_dangling_blobs > 0 {
            ui::cli::log!("Found {} unreferenced blobs", num_dangling_blobs);
        }

        ui::cli::log!();
    }

    let num_snapshots = snapshot_stream.len();
    let mut ok_counter = 0;
    let mut error_counter = 0;

    for (i, (snapshot_id, _snapshot)) in snapshot_stream.enumerate() {
        ui::cli::log!(
            "Verifying snapshot {}  ({} / {})",
            snapshot_id
                .to_short_hex(SHORT_SNAPSHOT_ID_LEN)
                .bold()
                .yellow(),
            i + 1,
            num_snapshots
        );

        match verify_snapshot_refs(repo_arc.clone(), &snapshot_id) {
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
        bail!(
            "Verify failed after {}",
            utils::pretty_print_duration(start.elapsed())
        )
    } else {
        ui::cli::log!();
        ui::cli::log!(
            "Finished in {}",
            utils::pretty_print_duration(start.elapsed())
        );

        Ok(())
    }
}
