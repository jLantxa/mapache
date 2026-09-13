use std::{
    io,
    path::{Path, PathBuf},
    sync::atomic::{AtomicU64, AtomicUsize, Ordering},
};

use clap::Args;
use rayon::prelude::*;

use crate::{
    backend::cache::CacheBackend,
    commands::ToExitCode,
    common::{defaults::SHORT_REPO_ID_LEN, error::MapacheError},
    ui::{
        self,
        cli::{
            color::Colorize,
            table::{Alignment, Table},
        },
    },
    utils,
};

#[derive(Debug, thiserror::Error)]
pub enum CacheError {
    #[error(transparent)]
    Repo(#[from] MapacheError),
    #[error(transparent)]
    Io(#[from] io::Error),
}

impl ToExitCode for CacheError {
    fn to_exit_code(&self) -> i32 {
        match self {
            CacheError::Repo(_) => 1,
            CacheError::Io(_) => 4,
        }
    }
}

#[derive(Args, Debug)]
#[clap(
    about = "List and cleanup cache directories",
    long_about = "List the local cache directories created by mapache (used to stage\n\
        data before upload), or delete specific ones.\n\n\
        Without options, prints the cache folders and their sizes. Use --delete\n\
        with one or more cache identifiers to remove only those, or --clear to\n\
        remove all cached data."
)]
pub struct CmdArgs {
    /// List of cache folder prefixes to delete
    #[clap(long = "delete", num_args = 1..)]
    pub delete_ids: Option<Vec<String>>,

    /// Delete all cache folders
    #[clap(long, conflicts_with = "delete_ids")]
    pub clear: bool,
}

pub fn run(args: &CmdArgs) -> Result<(), CacheError> {
    let cache_base = CacheBackend::default_dir();

    if let Some(list) = &args.delete_ids {
        cleanup(&cache_base, list)
    } else if args.clear {
        cleanup(&cache_base, &[])
    } else {
        list(&cache_base)
    }
}

/// Counts the number of regular files stored under `path`, recursively.
fn count_files_in_dir(path: &Path) -> u64 {
    let mut count = 0;
    let mut pending = vec![path.to_path_buf()];
    while let Some(dir) = pending.pop() {
        let Ok(entries) = std::fs::read_dir(&dir) else {
            continue;
        };
        for entry in entries.flatten() {
            let Ok(metadata) = entry.metadata() else {
                continue;
            };
            if metadata.is_dir() {
                pending.push(entry.path());
            } else if metadata.is_file() {
                count += 1;
            }
        }
    }
    count
}

/// List all cache folders.
fn list(cache_base: &Path) -> Result<(), CacheError> {
    if !cache_base.exists() {
        ui::cli::warning!(
            "Cache base directory does not exist: {}",
            cache_base.display()
        );
        return Ok(());
    }

    let mut folders: Vec<(String, PathBuf)> = std::fs::read_dir(cache_base)?
        .filter_map(|entry| {
            let entry = entry.ok()?;
            let path = entry.path();
            if !path.is_dir() {
                return None;
            }
            let name = path
                .file_name()
                .unwrap_or_default()
                .to_string_lossy()
                .to_string();
            Some((name, path))
        })
        .collect();

    folders.sort_by(|a, b| a.0.cmp(&b.0));

    if folders.is_empty() {
        ui::cli::log!(
            "{}",
            format!("No repo caches found in {}", cache_base.display()).bold()
        );
        return Ok(());
    }

    ui::cli::log!(
        "{}",
        format!("Repo caches in {}:", cache_base.display())
            .bold()
            .cyan()
    );
    ui::cli::log!();

    let mut table = Table::new_with_alignments(vec![
        Alignment::Left,
        Alignment::Right,
        Alignment::Right,
        Alignment::Right,
    ]);
    table.set_padding(0);
    table.set_headers(vec![
        "Repo ID".bold().yellow().to_string(),
        "Files".bold().yellow().to_string(),
        "Size".bold().yellow().to_string(),
        "Modified".bold().yellow().to_string(),
    ]);

    let mut num_directories = 0;
    let mut total_cache_size = 0;

    for (name, path) in &folders {
        let size = match utils::dir_size(path) {
            Ok(size) => size,
            Err(e) => {
                ui::cli::warning!("Error calculating size for {}: {}", path.display(), e);
                continue;
            }
        };

        let modified = path
            .metadata()
            .ok()
            .and_then(|m| m.modified().ok())
            .map(|t| utils::pretty_print_system_time(t, Some("%Y-%m-%d %H:%M")).unwrap_or_default())
            .unwrap_or_default();

        table.add_row(vec![
            name.get(0..2 * SHORT_REPO_ID_LEN)
                .unwrap_or(name)
                .bold()
                .yellow()
                .to_string(),
            count_files_in_dir(path).to_string(),
            utils::format_size_binary(size, 3),
            modified.dimmed().to_string(),
        ]);
        num_directories += 1;
        total_cache_size += size;
    }

    ui::cli::log!("{}", table.render());

    ui::cli::log!();
    ui::cli::log!(
        "{} ({}) in {}",
        utils::format_count(num_directories, "directory", "directories").bold(),
        utils::format_size_binary(total_cache_size, 3)
            .bold()
            .green(),
        cache_base.display().to_string().dimmed()
    );

    Ok(())
}

/// Deletes cache folders by prefix.
fn cleanup(cache_base: &Path, folder_prefixes: &[String]) -> Result<(), CacheError> {
    tracing::info!(target: "cache", "Starting cache cleanup (base={:?})", cache_base);
    if !cache_base.exists() {
        ui::cli::warning!(
            "Cache base directory does not exist: {}",
            cache_base.display()
        );
        return Ok(());
    }

    let delete_all = folder_prefixes.is_empty();
    let entries: Vec<(String, PathBuf)> = std::fs::read_dir(cache_base)?
        .filter_map(|entry| {
            let e = match entry {
                Ok(e) => e,
                Err(err) => {
                    tracing::warn!(target: "cache", "Error reading cache entry: {err}");
                    return None;
                }
            };
            let p = e.path();
            let n = match p.file_name().and_then(|s| s.to_str()) {
                Some(n) => n.to_owned(),
                None => return None,
            };
            p.is_dir().then_some((n, p))
        })
        .collect();

    // Select folders to delete
    let to_delete: Vec<_> = if delete_all {
        entries.iter().map(|(_, p)| p.clone()).collect()
    } else {
        let mut matches = Vec::new();

        for prefix in folder_prefixes {
            let matched: Vec<_> = entries
                .iter()
                .filter(|(n, _)| n.starts_with(prefix))
                .collect();

            match matched.as_slice() {
                [] => ui::cli::warning!("No cache folder found for prefix: {}", prefix.cyan()),
                [(_, p)] => matches.push(p.clone()),
                m => {
                    let names = m
                        .iter()
                        .map(|(n, _)| n.as_str())
                        .collect::<Vec<_>>()
                        .join(", ");
                    return Err(CacheError::Io(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        format!(
                            "Ambiguous prefix '{}' matches multiple folders: {}",
                            prefix.cyan(),
                            names
                        ),
                    )));
                }
            }
        }

        matches.sort();
        matches.dedup();
        matches
    };

    if to_delete.is_empty() {
        ui::cli::log!("No cache folders to delete.");
        return Ok(());
    }

    let total = to_delete.len();
    ui::cli::log!(
        "{} {} in {}",
        "Deleting".bold().cyan(),
        utils::format_count(total, "repo cache", "repo caches")
            .bold()
            .cyan(),
        cache_base.display().to_string().dimmed()
    );
    ui::cli::log!();

    // Parallel deletion
    let num_deleted = AtomicUsize::new(0);
    let freed = AtomicU64::new(0);
    let done = AtomicUsize::new(0);
    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(4)
        .build()
        .map_err(|e| CacheError::Io(io::Error::other(e)))?;
    pool.install(|| {
        to_delete.par_iter().for_each(|path| {
            let name = path
                .file_name()
                .and_then(|n| n.to_str())
                .unwrap_or_default();
            let size = utils::dir_size(path).unwrap_or(0);
            let n = done.fetch_add(1, Ordering::Relaxed) + 1;

            tracing::info!(target: "cache", "Deleting cache directory {:?}", path);
            match std::fs::remove_dir_all(path) {
                Ok(_) => {
                    num_deleted.fetch_add(1, Ordering::Relaxed);
                    freed.fetch_add(size, Ordering::Relaxed);
                    ui::cli::log!(
                        "  [{}/{}] {} {} ({})",
                        n,
                        total,
                        "DELETED".bright_red().bold(),
                        name.cyan(),
                        utils::format_size_binary(size, 3).dimmed()
                    );
                }
                Err(e) => ui::cli::warning!("Failed to delete {}: {}", path.display(), e),
            }
        });
    });

    let num_deleted = num_deleted.load(Ordering::Relaxed);
    let freed = freed.load(Ordering::Relaxed);
    let failed = total.saturating_sub(num_deleted);

    if num_deleted > 0 {
        ui::cli::log!(
            "\n{} {} ({}) freed.",
            "[SUCCESS]".bold().green(),
            utils::format_count(num_deleted, "repo cache", "repo caches").bold(),
            utils::format_size_binary(freed, 3).bold().green()
        );
    }
    if failed > 0 {
        ui::cli::warning!("{}/{} repo caches failed to delete.", failed, total);
    }

    tracing::info!(target: "cache", "Cache cleanup finished (freed {})", utils::format_size_binary(freed, 3));

    Ok(())
}
