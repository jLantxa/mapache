use std::{
    collections::BTreeSet,
    io,
    path::PathBuf,
    str::FromStr,
    sync::{Arc, atomic::AtomicBool},
    time::Instant,
};

use clap::{ArgGroup, Args};
use serde::{Deserialize, Serialize};

use crate::{
    archiver::{self, SnapshotOptions, progress::SnapshotProcessSummary},
    backend::{StorageHint, new_backend_with_prompt},
    commands::{
        EMPTY_TAG_MARK, GlobalArgs, Merge, ToExitCode, UseSnapshot, cleanup::CleanupHandler,
        find_use_snapshot, merge_opt, parse_tags, with_repository_lock,
    },
    common::{
        self, ContentIdType, ID, config,
        defaults::{DEFAULT_SNAPSHOT_READERS, SHORT_SNAPSHOT_ID_LEN},
        error::MapacheError,
        hooks,
        vars::{PASSWORD_ENVVAR, USERNAME_ENVVAR, get_envvar},
    },
    fs::{
        self, calculate_lcp,
        filter::{
            PathFilter, merge_filtered_paths, normalized_exclude_paths,
            read_filtered_paths_from_file,
        },
    },
    repository::{
        lock::LockHandle,
        repo::Repository,
        snapshot::{SnapshotPair, SnapshotSummary},
    },
    ui::{
        self,
        cli::{
            color::Colorize,
            snapshot,
            table::{Alignment, Table},
        },
        events::{BackupEvent, Event, EventSender},
        json::snapshot as json_snapshot,
    },
    utils,
};

#[derive(Debug, thiserror::Error)]
pub enum SnapshotError {
    #[error("backend error: {0}")]
    BackendError(String),
    #[error("failed to open repository: {0}")]
    RepoOpenFail(String),
    #[error("invalid source path: {0}")]
    SourcePathError(String),
    #[error("parent snapshot not found: {0}")]
    ParentNotFound(String),
    #[error("snapshot creation failed: {0}")]
    SnapshotFailed(String),
    #[error("snapshot interrupted by user")]
    Interrupted,
    #[error(transparent)]
    Repo(#[from] MapacheError),
    #[error(transparent)]
    Io(#[from] io::Error),
}

impl ToExitCode for SnapshotError {
    fn to_exit_code(&self) -> i32 {
        match self {
            SnapshotError::BackendError(_) => 11,
            SnapshotError::RepoOpenFail(_) => 12,
            SnapshotError::SourcePathError(_) => 20,
            SnapshotError::ParentNotFound(_) => 21,
            SnapshotError::SnapshotFailed(_) => 30,
            SnapshotError::Interrupted => 130,
            SnapshotError::Repo(_) => 1,
            SnapshotError::Io(_) => 1,
        }
    }
}

#[derive(Args, Debug, Clone, Serialize, Deserialize, Default)]
#[clap(group = ArgGroup::new("scan_mode").multiple(false))]
#[clap(about = "Create a new snapshot")]
#[serde(default, rename_all = "kebab-case")]
pub struct CmdArgs {
    /// List of paths to backup
    #[clap(value_parser)]
    #[serde(deserialize_with = "crate::common::config::deserialize_config_paths_vec")]
    pub paths: Vec<PathBuf>,

    /// Use a single directory path as the snapshot root
    #[clap(long = "as-root", value_parser, action = clap::ArgAction::Set, num_args = 0..=1, default_missing_value = "true")]
    pub as_root: Option<bool>,

    /// A list of paths to exclude: path[,path,...]. Can be used multiple times.
    #[clap(long, value_parser, value_delimiter = ',', num_args = 1..)]
    #[serde(deserialize_with = "crate::common::config::deserialize_config_string_vec_opt")]
    pub exclude: Option<Vec<String>>,

    /// A file containing a list of paths to exclude, one per line.
    #[clap(long, value_parser)]
    #[serde(deserialize_with = "crate::common::config::deserialize_config_path_opt")]
    pub exclude_file: Option<PathBuf>,

    /// Tags
    #[clap(long = "tags", value_parser)]
    pub tags_str: Option<String>,

    /// Snapshot description
    #[clap(long, value_parser)]
    pub description: Option<String>,

    /// Force a complete analysis of all files and directories
    #[clap(long, group = "scan_mode")]
    pub no_parent: bool,

    /// Don't scan the file system
    #[clap(long, action = clap::ArgAction::Set, num_args = 0..=1, default_missing_value = "true")]
    pub no_scan: Option<bool>,

    /// Don't create a snapshot if there are no changes since the parent snapshot
    #[clap(long, action = clap::ArgAction::Set, num_args = 0..=1, default_missing_value = "true")]
    pub skip_if_unchanged: Option<bool>,

    /// Use a snapshot as parent (ID or 'latest'). This snapshot will be the base when analyzing differences.
    #[clap(long, group = "scan_mode", value_parser = clap::value_parser!(UseSnapshot))]
    #[serde(deserialize_with = "deserialize_use_snapshot_opt")]
    pub parent: Option<UseSnapshot>,

    /// Number of files to process in parallel.
    #[clap(long = "readers")]
    pub num_readers: Option<usize>,

    /// Number of writer threads.
    #[clap(long = "packers")]
    pub num_packers: Option<usize>,

    /// Dry run
    #[clap(long)]
    pub dry_run: bool,

    /// Store the access time for all files and directories.
    /// Enabling this may result in significantly more metadata, so it's off by default.
    #[clap(long, action = clap::ArgAction::Set, num_args = 0..=1, default_missing_value = "true")]
    pub with_atime: Option<bool>,

    /// Read backup data from stdin as a single file at /stdin.
    /// Mutually exclusive with paths, excludes, and parent snapshot.
    /// Requires --auth-file or MAPACHE_USERNAME/MAPACHE_PASSWORD env vars.
    #[clap(long, conflicts_with_all = &["paths", "exclude", "exclude_file", "parent", "as_root"])]
    pub stdin: bool,
}

impl CmdArgs {
    pub(crate) fn template() -> Self {
        Self {
            paths: vec![PathBuf::from("/home/user/Documents")],
            as_root: Some(false),
            exclude: Some(vec!["**/node_modules".to_string(), "**/.git".to_string()]),
            exclude_file: None,
            tags_str: Some("work,important".to_string()),
            description: Some("Daily backup".to_string()),
            no_parent: false,
            no_scan: Some(false),
            skip_if_unchanged: Some(false),
            parent: Some(crate::commands::UseSnapshot::Latest),
            num_readers: Some(crate::common::defaults::DEFAULT_SNAPSHOT_READERS),
            num_packers: Some(crate::common::defaults::DEFAULT_SNAPSHOT_PACKERS),
            dry_run: false,
            with_atime: Some(false),
            stdin: false,
        }
    }
}

impl Merge for CmdArgs {
    fn merge(&mut self, other: Self) {
        if !other.paths.is_empty() {
            self.paths = other.paths;
        }

        merge_opt(&mut self.as_root, other.as_root);

        config::merge_option_vec(&mut self.exclude, other.exclude);

        merge_opt(&mut self.exclude_file, other.exclude_file);
        merge_opt(&mut self.tags_str, other.tags_str);
        merge_opt(&mut self.description, other.description);

        // skip: no_parent

        merge_opt(&mut self.no_scan, other.no_scan);
        merge_opt(&mut self.skip_if_unchanged, other.skip_if_unchanged);
        merge_opt(&mut self.parent, other.parent);
        merge_opt(&mut self.num_readers, other.num_readers);
        merge_opt(&mut self.num_packers, other.num_packers);

        // skip: dry_run

        merge_opt(&mut self.with_atime, other.with_atime);

        // skip: stdin
    }
}

fn deserialize_use_snapshot_opt<'de, D>(
    deserializer: D,
) -> std::result::Result<Option<UseSnapshot>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let opt = Option::<String>::deserialize(deserializer)?;
    opt.map(|s| UseSnapshot::from_str(&s).map_err(serde::de::Error::custom))
        .transpose()
}

pub struct SnapshotRunOptions {
    pub paths: Vec<PathBuf>,
    pub as_root: bool,
    pub exclude: Option<Vec<String>>,
    pub exclude_file: Option<PathBuf>,
    pub tags: Option<String>,
    pub description: Option<String>,
    pub no_scan: bool,
    pub skip_if_unchanged: bool,
    pub with_atime: bool,
    pub num_readers: usize,
    pub num_packers: usize,
    pub stdin: bool,
}

impl From<&CmdArgs> for SnapshotRunOptions {
    fn from(args: &CmdArgs) -> Self {
        Self {
            paths: args.paths.clone(),
            as_root: args.as_root.unwrap_or(false),
            exclude: args.exclude.clone(),
            exclude_file: args.exclude_file.clone(),
            tags: args.tags_str.clone(),
            description: args.description.clone(),
            no_scan: args.no_scan.unwrap_or(false),
            skip_if_unchanged: args.skip_if_unchanged.unwrap_or(false),
            with_atime: args.with_atime.unwrap_or(false),
            num_readers: args.num_readers.unwrap_or(DEFAULT_SNAPSHOT_READERS),
            num_packers: args
                .num_packers
                .unwrap_or(common::defaults::DEFAULT_SNAPSHOT_PACKERS),
            stdin: args.stdin,
        }
    }
}

pub async fn run(global_args: &GlobalArgs, args: &CmdArgs) -> Result<(), SnapshotError> {
    tracing::info!(target: "snapshot", "Starting snapshot command");

    if args.stdin
        && global_args.auth_file.is_none()
        && (get_envvar(USERNAME_ENVVAR).is_none() || get_envvar(PASSWORD_ENVVAR).is_none())
    {
        return Err(SnapshotError::SourcePathError(
            "--stdin requires --auth-file or both MAPACHE_USERNAME and \
             MAPACHE_PASSWORD env vars (stdin is consumed by backup data, \
             so password cannot be prompted interactively)"
                .to_string(),
        ));
    }

    if args.paths.is_empty() && !args.stdin {
        return Err(SnapshotError::SourcePathError(
            "no source paths provided.".to_string(),
        ));
    };

    // Verify source paths exist before acquiring the repository lock
    if !args.stdin {
        for path in &args.paths {
            let normalized = fs::get_absolute_normalized_path(path).map_err(|e| {
                SnapshotError::SourcePathError(format!(
                    "error processing path {:?}: {}",
                    path,
                    e.inner()
                ))
            })?;
            if !normalized.try_exists().map_err(|e| {
                SnapshotError::SourcePathError(format!(
                    "error accessing path {:?}: {}",
                    normalized, e
                ))
            })? {
                return Err(SnapshotError::SourcePathError(format!(
                    "source path does not exist: {:?}",
                    normalized.display()
                )));
            }
        }
    }

    let dry_run = args.dry_run;

    let num_readers = args.num_readers.unwrap_or(DEFAULT_SNAPSHOT_READERS);
    let json_output = global_args.json;

    let repo_result = with_repository_lock(
        global_args.auth_file.as_ref(),
        global_args.key.as_ref(),
        new_backend_with_prompt(global_args.backend_options(dry_run))
            .await
            .map_err(|e| {
                SnapshotError::SnapshotFailed(format!(
                    "failed to initialize backend: {}",
                    e.inner()
                ))
            })?,
        global_args.to_repo_config(),
        false,
        global_args.retry_lock_duration,
        global_args.no_lock,
        |repo, _, lock_handle| async move {
            let parent_snapshot_pair = if args.stdin {
                None
            } else {
                resolve_parent_snapshot(repo.clone(), args.no_parent, args.parent.clone()).await?
            };

            if !json_output {
                ui::cli::log!("");
                if dry_run {
                    ui::cli::log!("{}", "[DRY RUN]".bold().purple());
                }
                if args.no_parent {
                    ui::cli::log!("Full scan");
                } else if let Some(ref pair) = parent_snapshot_pair {
                    ui::cli::log!(
                        "Using snapshot {} as parent",
                        pair.id.to_short_hex(SHORT_SNAPSHOT_ID_LEN).bold().yellow()
                    );
                } else {
                    ui::cli::log!("{} No parent snapshot used.", "[!]".bold().cyan());
                }
            }

            let event_sender = if json_output {
                json_snapshot::make_event_sender(None, None)
            } else {
                snapshot::make_event_sender(None, None, num_readers)
            };

            if !dry_run {
                // Run pre-hook: abort command if it fails
                hooks::run_pre(hooks::snapshot(), "snapshot", &global_args.repo).await?;
            }

            let result = run_with_repo(
                repo,
                lock_handle,
                SnapshotRunOptions::from(args),
                event_sender,
                parent_snapshot_pair,
                None,
            )
            .await?;

            match result {
                SnapshotOutcome::Saved(ref completion) => {
                    if json_output {
                        #[derive(serde::Serialize)]
                        struct SnapshotCompleteMsg {
                            summary: SnapshotProcessSummary,
                            raw_bytes_data: u64,
                            compressed_bytes_data: u64,
                            raw_bytes_meta: u64,
                            compressed_bytes_meta: u64,
                            time_taken_seconds: f64,
                        }

                        ui::json::emit_static(
                            "snapshot_complete",
                            &SnapshotCompleteMsg {
                                summary: completion.process_summary.clone(),
                                raw_bytes_data: completion.summary.raw_bytes,
                                compressed_bytes_data: completion.summary.encoded_bytes,
                                raw_bytes_meta: completion.meta_without_index_raw,
                                compressed_bytes_meta: completion.meta_without_index_encoded,
                                time_taken_seconds: completion.duration.as_secs_f64(),
                            },
                        );
                    } else {
                        ui::cli::log!("");
                        show_cli_summary(completion, args.dry_run);
                        let prefix = if args.dry_run {
                            format!("{} ", "[DRY RUN]".bold().purple())
                        } else {
                            String::new()
                        };
                        ui::cli::log!(
                            "{}Processed {} in {}",
                            prefix,
                            utils::format_size_binary(completion.summary.processed_bytes, 3).cyan(),
                            utils::pretty_print_duration(completion.duration).cyan(),
                        );
                    }
                }
                SnapshotOutcome::SkippedNoChanges if !json_output => {
                    ui::cli::log!("No changes detected since parent. Skipping snapshot.");
                }
                SnapshotOutcome::Interrupted => {
                    return Err(SnapshotError::Interrupted);
                }
                _ => {}
            }

            Ok(())
        },
    )
    .await;

    let result_str = match &repo_result {
        Ok(_) => "success".to_string(),
        Err(e) => format!("{e}"),
    };
    if !dry_run {
        // Run post-hook: warning on failure, always continues
        hooks::run_post(
            hooks::snapshot(),
            "snapshot",
            &global_args.repo,
            &result_str,
        )
        .await;
    }

    repo_result
}

pub struct SnapshotCompletion {
    pub snapshot_id: ID,
    pub summary: SnapshotSummary,
    pub process_summary: SnapshotProcessSummary,
    pub meta_without_index_raw: u64,
    pub meta_without_index_encoded: u64,
    pub duration: std::time::Duration,
}

pub enum SnapshotOutcome {
    Saved(Box<SnapshotCompletion>),
    SkippedNoChanges,
    Interrupted,
}

pub(crate) async fn run_with_repo(
    repo: Arc<Repository>,
    lock_handle: Option<LockHandle>,
    options: SnapshotRunOptions,
    event_sender: EventSender,
    parent_snapshot_pair: Option<SnapshotPair>,
    shutdown_signal: Option<Arc<AtomicBool>>,
) -> Result<SnapshotOutcome, SnapshotError> {
    let as_root = options.as_root;
    let skip_if_unchanged = options.skip_if_unchanged;
    let num_readers = options.num_readers;
    let num_packers = options.num_packers;
    let tags_str = options.tags.unwrap_or_else(|| EMPTY_TAG_MARK.to_string());

    let start = Instant::now();

    tracing::info!(target: "snapshot", "Reloading master index");
    repo.reload_master_index().await?;

    // Get source paths from arguments or readdir root path
    tracing::info!(target: "snapshot", "Processing source paths: {:?}", options.paths);
    let source_paths = if !as_root {
        options.paths.clone()
    } else {
        if options.paths.len() != 1 {
            return Err(SnapshotError::SourcePathError(
                "only one path can be the snapshot root".to_string(),
            ));
        }
        let root = &options.paths[0];
        if !root.is_dir() {
            return Err(SnapshotError::SourcePathError(
                "the snapshot root must be a directory".to_string(),
            ));
        }

        let mut dir = tokio::fs::read_dir(root).await?;
        let mut paths = Vec::new();
        while let Some(entry) = dir.next_entry().await? {
            paths.push(entry.path());
        }
        paths
    };

    let mut tags: BTreeSet<String> = parse_tags(Some(&tags_str));
    tags.retain(|tag| tag != EMPTY_TAG_MARK);

    // Canonicalize and deduplicate source paths
    let mut absolute_source_paths = BTreeSet::new();
    for path in &source_paths {
        match fs::get_absolute_normalized_path(path) {
            Ok(absolute_path) => {
                let _ = absolute_source_paths.insert(absolute_path);
            }
            Err(e) => {
                return Err(SnapshotError::SourcePathError(format!(
                    "error processing path {:?}: {}",
                    path,
                    e.inner()
                )));
            }
        }
    }

    let (
        absolute_source_paths,
        snapshot_root_path,
        exclude_paths,
        parent_snapshot,
        no_scan,
        with_atime,
        stdin,
    ) = if options.stdin {
        (
            vec![PathBuf::from("/stdin")],
            PathBuf::from("/"),
            Vec::new(),
            None,
            true,
            false,
            true,
        )
    } else {
        // Read exclude paths from file if provided.
        let excludes_from_file = match &options.exclude_file {
            Some(path) => Some(read_filtered_paths_from_file(path).map_err(|e| {
                SnapshotError::SourcePathError(format!("error reading exclude file: {}", e.inner()))
            })?),
            None => None,
        };

        let all_excludes =
            merge_filtered_paths(options.exclude.as_ref(), excludes_from_file.as_ref());

        // Normalize the exclude paths and filter the source paths using the excludes
        let normalized_excludes: Option<Vec<PathBuf>> =
            normalized_exclude_paths(all_excludes.as_ref())?;
        let path_filter = PathFilter::new(None, normalized_excludes.clone());

        absolute_source_paths.retain(|p| path_filter.allow(p));
        let absolute_source_paths: Vec<PathBuf> = absolute_source_paths.into_iter().collect();

        // Extract the snapshot root path
        let snapshot_root_path = calculate_lcp(&absolute_source_paths, false);

        (
            absolute_source_paths,
            snapshot_root_path,
            normalized_excludes.unwrap_or_default(),
            parent_snapshot_pair.as_ref(),
            options.no_scan,
            options.with_atime,
            false,
        )
    };

    // Init cleanup handler (listens for SIGINT/SIGTERM, releases locks on signal).
    // When a shutdown_signal is provided (e.g. from the TUI) it is shared directly
    // with the archiver pipeline, so both OS signals and the TUI's Esc key abort
    // the work. Without one, a fresh signal is created internally.
    let event_sender_for_cleanup = event_sender.clone();
    let cleanup_handler = match shutdown_signal {
        Some(signal) => {
            CleanupHandler::new_with_interrupt_and_callback(signal.clone(), move || {
                event_sender_for_cleanup(Event::Backup(BackupEvent::Finished(
                    SnapshotProcessSummary {
                        processed_items_count: 0,
                        processed_bytes: 0,
                        diff_counts: Default::default(),
                    },
                )));
            })
        }
        None => CleanupHandler::new_with_callback(move || {
            event_sender_for_cleanup(Event::Backup(BackupEvent::Finished(
                SnapshotProcessSummary {
                    processed_items_count: 0,
                    processed_bytes: 0,
                    diff_counts: Default::default(),
                },
            )));
        }),
    };
    cleanup_handler.add_lock(lock_handle);

    repo.init_pack_saver(num_packers).map_err(|e| {
        SnapshotError::SnapshotFailed(format!("failed to initialize pack saver: {}", e.inner()))
    })?;

    // Process and save new snapshot
    tracing::info!(target: "snapshot", "Starting archival process");
    let snapshot_result = archiver::snapshot(
        repo.clone(),
        SnapshotOptions {
            absolute_source_paths,
            snapshot_root_path,
            exclude_paths,
            parent_snapshot,
            tags,
            description: options.description.clone(),
            no_scan,
            with_atime,
            stdin,
        },
        num_readers,
        event_sender,
        cleanup_handler.interrupted.clone(),
    )
    .await;

    // Flush repo and finalize pack saver
    tracing::info!(target: "snapshot", "Flushing and finalizing repository");
    let repo_stats = repo.flush_and_finalize_pack_saver().await.map_err(|e| {
        SnapshotError::SnapshotFailed(format!("failed to finalize snapshot: {}", e.inner()))
    })?;

    // Handle potential interruption or error
    let mut new_snapshot = match snapshot_result {
        Ok(s) => s,
        Err(e) => {
            if cleanup_handler.is_interrupted() {
                tracing::info!(target: "snapshot", "Snapshot interrupted by user");
                return Ok(SnapshotOutcome::Interrupted);
            }

            tracing::error!(target: "snapshot", "Snapshot archival failed: {e}");
            return Err(SnapshotError::SnapshotFailed(e.to_string()));
        }
    };

    let process_summary = SnapshotProcessSummary {
        processed_items_count: new_snapshot.summary.processed_items_count,
        processed_bytes: new_snapshot.summary.processed_bytes,
        diff_counts: new_snapshot.summary.diff_counts.clone(),
    };

    // Fill snapshot summary from repo stats
    new_snapshot.summary.raw_bytes = repo_stats.data.raw;
    new_snapshot.summary.encoded_bytes = repo_stats.data.encoded;
    new_snapshot.summary.meta_raw_bytes = repo_stats.meta.raw;
    new_snapshot.summary.meta_encoded_bytes = repo_stats.meta.encoded;
    new_snapshot.summary.total_raw_bytes =
        new_snapshot.summary.raw_bytes + new_snapshot.summary.meta_raw_bytes;
    new_snapshot.summary.total_encoded_bytes =
        new_snapshot.summary.encoded_bytes + new_snapshot.summary.meta_encoded_bytes;
    new_snapshot.summary.data_blobs = repo_stats.blobs;
    new_snapshot.summary.meta_blobs = repo_stats.meta_blobs;

    let should_save_snapshot = if !skip_if_unchanged {
        true
    } else if let Some(ref parent) = parent_snapshot_pair {
        parent.snapshot.tree != new_snapshot.tree
    } else {
        true
    };

    let completion = if should_save_snapshot {
        tracing::info!(target: "snapshot", "Saving new snapshot");
        let (new_snapshot_id, new_snapshot_size) = repo
            .save_file(
                &common::SaveID::CalculateID,
                serde_json::to_string(&new_snapshot)
                    .map_err(|e| SnapshotError::Repo(MapacheError::Serialization(e)))?
                    .as_bytes(),
                StorageHint {
                    is_metadata: true,
                    file_type: ContentIdType::Snapshot,
                },
                None,
            )
            .await?;
        tracing::info!(target: "snapshot", "Snapshot saved: {new_snapshot_id}");

        let meta_without_index_raw = new_snapshot.summary.meta_raw_bytes + new_snapshot_size.raw;
        let meta_without_index_encoded =
            new_snapshot.summary.meta_encoded_bytes + new_snapshot_size.encoded;

        // Add the size of the snapshot file and index for persistence
        new_snapshot.summary.meta_raw_bytes += new_snapshot_size.raw + repo_stats.index.raw;
        new_snapshot.summary.meta_encoded_bytes +=
            new_snapshot_size.encoded + repo_stats.index.encoded;
        new_snapshot.summary.total_raw_bytes =
            new_snapshot.summary.raw_bytes + new_snapshot.summary.meta_raw_bytes;
        new_snapshot.summary.total_encoded_bytes =
            new_snapshot.summary.encoded_bytes + new_snapshot.summary.meta_encoded_bytes;

        SnapshotOutcome::Saved(Box::new(SnapshotCompletion {
            snapshot_id: new_snapshot_id,
            summary: new_snapshot.summary.clone(),
            process_summary,
            meta_without_index_raw,
            meta_without_index_encoded,
            duration: start.elapsed(),
        }))
    } else {
        tracing::info!(target: "snapshot", "No changes detected. Snapshot skipped.");
        SnapshotOutcome::SkippedNoChanges
    };

    tracing::info!(target: "snapshot", "Snapshot command completed in {:?}", start.elapsed());
    Ok(completion)
}

pub(crate) async fn resolve_parent_snapshot(
    repo: Arc<Repository>,
    no_parent: bool,
    parent: Option<UseSnapshot>,
) -> Result<Option<SnapshotPair>, SnapshotError> {
    if no_parent {
        return Ok(None);
    }
    let use_snapshot = parent.unwrap_or(UseSnapshot::Latest);
    match find_use_snapshot(repo, &use_snapshot).await {
        Ok(Some((id, snapshot))) => Ok(Some(SnapshotPair { id, snapshot })),
        Ok(None) => Ok(None),
        Err(e) => Err(SnapshotError::ParentNotFound(e.to_string())),
    }
}

fn show_cli_summary(completion: &SnapshotCompletion, dry_run: bool) {
    let summary = &completion.summary;
    let snapshot_id = &completion.snapshot_id;

    ui::cli::log!("{}", "Changes since parent snapshot:".bold());

    let mut table = Table::new_with_alignments(vec![
        Alignment::Left,
        Alignment::Right,
        Alignment::Right,
        Alignment::Right,
        Alignment::Right,
    ]);
    table.set_headers(vec![
        "".to_string(),
        "new".bold().green().to_string(),
        "changed".bold().yellow().to_string(),
        "deleted".bold().red().to_string(),
        "unchanged".bold().to_string(),
    ]);

    table.add_row(vec![
        "Files".bold().to_string(),
        summary.diff_counts.new_files.to_string(),
        summary.diff_counts.changed_files.to_string(),
        summary.diff_counts.deleted_files.to_string(),
        summary.diff_counts.unchanged_files.to_string(),
    ]);
    table.add_row(vec![
        "Dirs".bold().to_string(),
        summary.diff_counts.new_dirs.to_string(),
        summary.diff_counts.changed_dirs.to_string(),
        summary.diff_counts.deleted_dirs.to_string(),
        summary.diff_counts.unchanged_dirs.to_string(),
    ]);
    ui::cli::log!("{}", table.render());

    if !dry_run {
        ui::cli::log!(
            "Created snapshot {}",
            snapshot_id
                .to_short_hex(common::defaults::SHORT_SNAPSHOT_ID_LEN)
                .to_string()
                .bold()
                .green()
        );
        ui::cli::log!("This snapshot added:");
    } else {
        ui::cli::log!("This snapshot would add:");
    }

    let mut data_table =
        Table::new_with_alignments(vec![Alignment::Left, Alignment::Right, Alignment::Right]);
    data_table.set_headers(vec![
        "".to_string(),
        "Raw".bold().yellow().to_string(),
        "Compressed".bold().green().to_string(),
    ]);
    data_table.add_row(vec![
        "Data".bold().to_string(),
        utils::format_size_binary(summary.raw_bytes, 3)
            .yellow()
            .to_string(),
        utils::format_size_binary(summary.encoded_bytes, 3)
            .green()
            .to_string(),
    ]);
    data_table.add_row(vec![
        "Metadata".bold().to_string(),
        utils::format_size_binary(summary.meta_raw_bytes, 3)
            .yellow()
            .to_string(),
        utils::format_size_binary(summary.meta_encoded_bytes, 3)
            .green()
            .to_string(),
    ]);
    data_table.add_separator();
    data_table.add_row(vec![
        "Total".bold().to_string(),
        utils::format_size_binary(summary.total_raw_bytes, 3)
            .bold()
            .yellow()
            .to_string(),
        utils::format_size_binary(summary.total_encoded_bytes, 3)
            .bold()
            .green()
            .to_string(),
    ]);
    ui::cli::log!("{}", data_table.render());
}
