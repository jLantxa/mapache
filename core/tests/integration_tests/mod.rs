use std::{
    path::{Path, PathBuf},
    sync::Arc,
};

use anyhow::{Context, Result};
use tempfile::tempdir;
use zeroize::Zeroizing;

use mapache::{
    backend::{StorageBackend, localfs::LocalFS, read_backend_dir},
    commands::{Compression, GlobalArgs},
    mapache::{defaults::DEFAULT_PACK_SIZE_MIB, global::set_global_opts_with_args},
    repository::repo::{Auth, Repository},
};

use crate::{TEST_QUIET, test_utils};

mod test_cmd_amend;
mod test_cmd_bundle;
mod test_cmd_cat;
mod test_cmd_clean;
mod test_cmd_completion;
mod test_cmd_diff;
mod test_cmd_find;
mod test_cmd_forget;
mod test_cmd_init;
mod test_cmd_key;
mod test_cmd_lock;
mod test_cmd_log;
mod test_cmd_ls;
mod test_cmd_rebuild_index;
mod test_cmd_rechunk;
mod test_cmd_restore;
mod test_cmd_snapshot;
mod test_cmd_stats;
mod test_cmd_sync;
mod test_cmd_verify;
mod test_corrupt_repo;
mod test_lock_cleanup;
mod test_permission_denied;
mod test_zeroize;

#[cfg(all(feature = "fuse", unix))]
mod test_cmd_mount;

const BACKUP_DATA_PATH: &str = "backup_data.tar.xz";

pub fn assert_times_equal(t1: std::time::SystemTime, t2: std::time::SystemTime) {
    if t1 == t2 {
        return;
    }

    use std::time::UNIX_EPOCH;
    let d1 = t1.duration_since(UNIX_EPOCH).unwrap_or_default();
    let d2 = t2.duration_since(UNIX_EPOCH).unwrap_or_default();
    let diff = d1.abs_diff(d2);

    if diff.as_secs() <= 1 {
        return;
    }

    assert_eq!(t1, t2, "timestamps differ by {diff:?}");
}

pub struct TestContext {
    pub _tmp_dir: tempfile::TempDir,
    pub repo_path: PathBuf,
    pub auth: Auth,
    pub auth_file_path: PathBuf,
    pub global: GlobalArgs,
    pub backup_data_path: Option<PathBuf>,
}

impl TestContext {
    pub async fn new() -> Result<Self> {
        let tmp_dir = tempdir()?;
        let tmp_path = tmp_dir.path();
        let auth = Auth {
            username: "mapachito".to_string(),
            password: Zeroizing::new("password".to_string()),
        };
        let auth_file_path = tmp_path.join("auth");
        std::fs::write(
            &auth_file_path,
            format!("{}\n{}", auth.username, *auth.password),
        )?;

        let repo_path = tmp_path.join("repo");

        let global = GlobalArgs {
            repo: repo_path.to_string_lossy().to_string(),
            auth_file: Some(auth_file_path.clone()),
            key: None,
            quiet: *TEST_QUIET,
            json: false,
            verbosity: Some(3),
            ssh_privatekey: None,
            pack_size_mib: DEFAULT_PACK_SIZE_MIB,
            no_cache: true,
            retry_lock_duration: None,
            compression_level: Compression::Fastest,
            limit_upload: None,
            limit_download: None,
        };
        set_global_opts_with_args(&global);

        Ok(Self {
            _tmp_dir: tmp_dir,
            repo_path,
            auth,
            auth_file_path,
            global,
            backup_data_path: None,
        })
    }

    pub fn setup_backup_data(&mut self) -> Result<()> {
        let backup_data_path = test_utils::get_test_data_path(BACKUP_DATA_PATH);
        let backup_data_tmp_path = self._tmp_dir.path().join("backup");
        test_utils::extract_tar_xz_archive(&backup_data_path, &backup_data_tmp_path)?;
        self.backup_data_path = Some(backup_data_tmp_path);
        Ok(())
    }

    pub async fn init_repo(&self) -> Result<()> {
        init_repo(&self.auth, self.repo_path.clone()).await
    }

    /// Creates a default Snapshot CmdArgs builder.
    pub fn snapshot_builder(&self, paths: Vec<PathBuf>) -> SnapshotBuilder {
        SnapshotBuilder::new(paths)
    }

    /// Run a snapshot command with the given paths and default options.
    pub async fn snapshot(&self, paths: Vec<PathBuf>) -> Result<()> {
        self.snapshot_builder(paths).run(&self.global).await
    }

    /// Creates a default Restore CmdArgs builder.
    pub fn restore_builder(&self, target: PathBuf) -> RestoreBuilder {
        RestoreBuilder::new(target)
    }

    /// Creates a default Init CmdArgs builder.
    pub fn init_builder(&self) -> InitBuilder {
        InitBuilder::new()
    }

    /// Creates a default RebuildIndex CmdArgs builder.
    pub fn rebuild_index_builder(&self) -> RebuildIndexBuilder {
        RebuildIndexBuilder::new()
    }

    /// Creates a default Rechunk CmdArgs builder.
    pub fn rechunk_builder(&self) -> RechunkBuilder {
        RechunkBuilder::new()
    }

    /// Creates a default Amend CmdArgs builder.
    pub fn amend_builder(&self) -> AmendBuilder {
        AmendBuilder::new()
    }

    /// Creates a default Bundle CmdArgs builder.
    pub fn bundle_builder(&self) -> BundleBuilder {
        BundleBuilder::new()
    }

    /// Creates a default Forget CmdArgs builder.
    pub fn forget_builder(&self) -> ForgetBuilder {
        ForgetBuilder::new()
    }

    /// Creates a default Clean CmdArgs builder.
    pub fn clean_builder(&self) -> CleanBuilder {
        CleanBuilder::new()
    }

    /// Creates a default Verify CmdArgs builder.
    pub fn verify_builder(&self) -> VerifyBuilder {
        VerifyBuilder::new()
    }

    /// Creates a default Recall CmdArgs builder.
    pub fn recall_builder(&self, id: String) -> RecallBuilder {
        RecallBuilder::new(id)
    }

    /// Creates a default Unlock CmdArgs builder.
    pub fn unlock_builder(&self) -> UnlockBuilder {
        UnlockBuilder::new()
    }

    /// Creates a default Sync CmdArgs builder.
    pub fn sync_builder(&self, target: String) -> SyncBuilder {
        SyncBuilder::new(target)
    }

    /// Creates a default Log CmdArgs builder.
    pub fn log_builder(&self) -> LogBuilder {
        LogBuilder::new()
    }

    /// Creates a default Stats CmdArgs builder.
    pub fn stats_builder(&self) -> StatsBuilder {
        StatsBuilder::new()
    }

    /// Creates a default Cat CmdArgs builder.
    pub fn cat_builder(&self, object: mapache::commands::cmd_cat::Object) -> CatBuilder {
        CatBuilder::new(object)
    }

    /// Run the mapache binary with the given arguments and return the output.
    /// Automatically adds --repo and --auth-file as global arguments if supported.
    pub fn run_mapache(&self, args: &[&str]) -> Result<std::process::Output> {
        if args.is_empty() {
            return run_bin(args);
        }

        // Commands that do NOT support global arguments
        let no_global = matches!(args[0], "completion" | "bundle");

        let mut final_args = vec![args[0]];
        if !no_global {
            final_args.extend_from_slice(&[
                "--repo",
                self.repo_path.to_str().unwrap(),
                "--auth-file",
                self.auth_file_path.to_str().unwrap(),
            ]);
        }
        final_args.extend_from_slice(&args[1..]);
        run_bin(&final_args)
    }

    /// Run the mapache binary and return stdout as a String.
    pub fn run_mapache_ok(&self, args: &[&str]) -> Result<String> {
        let output = self.run_mapache(args)?;
        assert!(
            output.status.success(),
            "Command failed: mapache {}\nstdout: {}\nstderr: {}",
            args.join(" "),
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        Ok(String::from_utf8(output.stdout)?)
    }

    /// Get all snapshot IDs in the repository, sorted by creation time.
    pub fn get_snapshot_ids(&self) -> Result<Vec<String>> {
        use mapache::repository::repo::SNAPSHOTS_DIR;
        let snapshots_dir = self.repo_path.join(SNAPSHOTS_DIR);
        let mut snapshots = std::fs::read_dir(&snapshots_dir)?
            .map(|res| res.map(|e| e.path()))
            .collect::<Result<Vec<_>, _>>()?;

        // Sort by metadata modified time to get chronological order
        snapshots.sort_by_key(|p| std::fs::metadata(p).and_then(|m| m.modified()).unwrap());

        Ok(snapshots
            .iter()
            .map(|p| p.file_name().unwrap().to_str().unwrap().to_string())
            .collect())
    }
}

#[derive(Clone)]
pub struct InitBuilder {
    pub args: mapache::commands::cmd_init::CmdArgs,
}

impl InitBuilder {
    pub fn new() -> Self {
        use mapache::commands::cmd_init;
        Self {
            args: cmd_init::CmdArgs {},
        }
    }

    pub async fn run(self, global: &GlobalArgs) -> Result<()> {
        mapache::commands::cmd_init::run(global, &self.args).await
    }
}

#[derive(Clone)]
pub struct VerifyBuilder {
    pub args: mapache::commands::cmd_verify::CmdArgs,
}

impl VerifyBuilder {
    pub fn new() -> Self {
        use mapache::commands::cmd_verify;
        Self {
            args: cmd_verify::CmdArgs {
                read_packs: false,
                parallel: 4,
                with_cache: false,
                fail_early: false,
                sample: None,
            },
        }
    }

    pub fn read_packs(mut self, read_packs: bool) -> Self {
        self.args.read_packs = read_packs;
        self
    }

    pub fn parallel(mut self, parallel: usize) -> Self {
        self.args.parallel = parallel;
        self
    }

    pub fn fail_early(mut self, fail_early: bool) -> Self {
        self.args.fail_early = fail_early;
        self
    }

    pub fn sample(mut self, sample: Option<f64>) -> Self {
        self.args.sample = sample;
        self
    }

    pub async fn run(self, global: &GlobalArgs) -> Result<()> {
        mapache::commands::cmd_verify::run(global, &self.args).await
    }
}

#[derive(Clone)]
pub struct RecallBuilder {
    pub args: mapache::commands::cmd_recall::CmdArgs,
}

impl RecallBuilder {
    pub fn new(id: String) -> Self {
        use mapache::commands::cmd_recall;
        Self {
            args: cmd_recall::CmdArgs { id },
        }
    }

    pub async fn run(self, global: &GlobalArgs) -> Result<()> {
        mapache::commands::cmd_recall::run(global, &self.args).await
    }
}

#[derive(Clone)]
pub struct SnapshotBuilder {
    pub args: mapache::commands::cmd_snapshot::CmdArgs,
}

impl SnapshotBuilder {
    pub fn new(paths: Vec<PathBuf>) -> Self {
        use mapache::commands::{UseSnapshot, cmd_snapshot};
        Self {
            args: cmd_snapshot::CmdArgs {
                paths,
                as_root: false,
                exclude: None,
                exclude_file: None,
                tags_str: String::new(),
                description: None,
                no_parent: false,
                skip_if_unchanged: false,
                no_scan: false,
                parent: UseSnapshot::Latest,
                num_readers: 2,
                num_packers: 2,
                dry_run: false,
            },
        }
    }

    pub fn dry_run(mut self, dry_run: bool) -> Self {
        self.args.dry_run = dry_run;
        self
    }

    pub fn exclude(mut self, exclude: Vec<String>) -> Self {
        self.args.exclude = Some(exclude);
        self
    }

    pub fn no_parent(mut self, no_parent: bool) -> Self {
        self.args.no_parent = no_parent;
        self
    }

    pub fn skip_if_unchanged(mut self, skip: bool) -> Self {
        self.args.skip_if_unchanged = skip;
        self
    }

    pub fn no_scan(mut self, no_scan: bool) -> Self {
        self.args.no_scan = no_scan;
        self
    }

    pub fn root(mut self, as_root: bool) -> Self {
        self.args.as_root = as_root;
        self
    }

    pub fn tags(mut self, tags: String) -> Self {
        self.args.tags_str = tags;
        self
    }

    pub fn description(mut self, description: String) -> Self {
        self.args.description = Some(description);
        self
    }

    pub fn parent(mut self, parent: mapache::commands::UseSnapshot) -> Self {
        self.args.parent = parent;
        self
    }

    pub fn num_readers(mut self, num: usize) -> Self {
        self.args.num_readers = num;
        self
    }

    pub fn num_packers(mut self, num: usize) -> Self {
        self.args.num_packers = num;
        self
    }

    pub async fn run(self, global: &GlobalArgs) -> Result<()> {
        mapache::commands::cmd_snapshot::run(global, &self.args).await
    }
}

#[derive(Clone)]
pub struct RestoreBuilder {
    pub args: mapache::commands::cmd_restore::CmdArgs,
}

impl RestoreBuilder {
    pub fn new(target: PathBuf) -> Self {
        use mapache::commands::{UseSnapshot, cmd_restore};
        use mapache::restorer::Strategy;
        Self {
            args: cmd_restore::CmdArgs {
                sparse: false,
                target,
                snapshot: UseSnapshot::Latest,
                dry_run: false,
                verify: false,
                include: None,
                exclude: None,
                include_file: None,
                exclude_file: None,
                strip_prefix: false,
                strategy: Strategy::Skip,
                quit_on_error: true,
                delete: false,
                no_preserve_root: false,
            },
        }
    }

    pub fn strip_prefix(mut self, strip: bool) -> Self {
        self.args.strip_prefix = strip;
        self
    }

    pub fn exclude(mut self, exclude: Vec<String>) -> Self {
        self.args.exclude = Some(exclude);
        self
    }

    pub fn include(mut self, include: Vec<String>) -> Self {
        self.args.include = Some(include);
        self
    }

    pub fn dry_run(mut self, dry_run: bool) -> Self {
        self.args.dry_run = dry_run;
        self
    }

    pub fn verify(mut self, verify: bool) -> Self {
        self.args.verify = verify;
        self
    }

    pub fn delete(mut self, delete: bool) -> Self {
        self.args.delete = delete;
        self
    }

    pub fn strategy(mut self, strategy: mapache::restorer::Strategy) -> Self {
        self.args.strategy = strategy;
        self
    }

    pub fn sparse(mut self, sparse: bool) -> Self {
        self.args.sparse = sparse;
        self
    }

    pub fn quit_on_error(mut self, quit: bool) -> Self {
        self.args.quit_on_error = quit;
        self
    }

    pub fn no_preserve_root(mut self, no_preserve: bool) -> Self {
        self.args.no_preserve_root = no_preserve;
        self
    }

    pub async fn run(self, global: &GlobalArgs) -> Result<()> {
        mapache::commands::cmd_restore::run(global, &self.args).await
    }
}

#[derive(Clone)]
pub struct AmendBuilder {
    pub args: mapache::commands::cmd_amend::CmdArgs,
}

impl AmendBuilder {
    pub fn new() -> Self {
        use mapache::commands::{UseSnapshot, cmd_amend};
        Self {
            args: cmd_amend::CmdArgs {
                snapshot: UseSnapshot::Latest,
                all: false,
                keep_old: false,
                tags_str: None,
                clear_tags: false,
                description: None,
                clear_description: false,
                exclude: None,
                exclude_file: None,
            },
        }
    }

    pub fn exclude(mut self, exclude: Vec<String>) -> Self {
        self.args.exclude = Some(exclude);
        self
    }

    pub fn clear_tags(mut self, clear: bool) -> Self {
        self.args.clear_tags = clear;
        self
    }

    pub fn clear_description(mut self, clear: bool) -> Self {
        self.args.clear_description = clear;
        self
    }

    pub fn tags(mut self, tags: String) -> Self {
        self.args.tags_str = Some(tags);
        self
    }

    pub fn description(mut self, description: String) -> Self {
        self.args.description = Some(description);
        self
    }

    pub async fn run(self, global: &GlobalArgs) -> Result<()> {
        mapache::commands::cmd_amend::run(global, &self.args).await
    }
}

#[derive(Clone)]
pub struct BundleBuilder {
    pub args: mapache::commands::cmd_bundle::CmdArgs,
}

impl BundleBuilder {
    pub fn new() -> Self {
        use mapache::commands::cmd_bundle;
        Self {
            args: cmd_bundle::CmdArgs::default(),
        }
    }

    pub fn bundle(mut self, bundle: bool) -> Self {
        self.args.bundle = bundle;
        self
    }

    pub fn extract(mut self, extract: bool) -> Self {
        self.args.extract = extract;
        self
    }

    pub fn input(mut self, input: Vec<PathBuf>) -> Self {
        self.args.input = input;
        self
    }

    pub fn output(mut self, output: PathBuf) -> Self {
        self.args.output = Some(output);
        self
    }

    pub fn password(mut self, password: String) -> Self {
        self.args.internal_password = Some(password);
        self
    }

    pub async fn run(self) -> Result<()> {
        mapache::commands::cmd_bundle::run(&self.args).await
    }
}

#[derive(Clone)]
pub struct ForgetBuilder {
    pub args: mapache::commands::cmd_forget::CmdArgs,
}

impl ForgetBuilder {
    pub fn new() -> Self {
        use mapache::commands::cmd_forget;
        Self {
            args: cmd_forget::CmdArgs {
                forget: Vec::new(),
                force: false,
                keep_last: None,
                keep_within: None,
                keep_yearly: None,
                keep_monthly: None,
                keep_weekly: None,
                keep_daily: None,
                run_gc: false,
                dry_run: false,
                tolerance: 0.0,
                tags_str: None,
                keep_tags_str: None,
            },
        }
    }

    pub fn forget(mut self, forget: Vec<String>) -> Self {
        self.args.forget = forget;
        self
    }

    pub fn force(mut self, force: bool) -> Self {
        self.args.force = force;
        self
    }

    pub fn keep_last(mut self, count: usize) -> Self {
        self.args.keep_last = Some(count);
        self
    }

    pub async fn run(self, global: &GlobalArgs) -> Result<()> {
        mapache::commands::cmd_forget::run(global, &self.args).await
    }
}

#[derive(Clone)]
pub struct CleanBuilder {
    pub args: mapache::commands::cmd_clean::CmdArgs,
}

impl CleanBuilder {
    pub fn new() -> Self {
        use mapache::commands::cmd_clean;
        Self {
            args: cmd_clean::CmdArgs {
                tolerance: 0.0,
                dry_run: false,
                no_repack: false,
            },
        }
    }

    pub fn tolerance(mut self, tolerance: f32) -> Self {
        self.args.tolerance = tolerance;
        self
    }

    pub fn dry_run(mut self, dry_run: bool) -> Self {
        self.args.dry_run = dry_run;
        self
    }

    pub async fn run(self, global: &GlobalArgs) -> Result<()> {
        mapache::commands::cmd_clean::run(global, &self.args).await
    }
}

#[derive(Clone)]
pub struct LogBuilder {
    pub args: mapache::commands::cmd_log::CmdArgs,
}

impl LogBuilder {
    pub fn new() -> Self {
        use mapache::commands::cmd_log;
        Self {
            args: cmd_log::CmdArgs {
                snapshot: None,
                dropped: false,
                all: false,
                compact: false,
                tags_str: None,
            },
        }
    }

    pub fn compact(mut self, compact: bool) -> Self {
        self.args.compact = compact;
        self
    }

    pub async fn run(self, global: &GlobalArgs) -> Result<()> {
        mapache::commands::cmd_log::run(global, &self.args).await
    }
}

#[derive(Clone)]
pub struct StatsBuilder {
    pub args: mapache::commands::cmd_stats::CmdArgs,
}

impl StatsBuilder {
    pub fn new() -> Self {
        use mapache::commands::cmd_stats;
        Self {
            args: cmd_stats::CmdArgs { full: false },
        }
    }

    pub async fn run(self, global: &GlobalArgs) -> Result<()> {
        mapache::commands::cmd_stats::run(global, &self.args).await
    }
}

#[derive(Clone)]
pub struct CatBuilder {
    pub args: mapache::commands::cmd_cat::CmdArgs,
}

impl CatBuilder {
    pub fn new(object: mapache::commands::cmd_cat::Object) -> Self {
        use mapache::commands::cmd_cat;
        Self {
            args: cmd_cat::CmdArgs { object },
        }
    }

    pub async fn run(self, global: &GlobalArgs) -> Result<()> {
        mapache::commands::cmd_cat::run(global, &self.args).await
    }
}

#[derive(Clone)]
pub struct SyncBuilder {
    pub args: mapache::commands::cmd_sync::CmdArgs,
}

impl SyncBuilder {
    pub fn new(target: String) -> Self {
        use mapache::commands::cmd_sync;
        Self {
            args: cmd_sync::CmdArgs {
                target,
                delete: false,
                dst_ssh_privatekey: None,
                dry_run: false,
            },
        }
    }

    pub fn delete(mut self, delete: bool) -> Self {
        self.args.delete = delete;
        self
    }

    pub async fn run(self, global: &GlobalArgs) -> Result<()> {
        mapache::commands::cmd_sync::run(global, &self.args).await
    }
}

#[derive(Clone)]
pub struct RebuildIndexBuilder {
    pub args: mapache::commands::cmd_rebuild_index::CmdArgs,
}

impl RebuildIndexBuilder {
    pub fn new() -> Self {
        use mapache::commands::cmd_rebuild_index;
        Self {
            args: cmd_rebuild_index::CmdArgs { dry_run: false },
        }
    }

    pub async fn run(self, global: &GlobalArgs) -> Result<()> {
        mapache::commands::cmd_rebuild_index::run(global, &self.args).await
    }
}

#[derive(Clone)]
pub struct RechunkBuilder {
    pub args: mapache::commands::cmd_rechunk::CmdArgs,
}

impl RechunkBuilder {
    pub fn new() -> Self {
        use mapache::commands::cmd_rechunk;
        Self {
            args: cmd_rechunk::CmdArgs {},
        }
    }

    pub async fn run(self, global: &GlobalArgs) -> Result<()> {
        mapache::commands::cmd_rechunk::run(global, &self.args).await
    }
}

#[derive(Clone)]
pub struct UnlockBuilder {
    pub args: mapache::commands::cmd_unlock::CmdArgs,
}

impl UnlockBuilder {
    pub fn new() -> Self {
        use mapache::commands::cmd_unlock;
        Self {
            args: cmd_unlock::CmdArgs { force: false },
        }
    }

    pub fn force(mut self, force: bool) -> Self {
        self.args.force = force;
        self
    }

    pub async fn run(self, global: &GlobalArgs) -> Result<()> {
        mapache::commands::cmd_unlock::run(global, &self.args).await
    }
}

async fn init_repo(auth: &Auth, repo_path: PathBuf) -> Result<()> {
    let backend = Arc::new(LocalFS::new(repo_path));
    let _ = Repository::init(auth, None, backend)
        .await
        .context("Failed to init repo")?;
    Ok(())
}

/// Remove all file nodes from a base directory. This is useful to remove all
/// index files or packs from the repository, without deleting the directories.
async fn delete_all_files_from(backend: &dyn StorageBackend, dir: &Path) -> Result<()> {
    let backend_objects = read_backend_dir(backend, &PathBuf::from(dir)).await?;

    for node in backend_objects {
        match node {
            mapache::backend::BackendNode::File(path, _) => backend.remove(&path).await?,
            mapache::backend::BackendNode::Dir(_) => (),
        }
    }

    Ok(())
}

fn set_write_permission<P: AsRef<Path>>(path: P, writable: bool) -> std::io::Result<()> {
    let metadata = std::fs::metadata(&path)?;
    let mut perms = metadata.permissions();

    perms.set_readonly(!writable);

    std::fs::set_permissions(&path, perms)
}

pub fn run_bin(args: &[&str]) -> Result<std::process::Output> {
    let bin_path = env!("CARGO_BIN_EXE_mapache");
    let mut cmd = std::process::Command::new(bin_path);
    cmd.args(args);

    // Set --no-cache for commands requiring it.
    if !args.is_empty() {
        match args[0] {
            // These commands don't accept --no-cache
            "cache" | "completion" | "key" => (),
            _ => {
                let _ = cmd.arg("--no-cache");
            }
        }
    }

    println!("{cmd:?}");

    let output = cmd.output().context("Failed to execute mapache binary")?;
    if !output.status.success() {
        eprintln!("Command failed with status: {}", output.status);
        eprintln!("stdout: {}", String::from_utf8_lossy(&output.stdout));
        eprintln!("stderr: {}", String::from_utf8_lossy(&output.stderr));
    }
    Ok(output)
}
