use std::path::PathBuf;

use serde::{Deserialize, Deserializer, Serialize};

use crate::{
    commands,
    common::error::{MapacheError, Result},
    fs,
};

/// Deserializes an optional list of strings, expanding `~` in each entry.
pub(crate) fn deserialize_config_string_vec_opt<'de, D>(
    deserializer: D,
) -> std::result::Result<Option<Vec<String>>, D::Error>
where
    D: Deserializer<'de>,
{
    let opt = Option::<Vec<String>>::deserialize(deserializer)?;
    Ok(opt.map(|v| {
        v.into_iter()
            .map(|s| {
                if let Some(rest) = s.strip_prefix("~/")
                    && let Ok(home) = std::env::var("HOME")
                {
                    format!("{}/{}", home, rest)
                } else if s == "~"
                    && let Ok(home) = std::env::var("HOME")
                {
                    home
                } else {
                    s
                }
            })
            .collect()
    }))
}

/// Merge strategy for `Option<Vec<T>>`: appends config values to CLI values.
/// CLI values come first, config values are appended.
pub(crate) fn merge_option_vec<T>(left: &mut Option<Vec<T>>, right: Option<Vec<T>>) {
    match (left.take(), right) {
        (Some(mut l), Some(mut r)) => {
            l.append(&mut r);
            *left = Some(l);
        }
        (None, Some(r)) => *left = Some(r),
        (Some(l), None) => *left = Some(l),
        (None, None) => {}
    }
}

/// Deserializes an optional string into an optional PathBuf, expanding `~`.
pub(crate) fn deserialize_config_path_opt<'de, D>(
    deserializer: D,
) -> std::result::Result<Option<PathBuf>, D::Error>
where
    D: Deserializer<'de>,
{
    let opt = Option::<String>::deserialize(deserializer)?;
    opt.map(|s| config_path(&s).map_err(serde::de::Error::custom))
        .transpose()
}

/// Deserializes a list of strings into PathBufs, expanding `~`.
pub(crate) fn deserialize_config_paths_vec<'de, D>(
    deserializer: D,
) -> std::result::Result<Vec<PathBuf>, D::Error>
where
    D: Deserializer<'de>,
{
    let strings = Vec::<String>::deserialize(deserializer)?;
    strings
        .into_iter()
        .map(|s| config_path(&s).map_err(serde::de::Error::custom))
        .collect()
}

fn home_dir() -> Option<PathBuf> {
    std::env::var("HOME")
        .ok()
        .map(PathBuf::from)
        .or_else(|| std::env::var("USERPROFILE").ok().map(PathBuf::from))
}

/// Converts a config string to a PathBuf, expanding leading `~` to the user's
/// home directory and resolving `.`/`..` components.
pub(crate) fn config_path(s: &str) -> Result<PathBuf> {
    let path = if let Some(rest) = s.strip_prefix("~/") {
        let home = home_dir().ok_or_else(|| {
            MapacheError::Config("cannot expand '~': neither $HOME nor $USERPROFILE is set".into())
        })?;
        home.join(rest)
    } else if s == "~" {
        home_dir().ok_or_else(|| {
            MapacheError::Config("cannot expand '~': neither $HOME nor $USERPROFILE is set".into())
        })?
    } else {
        PathBuf::from(s)
    };

    fs::get_absolute_normalized_path(&path)
}

/// Runtime-configurable defaults (the `[runtime]` section in the TOML config).
#[derive(Serialize, Deserialize, Default, Debug, Clone)]
#[serde(default, deny_unknown_fields, rename_all = "kebab-case")]
pub struct RuntimeConfig {
    // Restore
    pub restore_blob_concurrency: Option<usize>,
    pub restore_decoded_budget: Option<u64>,
    pub restore_max_open_files: Option<usize>,
    pub restore_pack_prefetch: Option<usize>,
    pub restore_pack_read_merge_threshold: Option<u64>,
    pub restore_pack_segment_max_size: Option<u64>,
    // GC
    pub min_pack_size_factor: Option<f32>,
    pub gc_decoded_budget: Option<u64>,
    pub gc_repack_concurrency: Option<usize>,
    // Index
    pub blobs_per_index_file: Option<usize>,
    pub index_flush_timeout_secs: Option<u64>,
    // S3
    pub s3_multipart_threshold: Option<u64>,
    pub s3_multipart_part_size: Option<u64>,
    // UI
    pub max_path_display_len: Option<usize>,
    pub ui_snapshot_progress_item_min_size: Option<u64>,
}

impl RuntimeConfig {
    pub(crate) fn template() -> Self {
        use crate::common::defaults::*;
        Self {
            restore_blob_concurrency: Some(DEFAULT_RESTORE_BLOB_CONCURRENCY),
            restore_decoded_budget: Some(DEFAULT_RESTORE_DECODED_BUDGET),
            restore_max_open_files: Some(DEFAULT_RESTORE_MAX_OPEN_FILES),
            restore_pack_prefetch: Some(DEFAULT_RESTORE_PACK_PREFETCH),
            restore_pack_read_merge_threshold: Some(DEFAULT_RESTORE_PACK_READ_MERGE_THRESHOLD),
            restore_pack_segment_max_size: Some(DEFAULT_RESTORE_PACK_SEGMENT_MAX_SIZE),
            min_pack_size_factor: Some(DEFAULT_MIN_PACK_SIZE_FACTOR),
            gc_decoded_budget: Some(DEFAULT_GC_DECODED_BUDGET),
            gc_repack_concurrency: Some(DEFAULT_GC_REPACK_CONCURRENCY),
            blobs_per_index_file: Some(BLOBS_PER_INDEX_FILE),
            index_flush_timeout_secs: Some(INDEX_FLUSH_TIMEOUT.as_secs()),
            s3_multipart_threshold: Some(S3_MULTIPART_THRESHOLD),
            s3_multipart_part_size: Some(S3_MULTIPART_PART_SIZE),
            max_path_display_len: Some(MAX_PATH_DISPLAY_LEN),
            ui_snapshot_progress_item_min_size: UI_SNAPSHOT_PROGRESS_ITEM_MIN_SIZE,
        }
    }
}

/// Definition of a single hook (pre or post).
#[derive(Serialize, Deserialize, Default, Debug, Clone)]
#[serde(default, deny_unknown_fields)]
pub struct HookDef {
    /// Shell command to execute.
    pub command: String,
    /// Timeout in seconds. If the hook runs longer than this, it is killed.
    /// `None` means no timeout.
    pub timeout: Option<u64>,
}

/// Pre/post hooks for a specific command.
#[derive(Serialize, Deserialize, Default, Debug, Clone)]
#[serde(default, deny_unknown_fields)]
pub struct CommandHooks {
    /// Hook executed before the command starts.
    /// If the command fails (non-zero exit), the command is aborted.
    pub pre: Option<HookDef>,
    /// Hook executed after the command finishes, regardless of success/failure.
    /// Receives MAPACHE_RESULT = "success" or error message.
    pub post: Option<HookDef>,
}

/// Hooks configuration (the `[hooks]` section in TOML).
#[derive(Serialize, Deserialize, Default, Debug, Clone)]
#[serde(default, deny_unknown_fields)]
pub struct HooksConfig {
    pub snapshot: Option<CommandHooks>,
    pub restore: Option<CommandHooks>,
    pub forget: Option<CommandHooks>,
    pub clean: Option<CommandHooks>,
    pub verify: Option<CommandHooks>,
}

impl HooksConfig {
    pub(crate) fn template() -> Self {
        Self {
            snapshot: Some(CommandHooks {
                pre: Some(HookDef {
                    command: "echo 'starting snapshot'".into(),
                    timeout: None,
                }),
                post: Some(HookDef {
                    command: "echo 'snapshot finished: $MAPACHE_RESULT'".into(),
                    timeout: None,
                }),
            }),
            restore: None,
            forget: None,
            clean: None,
            verify: None,
        }
    }
}

/// Top-level config structure. Sections map to command names.
#[derive(Serialize, Deserialize, Default, Debug, Clone)]
#[serde(default, deny_unknown_fields)]
pub struct MapacheConfig {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub global: Option<commands::CliGlobalArgs>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub snapshot: Option<commands::cmd_snapshot::CmdArgs>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub restore: Option<commands::cmd_restore::CmdArgs>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub forget: Option<commands::cmd_forget::CmdArgs>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub runtime: Option<RuntimeConfig>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub hooks: Option<HooksConfig>,
}

impl MapacheConfig {
    pub(crate) fn template() -> Self {
        Self {
            global: Some(commands::CliGlobalArgs::template()),
            runtime: Some(RuntimeConfig::template()),
            snapshot: Some(commands::cmd_snapshot::CmdArgs::template()),
            restore: Some(commands::cmd_restore::CmdArgs::template()),
            forget: Some(commands::cmd_forget::CmdArgs::template()),
            hooks: Some(HooksConfig::template()),
        }
    }
}

pub fn load_config(path: &PathBuf) -> Result<MapacheConfig> {
    let content = std::fs::read_to_string(path).map_err(|e| {
        MapacheError::Config(format!(
            "Failed to read config file '{}': {}",
            path.display(),
            e
        ))
    })?;

    let config: MapacheConfig = toml::from_str(&content).map_err(|e| {
        MapacheError::Config(format!(
            "Failed to parse config file '{}': {}",
            path.display(),
            e
        ))
    })?;

    Ok(config)
}
