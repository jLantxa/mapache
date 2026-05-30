use std::path::PathBuf;

use anyhow::{Context, Result};
use serde::{Deserialize, Deserializer};

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

/// Converts a config string to a PathBuf, expanding leading `~` to $HOME
/// and resolving `.`/`..` components.
pub(crate) fn config_path(s: &str) -> Result<PathBuf> {
    let path = if let Some(rest) = s.strip_prefix("~/")
        && let Ok(home) = std::env::var("HOME")
    {
        PathBuf::from(home).join(rest)
    } else if s == "~"
        && let Ok(home) = std::env::var("HOME")
    {
        PathBuf::from(home)
    } else {
        PathBuf::from(s)
    };
    crate::fs::get_absolute_normalized_path(&path)
}

/// Runtime-configurable defaults (the `[runtime]` section in the TOML config).
#[derive(Deserialize, Default, Debug, Clone)]
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

/// Top-level config structure. Sections map to command names.
#[derive(Deserialize, Default, Debug, Clone)]
#[serde(default, deny_unknown_fields)]
pub struct MapacheConfig {
    pub global: Option<crate::commands::CliGlobalArgs>,
    pub runtime: Option<RuntimeConfig>,
    pub snapshot: Option<crate::commands::cmd_snapshot::CmdArgs>,
    pub restore: Option<crate::commands::cmd_restore::CmdArgs>,
    pub forget: Option<crate::commands::cmd_forget::CmdArgs>,
}

pub fn load_config(path: &PathBuf) -> Result<MapacheConfig> {
    let content = std::fs::read_to_string(path)
        .with_context(|| format!("Failed to read config file: {}", path.display()))?;

    let config: MapacheConfig = toml::from_str(&content)
        .with_context(|| format!("Failed to parse config file: {}", path.display()))?;

    Ok(config)
}
