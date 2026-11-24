//! Debug logging module for mapache.
//!
//! Initializes a file-based logger controlled by environment variables:
//! - `MAPACHE_DEBUG_LEVEL`: Optional. One of `trace`, `debug`, `info`, `warn`, or `error`.
//!   When set (and valid) a log file is written. When unset, no subscriber is
//!   installed.
//! - `MAPACHE_DEBUG_PATH`: Optional. Directory where log files are written.
//!   Defaults to the current working directory if unset.
//!
//! Log files are named `mapache_<timestamp>.log` with automatic collision handling.

use std::{fs::OpenOptions, path::PathBuf, sync::Arc};

use tracing_subscriber::{
    filter::LevelFilter,
    fmt::{
        FmtContext, Layer,
        format::{FormatEvent, FormatFields, Writer},
    },
    layer::SubscriberExt,
    util::SubscriberInitExt,
};

use crate::{
    common::{
        time,
        vars::{DEBUG_LEVEL_ENVVAR, DEBUG_PATH_ENVVAR, get_envvar},
    },
    ui,
};

/// Custom event formatter for mapache logs.
///
/// Format: `[YYYY-MM-DD HH:MM:SS] [LEVEL] [TARGET] file:line: message`
struct MapacheFormat;

impl<S, N> FormatEvent<S, N> for MapacheFormat
where
    S: tracing::Subscriber + for<'a> tracing_subscriber::registry::LookupSpan<'a>,
    N: for<'a> FormatFields<'a> + 'static,
{
    fn format_event(
        &self,
        ctx: &FmtContext<'_, S, N>,
        mut writer: Writer<'_>,
        event: &tracing::Event<'_>,
    ) -> std::fmt::Result {
        let now = time::now_utc();
        let meta = event.metadata();

        write!(writer, "[{}] ", now.strftime("%Y-%m-%d %H:%M:%S"))?;
        write!(writer, "[{}] ", meta.level())?;
        write!(writer, "[{}] ", meta.target())?;

        if let (Some(file), Some(line)) = (meta.file(), meta.line()) {
            write!(writer, "{}:{} ", file, line)?;
        }

        ctx.field_format().format_fields(writer.by_ref(), event)?;
        writeln!(writer)
    }
}

/// Parses a log level string into a `tracing::Level`.
///
/// Returns `None` if the string is not a recognized log level.
fn parse_level(s: &str) -> Option<tracing::Level> {
    match s.to_lowercase().trim() {
        "trace" => Some(tracing::Level::TRACE),
        "debug" => Some(tracing::Level::DEBUG),
        "info" => Some(tracing::Level::INFO),
        "warn" | "warning" => Some(tracing::Level::WARN),
        "error" => Some(tracing::Level::ERROR),
        _ => None,
    }
}

/// Initializes a file-based tracing subscriber when `MAPACHE_DEBUG_LEVEL`
/// names a valid level. Without one, no subscriber is installed.
pub(crate) fn init_debugger() {
    let Some(level) = get_envvar(DEBUG_LEVEL_ENVVAR)
        .filter(|val| !val.is_empty())
        .and_then(|val| parse_level(&val))
    else {
        return;
    };

    let registry = tracing_subscriber::registry().with(LevelFilter::from(level));

    let debug_path = match get_envvar(DEBUG_PATH_ENVVAR) {
        Some(p) if !p.is_empty() => PathBuf::from(p),
        _ => PathBuf::from("."),
    };

    let now = time::now();
    let ts = now.strftime("%Y%m%d_%H%M%S");
    let log_name = format!("mapache_{ts}.log");
    let mut log_path = debug_path.clone();
    log_path.push(&log_name);

    if log_path.exists() {
        let mut counter = 1u32;
        loop {
            let mut alt_path = debug_path.clone();
            alt_path.push(format!("mapache_{ts}_{counter}.log"));
            if !alt_path.exists() {
                log_path = alt_path;
                break;
            }
            counter = counter.saturating_add(1);
            if counter == 0 {
                // Wrapped around — give up to avoid infinite loop
                break;
            }
        }
    }

    if let Err(e) = std::fs::create_dir_all(&debug_path) {
        ui::cli::warning!(
            "Failed to create debug log directory '{}': {}",
            debug_path.display(),
            e
        );
        return;
    }

    let file = match OpenOptions::new().create(true).append(true).open(&log_path) {
        Ok(f) => f,
        Err(e) => {
            ui::cli::warning!("Failed to initialize debug logger: {}", e);
            return;
        }
    };

    let writer = Arc::new(file);

    let layer = Layer::new()
        .with_writer(writer)
        .event_format(MapacheFormat)
        .with_ansi(false);

    if registry.with(layer).try_init().is_ok() {
        ui::cli::log!("Debug logging to {}", log_path.display());
    }
}
