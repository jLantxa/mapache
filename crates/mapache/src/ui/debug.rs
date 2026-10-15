//! Debug logging module for mapache.
//!
//! Initializes a file-based logger controlled by environment variables:
//! - `MAPACHE_DEBUG_LEVEL`: Optional. One of `trace`, `debug`, `info`, `warn`, or `error`.
//!   When set (and valid) a log file is written. When unset, `info` is captured
//!   for the in-TUI log overlay but no file is created.
//! - `MAPACHE_DEBUG_PATH`: Optional. Directory where log files are written.
//!   Defaults to the current working directory if unset.
//!
//! Log files are named `mapache_<timestamp>.log` with automatic collision handling.
//!
//! Regardless of the file logger, every event at or above the active level is
//! also kept in an in-memory ring buffer that the TUI's log overlay reads.

use std::{
    collections::VecDeque,
    fmt::Write as _,
    fs::OpenOptions,
    path::PathBuf,
    sync::{Arc, Mutex, OnceLock},
};

use chrono::Utc;
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
    common::vars::{DEBUG_LEVEL_ENVVAR, DEBUG_PATH_ENVVAR, get_envvar},
    ui,
};

/// How many log records the in-TUI overlay keeps. Older records are dropped.
const MAX_LOG_RECORDS: usize = 1000;

/// A single captured tracing event, formatted for the log overlay.
#[derive(Debug, Clone)]
pub(crate) struct LogRecord {
    pub level: tracing::Level,
    pub target: String,
    pub message: String,
    pub timestamp: String,
}

/// Thread-safe ring buffer of recent log records, shared between the tracing
/// layer that fills it and the TUI that renders it.
#[derive(Clone, Default)]
pub(crate) struct LogBuffer {
    records: Arc<Mutex<VecDeque<LogRecord>>>,
}

impl LogBuffer {
    fn push(&self, record: LogRecord) {
        let mut records = self.records.lock().unwrap_or_else(|e| e.into_inner());
        if records.len() >= MAX_LOG_RECORDS {
            records.pop_front();
        }
        records.push_back(record);
    }

    /// A snapshot of the buffered records, oldest first.
    pub(crate) fn snapshot(&self) -> Vec<LogRecord> {
        self.records
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .iter()
            .cloned()
            .collect()
    }

    /// Drops every buffered record.
    pub(crate) fn clear(&self) {
        self.records
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clear();
    }
}

/// The process-wide log buffer, set when the subscriber is installed.
static LOG_BUFFER: OnceLock<LogBuffer> = OnceLock::new();

/// The shared log buffer, if logging was initialized.
pub(crate) fn log_buffer() -> Option<LogBuffer> {
    LOG_BUFFER.get().cloned()
}

/// Collects the fields of an event into a single readable message.
#[derive(Default)]
struct MessageVisitor {
    message: String,
}

impl tracing::field::Visit for MessageVisitor {
    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
        if field.name() == "message" {
            let _ = write!(self.message, "{value:?}");
        } else {
            if !self.message.is_empty() {
                self.message.push(' ');
            }
            let _ = write!(self.message, "{}={value:?}", field.name());
        }
    }

    fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
        if field.name() == "message" {
            self.message.push_str(value);
        } else {
            if !self.message.is_empty() {
                self.message.push(' ');
            }
            let _ = write!(self.message, "{}={value}", field.name());
        }
    }
}

/// A `tracing` layer that mirrors every event into the in-memory ring buffer.
struct BufferLayer {
    buffer: LogBuffer,
}

impl<S> tracing_subscriber::Layer<S> for BufferLayer
where
    S: tracing::Subscriber,
{
    fn on_event(
        &self,
        event: &tracing::Event<'_>,
        _ctx: tracing_subscriber::layer::Context<'_, S>,
    ) {
        let meta = event.metadata();
        let mut visitor = MessageVisitor::default();
        event.record(&mut visitor);

        self.buffer.push(LogRecord {
            level: *meta.level(),
            target: meta.target().to_string(),
            message: visitor.message,
            timestamp: chrono::Local::now().format("%H:%M:%S").to_string(),
        });
    }
}

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
        let now = Utc::now();
        let meta = event.metadata();

        write!(writer, "[{}] ", now.format("%Y-%m-%d %H:%M:%S"))?;
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

/// Initializes the global tracing subscriber if `MAPACHE_DEBUG_LEVEL` is set to a valid value.
///
/// Reads configuration from environment variables and sets up a file-based logger.
/// Installs the tracing subscriber.
///
/// A file logger is only created when `MAPACHE_DEBUG_LEVEL` names a valid
/// level. Either way, the in-memory ring buffer is always installed (at
/// `info`, or the configured level) so the TUI log overlay has data to show.
pub(crate) fn init_debugger() {
    let configured = get_envvar(DEBUG_LEVEL_ENVVAR)
        .filter(|val| !val.is_empty())
        .and_then(|val| parse_level(&val));

    let level = configured.unwrap_or(tracing::Level::INFO);
    let filter = LevelFilter::from(level);

    let buffer = LogBuffer::default();
    let _ = LOG_BUFFER.set(buffer.clone());

    let registry = tracing_subscriber::registry()
        .with(filter)
        .with(BufferLayer { buffer });

    let Some(_) = configured else {
        // No file logging requested; the ring buffer alone is enough.
        let _ = registry.try_init();
        return;
    };

    let debug_path = match get_envvar(DEBUG_PATH_ENVVAR) {
        Some(p) if !p.is_empty() => PathBuf::from(p),
        _ => PathBuf::from("."),
    };

    let now = chrono::Local::now();
    let ts = now.format("%Y%m%d_%H%M%S");
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

#[cfg(test)]
mod tests {
    use super::*;

    fn record(message: &str) -> LogRecord {
        LogRecord {
            level: tracing::Level::INFO,
            target: "test".to_string(),
            message: message.to_string(),
            timestamp: "00:00:00".to_string(),
        }
    }

    #[test]
    fn buffer_keeps_only_the_newest_records() {
        let buffer = LogBuffer::default();
        for i in 0..MAX_LOG_RECORDS + 5 {
            buffer.push(record(&format!("m{i}")));
        }

        let snapshot = buffer.snapshot();
        assert_eq!(snapshot.len(), MAX_LOG_RECORDS);
        assert_eq!(snapshot.first().unwrap().message, "m5");

        buffer.clear();
        assert!(buffer.snapshot().is_empty());
    }
}
