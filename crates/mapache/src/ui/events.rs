use std::{path::PathBuf, sync::Arc};

use crate::{archiver::progress::SnapshotProcessSummary, fs::tree::NodeDiff};

/// Top-level event enum dispatched to all consumers.
/// Each variant wraps a domain-specific event.
#[derive(Debug, Clone)]
pub enum Event {
    Backup(BackupEvent),
    Restore(RestoreEvent),
    Gc(GcEvent),
    Task(TaskEvent),
}

/// Thread-safe, cloneable sender of events.
pub type EventSender = Arc<dyn Fn(Event) + Send + Sync>;

/// A no-op event sender that discards all events.
pub fn noop_sender() -> EventSender {
    Arc::new(|_| {})
}

/// Helper to emit an event through the sender.
#[inline]
pub fn emit_event(sender: &EventSender, event: Event) {
    sender(event);
}

// ---------------------------------------------------------------------------
// Backup
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub enum BackupEvent {
    /// Background file system scan started.
    ScanStarted,
    /// Progress during the file system scan.
    ScanProgress { items: u64, bytes: u64 },
    /// Scan finished; total items and bytes are now known.
    ScanFinished { total_items: u64, total_bytes: u64 },
    /// Starting to process a single node (file / dir).
    NodeProcessing {
        path: PathBuf,
        diff: NodeDiff,
        size_hint: Option<u64>,
    },
    /// A single node has been fully processed.
    NodeProcessed {
        path: PathBuf,
        diff: NodeDiff,
        size_hint: Option<u64>,
    },
    /// A number of bytes has been read and stored.
    BytesProcessed(u64),
    /// Non-fatal warning.
    Warning(String),
    /// Recoverable error (file skipped, etc.).
    Error(String),
    /// Informational log message.
    Log(String),
    /// Snapshot is complete with the final summary.
    Finished(SnapshotProcessSummary),
}

// ---------------------------------------------------------------------------
// Restore
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub enum RestoreEvent {
    /// Planning phase started.
    Planning,
    /// A node was visited during planning.
    NodeVisited(u64),
    /// Plan is complete; totals are known.
    PlanBuilt { total_items: u64, total_bytes: u64 },
    /// A single item has been restored or skipped.
    ItemProcessed(PathBuf),
    /// A number of bytes written.
    BytesProcessed(u64),
    /// Non-fatal warning.
    Warning(String),
    /// Recoverable error.
    Error(String),
    /// Informational log message.
    Log(String),
    /// Restore finished.
    Finished,
}

// ---------------------------------------------------------------------------
// Garbage Collection
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub enum GcEvent {
    /// Task progress with optional total for determinate progress.
    TaskProgress {
        kind: GcTaskKind,
        pos: u64,
        total: Option<u64>,
    },
    /// A task is finished (bar should be cleared).
    TaskFinished { kind: GcTaskKind },
    /// Non-fatal warning.
    Warning(String),
    /// Recoverable error.
    Error(String),
    /// Informational log message.
    Log(String),
    /// GC finished with size info.
    Finished {
        added_bytes: u64,
        deleted_bytes: u64,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GcTaskKind {
    SearchingReferencedBlobs,
    FindingObsoleteBlobs,
    CheckingGarbageLevels,
    DeletingUnusedPacks,
    RepackingBlobs,
    DeletingOldIndices,
    DeletingObsoletePacks,
}

// ---------------------------------------------------------------------------
// Generic task (verify, sync, rebuild-index, stats)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub enum TaskEvent {
    /// A named phase started (with optional total items).
    Started {
        name: &'static str,
        total: Option<u64>,
    },
    /// Progress within the current phase.
    Progress { pos: u64, message: Option<String> },
    /// Current phase finished.
    Finished,
    /// Non-fatal warning.
    Warning(String),
    /// Recoverable error.
    Error(String),
    /// Informational log message.
    Log(String),
}
