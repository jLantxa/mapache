use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

use crossterm::event::KeyCode;
use tokio::sync::mpsc;

use crate::ui::tui::{app::Transition, background::BackgroundTask};

use super::{ProgressKey, SummaryKey};

/// The state a screen needs to drive one cancellable background task behind
/// its progress view.
///
/// Every action screen (snapshot, restore, verify, clean) would otherwise own
/// the same cancellation flag, progress-event channel and task handle, plus the
/// completion polling and the progress/summary key handling. Bundling them here
/// keeps that machinery identical across screens.
pub(crate) struct TaskSession<E, T> {
    signal: Arc<AtomicBool>,
    events: Option<mpsc::UnboundedReceiver<E>>,
    task: Option<BackgroundTask<T>>,
}

impl<E, T: Send + 'static> TaskSession<E, T> {
    pub fn new() -> Self {
        Self {
            signal: Arc::new(AtomicBool::new(false)),
            events: None,
            task: None,
        }
    }

    /// A clone of the cancellation flag, to hand to the worker and to read from
    /// inside it.
    pub fn signal(&self) -> Arc<AtomicBool> {
        self.signal.clone()
    }

    /// Clears the cancellation flag and installs the progress-event stream for
    /// a new run.
    pub fn begin(&mut self, events: mpsc::UnboundedReceiver<E>) {
        self.events = Some(events);
        self.signal.store(false, Ordering::SeqCst);
    }

    /// Installs the spawned worker.
    pub fn set_task(&mut self, task: BackgroundTask<T>) {
        self.task = Some(task);
    }

    /// Whether a worker has been spawned.
    #[cfg(test)]
    pub fn is_running(&self) -> bool {
        self.task.is_some()
    }

    /// Drains buffered progress events into `on_event`, then returns the
    /// worker's result once it has finished, or `None` while it is still
    /// running. Events are always delivered before completion is reported, so
    /// the last progress update is reflected first.
    pub fn poll(&mut self, mut on_event: impl FnMut(E)) -> Option<Result<T, String>> {
        if let Some(events) = &mut self.events {
            while let Ok(event) = events.try_recv() {
                on_event(event);
            }
        }
        self.task.as_mut()?.poll()
    }

    /// Applies the shared progress-view keys: `Esc` asks the worker to stop.
    /// `cancelling` mirrors the request in the screen's progress display.
    /// (`q` is handled globally by `App`, which pops the screen and cancels.)
    pub fn handle_progress_key(
        &mut self,
        key: KeyCode,
        cancelling: &mut bool,
    ) -> Option<Transition> {
        match ProgressKey::from_key(key) {
            ProgressKey::Cancel => {
                self.request_shutdown();
                *cancelling = true;
                None
            }
            ProgressKey::None => None,
        }
    }

    /// Applies the shared summary-view keys: `Enter`/`Esc` go back.
    pub fn handle_summary_key(&mut self, key: KeyCode) -> Option<Transition> {
        match SummaryKey::from_key(key) {
            SummaryKey::Done => Some(Transition::Pop),
            SummaryKey::None => None,
        }
    }

    /// Asks the running worker to stop cooperatively.
    pub fn request_shutdown(&self) {
        self.signal.store(true, Ordering::SeqCst);
    }

    /// Cancels and awaits the worker, if one is running.
    pub async fn shutdown(&mut self) {
        if let Some(task) = &mut self.task {
            task.shutdown().await;
        }
    }
}

impl<E, T: Send + 'static> Default for TaskSession<E, T> {
    fn default() -> Self {
        Self::new()
    }
}
