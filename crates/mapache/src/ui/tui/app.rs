use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use async_trait::async_trait;
use crossterm::event::{
    self, Event, KeyCode, KeyEvent, KeyEventKind, KeyModifiers, MouseButton, MouseEvent,
    MouseEventKind,
};
use ratatui::{Frame, Terminal, backend::Backend, style::Style, widgets::Block};

use crate::{
    commands::{cmd_forget::CmdArgs as ForgetCmdArgs, cmd_snapshot::CmdArgs as SnapshotCmdArgs},
    common::error::{MapacheError, Result},
    repository::{lock::LockHandle, repo::Repository},
    ui::tui::{
        help,
        screens::dashboard::DashboardScreen,
        theme,
        widgets::{ToastQueue, ToastSink},
    },
};

#[async_trait]
pub trait Screen: Send {
    fn render(&mut self, frame: &mut Frame);
    async fn handle_key(&mut self, key: KeyEvent) -> Option<Transition>;
    async fn on_become_active(&mut self) -> Result<()> {
        Ok(())
    }

    fn request_shutdown(&mut self) {}

    async fn shutdown(&mut self) {}

    /// Key hints for the help overlay (`?` / `F1`), mirroring the keys this
    /// screen currently accepts.
    fn help_hints(&self) -> Vec<(&'static str, &'static str)> {
        Vec::new()
    }

    /// Whether a text input currently has focus. Used to keep the global
    /// help binding out of the way while the user is typing.
    fn text_input_active(&self) -> bool {
        false
    }

    /// Handles a mouse event (typically a left click used to select an item)
    /// and returns whether the event was consumed. The default ignores the
    /// mouse; list/table screens override this to select the item under the
    /// cursor.
    async fn handle_mouse(&mut self, _mouse: MouseEvent) -> bool {
        false
    }

    /// Receives the shared notification sink when the screen joins the
    /// stack. Screens that can surface errors override this.
    fn attach_toasts(&mut self, _sink: ToastSink) {}
}

pub enum Transition {
    Push(Box<dyn Screen>),
    Pop,
    Quit,
}

pub struct App {
    stack: Vec<Box<dyn Screen>>,
    should_quit: bool,
    toasts: ToastSink,
    help_visible: bool,
    help_scroll: u16,
}

impl App {
    pub fn new(
        repo: Arc<Repository>,
        lock_handle: Option<LockHandle>,
        repo_path: String,
        snapshot_config: Option<SnapshotCmdArgs>,
        forget_config: Option<ForgetCmdArgs>,
    ) -> Self {
        let repo_id = repo.manifest().id().to_hex();
        let dashboard = DashboardScreen::new(
            repo,
            lock_handle,
            repo_path,
            repo_id,
            snapshot_config,
            forget_config,
        );
        let toasts = ToastSink::default();
        let mut stack: Vec<Box<dyn Screen>> = vec![Box::new(dashboard)];
        if let Some(screen) = stack.last_mut() {
            screen.attach_toasts(toasts.clone());
        }
        Self {
            stack,
            should_quit: false,
            toasts,
            help_visible: false,
            help_scroll: 0,
        }
    }

    pub async fn run<B: Backend>(&mut self, terminal: &mut Terminal<B>) -> Result<()>
    where
        <B as Backend>::Error: Send + Sync + 'static,
    {
        let result = self.run_loop(terminal).await;
        self.shutdown_screens().await;
        result
    }

    async fn shutdown_screens(&mut self) {
        for screen in &mut self.stack {
            screen.request_shutdown();
        }
        for screen in self.stack.iter_mut().rev() {
            screen.shutdown().await;
        }
    }

    async fn run_loop<B: Backend>(&mut self, terminal: &mut Terminal<B>) -> Result<()>
    where
        <B as Backend>::Error: Send + Sync + 'static,
    {
        if let Some(screen) = self.stack.last_mut() {
            screen.on_become_active().await?;
        }

        while !self.should_quit {
            terminal
                .draw(|frame| self.render(frame))
                .map_err(|e| MapacheError::Internal(format!("terminal error: {e}")))?;

            if event::poll(Duration::from_millis(100))? {
                match event::read()? {
                    Event::Key(key) if key.kind != KeyEventKind::Release => {
                        self.handle_key(key).await;
                    }
                    Event::Mouse(mouse) => self.handle_mouse(mouse).await,
                    _ => {}
                }
            }
        }

        Ok(())
    }

    /// Translates mouse events into the key events the screens already
    /// understand: the wheel scrolls by moving the selection one line, and a
    /// left click is forwarded to the active screen.
    async fn handle_mouse(&mut self, mouse: MouseEvent) {
        let code = match mouse.kind {
            MouseEventKind::ScrollUp => Some(KeyCode::Up),
            MouseEventKind::ScrollDown => Some(KeyCode::Down),
            _ => None,
        };
        if let Some(code) = code {
            self.handle_key(KeyEvent::new(code, KeyModifiers::NONE))
                .await;
            return;
        }

        if matches!(mouse.kind, MouseEventKind::Down(MouseButton::Left))
            && let Some(active) = self.stack.last_mut()
        {
            active.handle_mouse(mouse).await;
        }
    }

    async fn handle_key(&mut self, key: KeyEvent) {
        // Ctrl+C always quits, whatever the screen (or the help overlay)
        // is doing. The run loop shuts the screens down on the way out.
        if key.modifiers.contains(KeyModifiers::CONTROL) && key.code == KeyCode::Char('c') {
            self.should_quit = true;
            return;
        }

        if self.help_visible {
            match key.code {
                KeyCode::Esc | KeyCode::Char('?') | KeyCode::F(1) => {
                    self.help_visible = false;
                    self.help_scroll = 0;
                }
                KeyCode::Up | KeyCode::Char('k') => {
                    self.help_scroll = self.help_scroll.saturating_sub(1);
                }
                KeyCode::Down | KeyCode::Char('j') => {
                    self.help_scroll = self.help_scroll.saturating_add(1);
                }
                KeyCode::PageUp => {
                    self.help_scroll = self.help_scroll.saturating_sub(10);
                }
                KeyCode::PageDown => {
                    self.help_scroll = self.help_scroll.saturating_add(10);
                }
                KeyCode::Home => self.help_scroll = 0,
                KeyCode::End => self.help_scroll = u16::MAX,
                _ => {}
            }
            return;
        }

        // `?` (or `F1`) opens the help overlay — unless the screen is
        // collecting text, where `?` belongs to the input.
        if key.code == KeyCode::F(1)
            || (key.code == KeyCode::Char('?')
                && !self.stack.last().is_some_and(|s| s.text_input_active()))
        {
            self.help_visible = true;
            self.help_scroll = 0;
            return;
        }

        // `q` pops back to the dashboard on any sub-screen; only the
        // dashboard itself quits. While a screen is collecting text the
        // letter belongs to the input.
        if key.code == KeyCode::Char('q')
            && self.stack.len() > 1
            && !self.stack.last().is_some_and(|s| s.text_input_active())
        {
            self.pop_screen().await;
            return;
        }

        let transition = if let Some(active) = self.stack.last_mut() {
            active.handle_key(key).await
        } else {
            None
        };

        if let Some(t) = transition {
            match t {
                Transition::Push(s) => {
                    self.stack.push(s);
                    if let Some(active) = self.stack.last_mut() {
                        active.attach_toasts(self.toasts.clone());
                        if let Err(e) = active.on_become_active().await {
                            tracing::error!("Failed to activate screen: {}", e);
                            self.toasts.error(format!("Failed to activate screen: {e}"));
                        }
                    }
                }
                Transition::Pop => self.pop_screen().await,
                Transition::Quit => self.should_quit = true,
            }
        }
    }

    /// Pops the top screen off the navigation stack, shutting it down and
    /// reactivating the screen below (or quitting once the stack is empty).
    async fn pop_screen(&mut self) {
        if let Some(screen) = self.stack.last_mut() {
            screen.request_shutdown();
            screen.shutdown().await;
        }
        self.stack.pop();
        if self.stack.is_empty() {
            self.should_quit = true;
        } else if let Some(active) = self.stack.last_mut()
            && let Err(e) = active.on_become_active().await
        {
            tracing::error!("Failed to activate screen: {}", e);
            self.toasts.error(format!("Failed to activate screen: {e}"));
        }
    }

    fn render(&mut self, frame: &mut Frame) {
        let area = frame.area();
        frame.render_widget(
            Block::default().style(Style::default().bg(theme::THEME.bg)),
            area,
        );

        if let Some(active) = self.stack.last_mut() {
            active.render(frame);
        }

        if self.help_visible {
            let hints = self
                .stack
                .last()
                .map(|s| s.help_hints())
                .unwrap_or_default();
            help::render(frame, &hints, &mut self.help_scroll);
        }

        self.toasts.with_queue(|queue: &mut ToastQueue| {
            queue.prune(Instant::now());
            queue.render(area, frame);
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ui::tui::background::BackgroundTask;
    use std::sync::atomic::{AtomicBool, Ordering};

    struct WorkerScreen {
        signal: Arc<AtomicBool>,
        task: BackgroundTask<()>,
    }

    #[async_trait]
    impl Screen for WorkerScreen {
        fn render(&mut self, _frame: &mut Frame) {}
        async fn handle_key(&mut self, _key: KeyEvent) -> Option<Transition> {
            Some(Transition::Quit)
        }
        fn request_shutdown(&mut self) {
            self.signal.store(true, Ordering::SeqCst);
        }
        async fn shutdown(&mut self) {
            self.task.shutdown().await;
        }
    }

    /// An app whose only screen runs a cooperative worker that stops when the
    /// screen is shut down. Returns the app and a flag the worker sets once it
    /// has observed the shutdown signal.
    fn worker_app(help_visible: bool) -> (App, Arc<AtomicBool>) {
        let signal = Arc::new(AtomicBool::new(false));
        let worker_signal = signal.clone();
        let finished = Arc::new(AtomicBool::new(false));
        let worker_finished = finished.clone();
        let task = BackgroundTask::spawn_async(signal.clone(), async move {
            while !worker_signal.load(Ordering::SeqCst) {
                tokio::task::yield_now().await;
            }
            worker_finished.store(true, Ordering::SeqCst);
            Ok(())
        });
        let app = App {
            stack: vec![Box::new(WorkerScreen { signal, task })],
            should_quit: false,
            toasts: ToastSink::default(),
            help_visible,
            help_scroll: 0,
        };
        (app, finished)
    }

    #[tokio::test]
    async fn quit_key_requests_quit_even_with_help_open() {
        for key in [
            KeyEvent::new(KeyCode::Char('c'), KeyModifiers::CONTROL),
            KeyEvent::new(KeyCode::Char('q'), KeyModifiers::NONE),
        ] {
            let (mut app, _finished) = worker_app(key.modifiers.contains(KeyModifiers::CONTROL));
            app.handle_key(key).await;
            assert!(app.should_quit);
        }
    }

    #[tokio::test]
    async fn shutdown_waits_for_workers() {
        let (mut app, finished) = worker_app(false);
        app.shutdown_screens().await;
        assert!(finished.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn help_scroll_responds_to_keys_and_resets_on_close() {
        let signal = Arc::new(AtomicBool::new(false));
        let task = BackgroundTask::spawn_async(signal.clone(), async { Ok(()) });
        let mut app = App {
            stack: vec![Box::new(WorkerScreen { signal, task })],
            should_quit: false,
            toasts: ToastSink::default(),
            help_visible: true,
            help_scroll: 0,
        };

        app.handle_key(KeyEvent::new(KeyCode::Char('j'), KeyModifiers::NONE))
            .await;
        assert_eq!(app.help_scroll, 1);
        app.handle_key(KeyEvent::new(KeyCode::Down, KeyModifiers::NONE))
            .await;
        assert_eq!(app.help_scroll, 2);
        app.handle_key(KeyEvent::new(KeyCode::PageDown, KeyModifiers::NONE))
            .await;
        assert_eq!(app.help_scroll, 12);
        app.handle_key(KeyEvent::new(KeyCode::Char('k'), KeyModifiers::NONE))
            .await;
        assert_eq!(app.help_scroll, 11);
        app.handle_key(KeyEvent::new(KeyCode::Home, KeyModifiers::NONE))
            .await;
        assert_eq!(app.help_scroll, 0);

        // Closing the overlay resets the offset so the next opening starts at
        // the top.
        app.handle_key(KeyEvent::new(KeyCode::Esc, KeyModifiers::NONE))
            .await;
        assert!(!app.help_visible);
        assert_eq!(app.help_scroll, 0);
    }

    fn worker_screen() -> Box<dyn Screen> {
        let signal = Arc::new(AtomicBool::new(false));
        let task = BackgroundTask::spawn_async(signal.clone(), async { Ok(()) });
        Box::new(WorkerScreen { signal, task })
    }

    #[tokio::test]
    async fn q_pops_sub_screens_instead_of_quitting() {
        let mut app = App {
            stack: vec![worker_screen(), worker_screen()],
            should_quit: false,
            toasts: ToastSink::default(),
            help_visible: false,
            help_scroll: 0,
        };

        app.handle_key(KeyEvent::new(KeyCode::Char('q'), KeyModifiers::NONE))
            .await;
        assert_eq!(app.stack.len(), 1, "q should pop the sub-screen");
        assert!(!app.should_quit, "popping must not quit the app");
    }

    #[tokio::test]
    async fn q_still_quits_on_the_dashboard() {
        let mut app = App {
            stack: vec![worker_screen()],
            should_quit: false,
            toasts: ToastSink::default(),
            help_visible: false,
            help_scroll: 0,
        };

        app.handle_key(KeyEvent::new(KeyCode::Char('q'), KeyModifiers::NONE))
            .await;
        assert!(app.should_quit, "q on the dashboard quits mapache");
    }
}
