use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use async_trait::async_trait;
use crossterm::event::{
    self, Event, KeyCode, KeyEvent, KeyEventKind, KeyModifiers, MouseButton, MouseEvent,
    MouseEventKind,
};
use ratatui::{Frame, Terminal, backend::Backend, layout::Rect, style::Style, widgets::Block};

use crate::{
    commands::{cmd_forget::CmdArgs as ForgetCmdArgs, cmd_snapshot::CmdArgs as SnapshotCmdArgs},
    common::error::{MapacheError, Result},
    repository::{lock::LockHandle, repo::Repository},
    ui::{
        debug::LogBuffer,
        tui::{
            help, logs,
            screens::dashboard::DashboardScreen,
            theme,
            widgets::{ToastQueue, ToastSink},
        },
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

    /// Whether this screen is animating — a spinner is spinning or a
    /// background task is running. While nothing animates the run loop redraws
    /// slowly instead of every frame, so an idle TUI does not burn CPU.
    fn wants_animation(&self) -> bool {
        false
    }
}

pub enum Transition {
    Push(Box<dyn Screen>),
    Pop,
    Quit,
}

/// A modal panel drawn over the active screen. Only one can be open at a time;
/// while one is open it captures keyboard and mouse input.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
enum Overlay {
    #[default]
    None,
    Help,
    Notifications,
    Logs,
}

pub struct App {
    stack: Vec<Box<dyn Screen>>,
    should_quit: bool,
    toasts: ToastSink,
    overlay: Overlay,
    overlay_scroll: u16,
    /// Area of the last rendered frame, used to hit-test toast clicks.
    last_area: Rect,
    log_buffer: Option<LogBuffer>,
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
            overlay: Overlay::None,
            overlay_scroll: 0,
            last_area: Rect::default(),
            log_buffer: crate::ui::debug::log_buffer(),
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

        // Redraw every frame only while something is animated (a spinner or a
        // running background task); otherwise wait longer between frames. The
        // slow idle tick still prunes fading notifications and polls any
        // background result, so no work is missed.
        while !self.should_quit {
            let animating = self
                .stack
                .last()
                .is_some_and(|screen| screen.wants_animation());
            let toasts_pending = self.toasts.with_queue(|queue| !queue.is_empty());
            let active = animating || toasts_pending;

            terminal
                .draw(|frame| self.render(frame))
                .map_err(|e| MapacheError::Internal(format!("terminal error: {e}")))?;

            let timeout = if active {
                Duration::from_millis(100)
            } else {
                Duration::from_millis(1000)
            };

            if event::poll(timeout)? {
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
            let key = KeyEvent::new(code, KeyModifiers::NONE);
            if self.overlay == Overlay::None {
                self.handle_key(key).await;
            } else {
                self.handle_overlay_key(key);
            }
            return;
        }

        if matches!(mouse.kind, MouseEventKind::Down(MouseButton::Left)) {
            // While an overlay is open it swallows clicks.
            if self.overlay != Overlay::None {
                return;
            }

            // A click on a toast dismisses it before anything else sees it.
            let area = self.last_area;
            if self
                .toasts
                .with_queue(|queue| queue.dismiss_at(area, mouse.column, mouse.row))
            {
                return;
            }

            if let Some(active) = self.stack.last_mut() {
                active.handle_mouse(mouse).await;
            }
        }
    }

    /// Opens `overlay`, or closes it if it is already open.
    fn toggle_overlay(&mut self, overlay: Overlay) {
        self.overlay = if self.overlay == overlay {
            Overlay::None
        } else {
            overlay
        };
        self.overlay_scroll = 0;
    }

    fn close_overlay(&mut self) {
        self.overlay = Overlay::None;
        self.overlay_scroll = 0;
    }

    fn scroll_overlay(&mut self, delta: i32) {
        let next = self.overlay_scroll as i32 + delta;
        self.overlay_scroll = next.max(0) as u16;
    }

    /// Handles a key while an overlay is open. Always consumes the key.
    fn handle_overlay_key(&mut self, key: KeyEvent) {
        match key.code {
            KeyCode::Esc | KeyCode::Char('q') => self.close_overlay(),
            KeyCode::Up | KeyCode::Char('k') => self.scroll_overlay(-1),
            KeyCode::Down | KeyCode::Char('j') => self.scroll_overlay(1),
            KeyCode::PageUp => self.scroll_overlay(-10),
            KeyCode::PageDown => self.scroll_overlay(10),
            KeyCode::Home => self.overlay_scroll = 0,
            KeyCode::End => self.overlay_scroll = u16::MAX,
            KeyCode::Char('c') => match self.overlay {
                Overlay::Notifications => self.toasts.clear(),
                Overlay::Logs => {
                    if let Some(buffer) = &self.log_buffer {
                        buffer.clear();
                    }
                }
                _ => {}
            },
            // `?` / `F1` closes the help overlay it opened.
            KeyCode::Char('?') | KeyCode::F(1) if self.overlay == Overlay::Help => {
                self.close_overlay();
            }
            _ => {}
        }
    }

    async fn handle_key(&mut self, key: KeyEvent) {
        // Ctrl+C always quits, whatever the screen (or an overlay) is doing.
        // The run loop shuts the screens down on the way out.
        if key.modifiers.contains(KeyModifiers::CONTROL) && key.code == KeyCode::Char('c') {
            self.should_quit = true;
            return;
        }

        // Global overlay toggles work from anywhere, including inside an
        // overlay (where they switch or close it).
        if key.modifiers.contains(KeyModifiers::CONTROL) {
            match key.code {
                KeyCode::Char('n') => {
                    self.toggle_overlay(Overlay::Notifications);
                    return;
                }
                KeyCode::Char('l') => {
                    self.toggle_overlay(Overlay::Logs);
                    return;
                }
                _ => {}
            }
        }

        // While an overlay is open it owns every remaining key.
        if self.overlay != Overlay::None {
            self.handle_overlay_key(key);
            return;
        }

        // `?` (or `F1`) opens the help overlay — unless the screen is
        // collecting text, where `?` belongs to the input.
        if key.code == KeyCode::F(1)
            || (key.code == KeyCode::Char('?')
                && !self.stack.last().is_some_and(|s| s.text_input_active()))
        {
            self.toggle_overlay(Overlay::Help);
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
        self.last_area = area;
        frame.render_widget(
            Block::default().style(Style::default().bg(theme::THEME.bg)),
            area,
        );

        if let Some(active) = self.stack.last_mut() {
            active.render(frame);
        }

        self.toasts.with_queue(|queue: &mut ToastQueue| {
            queue.prune(Instant::now());
            queue.render(area, frame);
        });

        match self.overlay {
            Overlay::None => {}
            Overlay::Help => {
                let hints = self
                    .stack
                    .last()
                    .map(|s| s.help_hints())
                    .unwrap_or_default();
                help::render(frame, &hints, &mut self.overlay_scroll);
            }
            Overlay::Notifications => {
                let mut scroll = self.overlay_scroll;
                self.toasts
                    .with_queue(|queue| queue.render_center(area, frame, &mut scroll));
                self.overlay_scroll = scroll;
            }
            Overlay::Logs => {
                let records = self
                    .log_buffer
                    .as_ref()
                    .map(|buffer| buffer.snapshot())
                    .unwrap_or_default();
                logs::render(frame, &records, &mut self.overlay_scroll);
            }
        }
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
            overlay: if help_visible {
                Overlay::Help
            } else {
                Overlay::None
            },
            overlay_scroll: 0,
            last_area: Rect::default(),
            log_buffer: None,
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
            overlay: Overlay::Help,
            overlay_scroll: 0,
            last_area: Rect::default(),
            log_buffer: None,
        };

        app.handle_key(KeyEvent::new(KeyCode::Char('j'), KeyModifiers::NONE))
            .await;
        assert_eq!(app.overlay_scroll, 1);
        app.handle_key(KeyEvent::new(KeyCode::Down, KeyModifiers::NONE))
            .await;
        assert_eq!(app.overlay_scroll, 2);
        app.handle_key(KeyEvent::new(KeyCode::PageDown, KeyModifiers::NONE))
            .await;
        assert_eq!(app.overlay_scroll, 12);
        app.handle_key(KeyEvent::new(KeyCode::Char('k'), KeyModifiers::NONE))
            .await;
        assert_eq!(app.overlay_scroll, 11);
        app.handle_key(KeyEvent::new(KeyCode::Home, KeyModifiers::NONE))
            .await;
        assert_eq!(app.overlay_scroll, 0);

        // Closing the overlay resets the offset so the next opening starts at
        // the top.
        app.handle_key(KeyEvent::new(KeyCode::Esc, KeyModifiers::NONE))
            .await;
        assert_eq!(app.overlay, Overlay::None);
        assert_eq!(app.overlay_scroll, 0);
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
            overlay: Overlay::None,
            overlay_scroll: 0,
            last_area: Rect::default(),
            log_buffer: None,
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
            overlay: Overlay::None,
            overlay_scroll: 0,
            last_area: Rect::default(),
            log_buffer: None,
        };

        app.handle_key(KeyEvent::new(KeyCode::Char('q'), KeyModifiers::NONE))
            .await;
        assert!(app.should_quit, "q on the dashboard quits mapache");
    }

    #[tokio::test]
    async fn overlay_toggles_switch_and_close() {
        let (mut app, _finished) = worker_app(false);

        app.handle_key(KeyEvent::new(KeyCode::Char('n'), KeyModifiers::CONTROL))
            .await;
        assert_eq!(app.overlay, Overlay::Notifications);
        app.handle_key(KeyEvent::new(KeyCode::Char('n'), KeyModifiers::CONTROL))
            .await;
        assert_eq!(
            app.overlay,
            Overlay::None,
            "Ctrl+N toggles the centre closed"
        );

        app.handle_key(KeyEvent::new(KeyCode::Char('l'), KeyModifiers::CONTROL))
            .await;
        assert_eq!(app.overlay, Overlay::Logs);
        app.handle_key(KeyEvent::new(KeyCode::Char('n'), KeyModifiers::CONTROL))
            .await;
        assert_eq!(
            app.overlay,
            Overlay::Notifications,
            "toggles switch overlay"
        );

        // An overlay swallows `q` instead of quitting.
        app.handle_key(KeyEvent::new(KeyCode::Char('q'), KeyModifiers::NONE))
            .await;
        assert_eq!(app.overlay, Overlay::None);
        assert!(!app.should_quit);
    }

    #[tokio::test]
    async fn ctrl_n_and_ctrl_l_are_not_sent_to_screens() {
        // A screen that quits on any key would end the app if these leaked
        // through; instead the overlay opens.
        let (mut app, _finished) = worker_app(false);
        app.handle_key(KeyEvent::new(KeyCode::Char('l'), KeyModifiers::CONTROL))
            .await;
        assert_eq!(app.overlay, Overlay::Logs);
        assert!(!app.should_quit);
    }
}
