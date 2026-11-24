use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use async_trait::async_trait;
use crossterm::event::{
    self, Event, KeyCode, KeyEvent, KeyEventKind, KeyModifiers, MouseButton, MouseEvent,
    MouseEventKind,
};
use ratatui::{
    Frame, Terminal,
    backend::Backend,
    layout::{Alignment, Rect},
    style::{Modifier, Style},
    text::{Line, Span, Text},
    widgets::{Block, Clear, Paragraph, Wrap},
};

use crate::{
    commands::{cmd_forget::CmdArgs as ForgetCmdArgs, cmd_snapshot::CmdArgs as SnapshotCmdArgs},
    common::error::{MapacheError, Result},
    repository::{lock::LockHandle, repo::Repository},
    ui::tui::{
        PaletteCommand, help,
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

    /// Key hints for the help overlay (`?` / `F1`) and the shared status bar,
    /// mirroring the keys this screen currently accepts.
    fn help_hints(&self) -> Vec<(&'static str, &'static str)> {
        Vec::new()
    }

    /// Actions this screen contributes to the command palette (`Ctrl+K`).
    fn palette_commands(&self) -> Vec<PaletteCommand> {
        Vec::new()
    }

    /// Runs a command previously returned by [`Screen::palette_commands`].
    async fn execute_palette_command(&mut self, _id: &str) -> Option<Transition> {
        None
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

/// Smallest terminal the screens are laid out for. Below this columns and
/// borders would clip, so the app shows a hint instead of a broken layout.
const MIN_TERMINAL_WIDTH: u16 = 80;
const MIN_TERMINAL_HEIGHT: u16 = 24;

/// Draws a centred hint when the terminal is smaller than the screens need.
fn render_too_small(frame: &mut Frame, area: Rect, min_width: u16, min_height: u16) {
    let lines = vec![
        Line::from(Span::styled("Terminal too small", theme::THEME.header)),
        Line::from(""),
        Line::from(Span::styled(
            format!(
                "needs at least {min_width}\u{d7}{min_height}, current {}x{}",
                area.width, area.height
            ),
            theme::THEME.footer,
        )),
        Line::from(Span::styled(
            "resize the window, or press q / Ctrl+C to quit",
            theme::THEME.footer,
        )),
    ];
    let height = lines.len() as u16;
    let target = Rect {
        y: area.y + area.height.saturating_sub(height) / 2,
        height: height.min(area.height),
        ..area
    };
    frame.render_widget(
        Paragraph::new(Text::from(lines))
            .alignment(Alignment::Center)
            .wrap(Wrap { trim: true }),
        target,
    );
}

/// Case-insensitive subsequence match, so `snp` finds `Snapshot`.
fn fuzzy_match(query: &str, label: &str) -> bool {
    let mut label = label.chars();
    query.chars().all(|c| label.any(|l| l == c))
}

/// A modal panel drawn over the active screen. Only one can be open at a time;
/// while one is open it captures keyboard and mouse input.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
enum Overlay {
    #[default]
    None,
    Help,
    Notifications,
    CommandPalette,
}

/// Commands available from any screen in the command palette.
const GLOBAL_PALETTE_COMMANDS: &[PaletteCommand] = &[PaletteCommand {
    id: "global:help",
    label: "Show help",
}];

/// State of the fuzzy command palette.
#[derive(Default)]
struct PaletteState {
    query: String,
    selected: usize,
}

pub struct App {
    stack: Vec<Box<dyn Screen>>,
    should_quit: bool,
    toasts: ToastSink,
    overlay: Overlay,
    overlay_scroll: u16,
    palette: PaletteState,
    /// Area of the last rendered frame, used to hit-test toast clicks.
    last_area: Rect,
    /// Row of the last left-button press, used to turn a drag into scrolling.
    drag_row: Option<u16>,
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
            palette: PaletteState::default(),
            last_area: Rect::default(),
            drag_row: None,
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

        // Redraw every frame only while something is animated (a spinner);
        // otherwise wait longer between frames. The slow idle tick still
        // prunes fading notifications and polls any background result, so no
        // work is missed.
        while !self.should_quit {
            let animating = self.stack.iter().any(|screen| screen.wants_animation());
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

    /// Sends a navigation key to the overlay when one is open, otherwise to
    /// the active screen. Mouse wheel and drag scrolling share this path so
    /// screens do not need to know where the event came from.
    async fn route_navigation_key(&mut self, code: KeyCode) {
        let key = KeyEvent::new(code, KeyModifiers::NONE);
        if self.overlay == Overlay::None {
            self.handle_key(key).await;
        } else {
            self.handle_overlay_key(key).await;
        }
    }

    /// Translates mouse events into the key events the screens already
    /// understand: the wheel and a vertical left-button drag scroll, while
    /// other clicks reach the screen.
    async fn handle_mouse(&mut self, mouse: MouseEvent) {
        match mouse.kind {
            MouseEventKind::ScrollUp => {
                self.route_navigation_key(KeyCode::Up).await;
            }
            MouseEventKind::ScrollDown => {
                self.route_navigation_key(KeyCode::Down).await;
            }
            MouseEventKind::Down(MouseButton::Left) => {
                self.drag_row = Some(mouse.row);

                // Interacting with the UI dismisses transient notifications.
                let area = self.last_area;
                let clicked_toast = self.toasts.with_queue(|queue| {
                    let clicked = queue.dismiss_at(area, mouse.column, mouse.row);
                    queue.dismiss_all();
                    clicked
                });

                // While an overlay is open it swallows clicks.
                if self.overlay == Overlay::None
                    && !clicked_toast
                    && let Some(active) = self.stack.last_mut()
                {
                    active.handle_mouse(mouse).await;
                }
            }
            MouseEventKind::Drag(MouseButton::Left) => {
                if let Some(previous) = self.drag_row {
                    let delta = i32::from(mouse.row) - i32::from(previous);
                    self.drag_row = Some(mouse.row);
                    if delta != 0 {
                        let code = if delta > 0 {
                            KeyCode::Down
                        } else {
                            KeyCode::Up
                        };
                        for _ in 0..delta.unsigned_abs().min(20) {
                            self.route_navigation_key(code).await;
                        }
                    }
                }
            }
            MouseEventKind::Up(MouseButton::Left) => {
                self.drag_row = None;
            }
            _ => {}
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
        if self.overlay == Overlay::CommandPalette {
            self.palette.query.clear();
            self.palette.selected = 0;
        }
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
    async fn handle_overlay_key(&mut self, key: KeyEvent) {
        // One global exit path for every modal: Esc means "close this panel",
        // not "leave the input".
        if key.code == KeyCode::Esc {
            self.close_overlay();
            return;
        }

        match self.overlay {
            Overlay::CommandPalette => {
                if key.code == KeyCode::F(1) {
                    self.close_overlay();
                } else {
                    self.handle_palette_key(key).await;
                }
            }
            _ => match key.code {
                KeyCode::Char('q') => self.close_overlay(),
                KeyCode::Up | KeyCode::Char('k') => self.scroll_overlay(-1),
                KeyCode::Down | KeyCode::Char('j') => self.scroll_overlay(1),
                KeyCode::PageUp => self.scroll_overlay(-10),
                KeyCode::PageDown => self.scroll_overlay(10),
                KeyCode::Home => self.overlay_scroll = 0,
                KeyCode::End => self.overlay_scroll = u16::MAX,
                KeyCode::Char('c') if self.overlay == Overlay::Notifications => self.toasts.clear(),
                // `?` / `F1` closes the help overlay it opened.
                KeyCode::Char('?') | KeyCode::F(1) if self.overlay == Overlay::Help => {
                    self.close_overlay();
                }
                _ => {}
            },
        }
    }

    async fn handle_key(&mut self, key: KeyEvent) {
        // Interacting with the UI dismisses transient notifications. The key
        // still reaches the screen so it is not stolen from the user.
        self.toasts.with_queue(|queue| queue.dismiss_all());

        // Ctrl+C always quits, whatever the screen (or an overlay) is doing.
        // The run loop shuts the screens down on the way out.
        if key.modifiers.contains(KeyModifiers::CONTROL) && key.code == KeyCode::Char('c') {
            self.should_quit = true;
            return;
        }

        // Global overlay toggles work from anywhere, including inside an
        // overlay (where they switch or close it). Ctrl+N is reserved for
        // notifications.
        if key.modifiers.contains(KeyModifiers::CONTROL) && key.code == KeyCode::Char('k') {
            self.toggle_overlay(Overlay::CommandPalette);
            return;
        }
        if key.modifiers.contains(KeyModifiers::CONTROL) && key.code == KeyCode::Char('n') {
            self.toggle_overlay(Overlay::Notifications);
            return;
        }

        // While an overlay is open it owns every remaining key.
        if self.overlay != Overlay::None {
            self.handle_overlay_key(key).await;
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
            self.apply_transition(t).await;
        }
    }

    async fn apply_transition(&mut self, transition: Transition) {
        match transition {
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

    fn palette_commands(&self) -> Vec<PaletteCommand> {
        let mut commands = GLOBAL_PALETTE_COMMANDS.to_vec();
        if let Some(active) = self.stack.last() {
            commands.extend(active.palette_commands());
        }
        let query = self.palette.query.trim().to_lowercase();
        if query.is_empty() {
            return commands;
        }
        commands
            .into_iter()
            .filter(|command| fuzzy_match(&query, &command.label.to_lowercase()))
            .collect()
    }

    async fn handle_palette_key(&mut self, key: KeyEvent) {
        let count = self.palette_commands().len();
        match key.code {
            KeyCode::Up | KeyCode::Char('k') if count > 0 => {
                self.palette.selected = (self.palette.selected + count - 1) % count;
            }
            KeyCode::Down | KeyCode::Char('j') if count > 0 => {
                self.palette.selected = (self.palette.selected + 1) % count;
            }
            KeyCode::Backspace => {
                self.palette.query.pop();
                self.palette.selected = 0;
            }
            KeyCode::Char(c) => {
                self.palette.query.push(c);
                self.palette.selected = 0;
            }
            KeyCode::Enter => {
                let command = self.palette_commands().get(self.palette.selected).copied();
                self.close_overlay();
                if let Some(command) = command {
                    match command.id {
                        "global:help" => self.toggle_overlay(Overlay::Help),
                        id => {
                            let transition = if let Some(active) = self.stack.last_mut() {
                                active.execute_palette_command(id).await
                            } else {
                                None
                            };
                            if let Some(transition) = transition {
                                self.apply_transition(transition).await;
                            }
                        }
                    }
                }
            }
            _ => {}
        }
    }

    fn render_palette(&self, frame: &mut Frame) {
        let area = frame.area();
        let commands = self.palette_commands();
        let height = (commands.len() as u16 + 6).min(area.height).max(6);
        let popup = crate::ui::tui::widgets::centered_rect(area, area.width.min(72), height);
        let block = theme::block("Commands");
        let inner = block.inner(popup);

        frame.render_widget(Clear, popup);
        frame.render_widget(block, popup);

        let mut lines = vec![Line::from(vec![
            Span::styled("> ", theme::THEME.header),
            Span::raw(self.palette.query.clone()),
        ])];
        for (index, command) in commands.iter().enumerate() {
            let selected = index == self.palette.selected;
            let style = if selected {
                Style::new()
                    .fg(theme::THEME.subtext)
                    .add_modifier(Modifier::BOLD)
                    .bg(theme::THEME.surface)
            } else {
                Style::new().fg(theme::THEME.subtext)
            };
            lines.push(Line::from(Span::styled(
                format!(
                    " {} {}",
                    if selected { '\u{203a}' } else { ' ' },
                    command.label
                ),
                style,
            )));
        }
        if commands.is_empty() {
            lines.push(Line::from(Span::styled(
                " No matching commands",
                theme::THEME.footer,
            )));
        }
        lines.push(Line::from(""));
        lines.push(Line::from(Span::styled(
            " \u{2191}\u{2193} select  Enter run  Esc close",
            theme::THEME.footer,
        )));

        let visible = inner.height as usize;
        let start = self
            .palette
            .selected
            .saturating_sub(visible.saturating_sub(3));
        let lines: Vec<_> = lines.into_iter().skip(start).take(visible).collect();
        frame.render_widget(Paragraph::new(Text::from(lines)), inner);
    }

    fn render(&mut self, frame: &mut Frame) {
        let area = frame.area();
        self.last_area = area;
        frame.render_widget(
            Block::default().style(Style::default().bg(theme::THEME.bg)),
            area,
        );

        if area.width < MIN_TERMINAL_WIDTH || area.height < MIN_TERMINAL_HEIGHT {
            render_too_small(frame, area, MIN_TERMINAL_WIDTH, MIN_TERMINAL_HEIGHT);
            return;
        }

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
            Overlay::CommandPalette => self.render_palette(frame),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{
        Mutex,
        atomic::{AtomicBool, Ordering},
    };

    use crate::ui::tui::background::BackgroundTask;

    use super::*;

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

    /// A screen that exposes one palette command and records the keys it gets.
    struct CommandScreen {
        last_key: Arc<Mutex<Option<KeyCode>>>,
        executed: Arc<AtomicBool>,
    }

    #[async_trait]
    impl Screen for CommandScreen {
        fn render(&mut self, frame: &mut Frame) {
            let area = frame.area();
            let footer = Rect {
                y: area.y + area.height - 1,
                height: 1,
                width: area.width,
                x: area.x,
            };
            frame.render_widget(
                Paragraph::new(theme::key_hint_footer(&self.help_hints())),
                footer,
            );
        }
        fn help_hints(&self) -> Vec<(&'static str, &'static str)> {
            vec![("Z", "screen control")]
        }
        async fn handle_key(&mut self, key: KeyEvent) -> Option<Transition> {
            *self.last_key.lock().unwrap() = Some(key.code);
            None
        }
        fn palette_commands(&self) -> Vec<PaletteCommand> {
            vec![PaletteCommand {
                id: "demo",
                label: "Demo Action",
            }]
        }
        async fn execute_palette_command(&mut self, _id: &str) -> Option<Transition> {
            self.executed.store(true, Ordering::SeqCst);
            Some(Transition::Quit)
        }
    }

    fn command_app() -> (App, Arc<AtomicBool>, Arc<Mutex<Option<KeyCode>>>) {
        let last_key = Arc::new(Mutex::new(None));
        let executed = Arc::new(AtomicBool::new(false));
        let app = App {
            stack: vec![Box::new(CommandScreen {
                last_key: last_key.clone(),
                executed: executed.clone(),
            })],
            should_quit: false,
            toasts: ToastSink::default(),
            overlay: Overlay::None,
            overlay_scroll: 0,
            palette: PaletteState::default(),
            last_area: Rect::default(),
            drag_row: None,
        };
        (app, executed, last_key)
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
            palette: PaletteState::default(),
            last_area: Rect::default(),
            drag_row: None,
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
    async fn any_key_dismisses_transient_toasts() {
        let (mut app, _finished) = worker_app(false);
        app.toasts.info("still visible");
        assert!(app.toasts.with_queue(|queue| !queue.is_empty()));

        app.handle_key(KeyEvent::new(KeyCode::Char('x'), KeyModifiers::NONE))
            .await;

        assert!(app.toasts.with_queue(|queue| queue.is_empty()));
    }

    #[tokio::test]
    async fn any_click_dismisses_transient_toasts() {
        let (mut app, _finished) = worker_app(false);
        app.toasts.warning("still visible");
        assert!(app.toasts.with_queue(|queue| !queue.is_empty()));

        app.handle_mouse(MouseEvent {
            kind: MouseEventKind::Down(MouseButton::Left),
            column: 10,
            row: 5,
            modifiers: KeyModifiers::NONE,
        })
        .await;

        assert!(app.toasts.with_queue(|queue| queue.is_empty()));
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
            palette: PaletteState::default(),
            last_area: Rect::default(),
            drag_row: None,
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
            palette: PaletteState::default(),
            last_area: Rect::default(),
            drag_row: None,
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
            palette: PaletteState::default(),
            last_area: Rect::default(),
            drag_row: None,
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

        app.handle_key(KeyEvent::new(KeyCode::Char('k'), KeyModifiers::CONTROL))
            .await;
        assert_eq!(app.overlay, Overlay::CommandPalette);
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
    async fn esc_closes_every_overlay() {
        let (mut app, _finished) = worker_app(false);
        let openers = [
            ("help", KeyEvent::new(KeyCode::F(1), KeyModifiers::NONE)),
            (
                "notifications",
                KeyEvent::new(KeyCode::Char('n'), KeyModifiers::CONTROL),
            ),
            (
                "command palette",
                KeyEvent::new(KeyCode::Char('k'), KeyModifiers::CONTROL),
            ),
        ];

        for (name, opener) in openers {
            app.handle_key(opener).await;
            assert_ne!(app.overlay, Overlay::None, "{name} did not open");
            app.handle_key(KeyEvent::new(KeyCode::Esc, KeyModifiers::NONE))
                .await;
            assert_eq!(app.overlay, Overlay::None, "Esc did not close {name}");
            assert!(!app.should_quit, "Esc leaked from {name}");
        }
    }

    #[tokio::test]
    async fn ctrl_n_and_ctrl_k_are_not_sent_to_screens() {
        // A screen that quits on any key would end the app if these leaked
        // through; instead the overlay opens.
        let (mut app, _finished) = worker_app(false);
        app.handle_key(KeyEvent::new(KeyCode::Char('k'), KeyModifiers::CONTROL))
            .await;
        assert_eq!(app.overlay, Overlay::CommandPalette);
        assert!(!app.should_quit);
    }

    #[tokio::test]
    async fn command_palette_filters_and_runs_the_selected_command() {
        let (mut app, executed, _last_key) = command_app();
        app.handle_key(KeyEvent::new(KeyCode::Char('k'), KeyModifiers::CONTROL))
            .await;
        assert_eq!(app.overlay, Overlay::CommandPalette);

        for c in "dem".chars() {
            app.handle_key(KeyEvent::new(KeyCode::Char(c), KeyModifiers::NONE))
                .await;
        }
        assert_eq!(app.palette.query, "dem");
        app.handle_key(KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE))
            .await;

        assert!(executed.load(Ordering::SeqCst));
        assert!(app.should_quit);
        assert_eq!(app.overlay, Overlay::None);
    }

    #[tokio::test]
    async fn dragging_the_mouse_scrolls_the_active_screen() {
        let (mut app, _executed, last_key) = command_app();
        app.handle_mouse(MouseEvent {
            kind: MouseEventKind::Down(MouseButton::Left),
            column: 1,
            row: 4,
            modifiers: KeyModifiers::NONE,
        })
        .await;
        app.handle_mouse(MouseEvent {
            kind: MouseEventKind::Drag(MouseButton::Left),
            column: 1,
            row: 6,
            modifiers: KeyModifiers::NONE,
        })
        .await;

        assert_eq!(*last_key.lock().unwrap(), Some(KeyCode::Down));
    }
}
