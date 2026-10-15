use std::{
    collections::VecDeque,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use ratatui::{
    Frame,
    layout::{Alignment, Rect},
    style::{Color, Modifier, Style},
    text::{Line, Span, Text},
    widgets::{Block, BorderType, Borders, Clear, Padding, Paragraph, Widget},
};

use crate::ui::tui::{
    theme,
    widgets::{centered_rect, text_width, wrap_line},
};

const TOAST_MAX_WIDTH: u16 = 70;
const TOAST_MARGIN: u16 = 2;
const TOAST_PADDING: u16 = 2;
const TOAST_BORDER: u16 = 2;
const TOAST_TITLE_EXTRA: u16 = 2;

/// How many notifications are kept on screen at once. When the queue is
/// full the oldest entry is dropped first.
const MAX_TOASTS: usize = 3;

/// How many notifications are remembered for the notification centre. The
/// history is not pruned by time, so it survives the transient toasts.
const MAX_HISTORY: usize = 200;

/// Severity of a notification.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ToastKind {
    Info,
    Success,
    Warning,
    Error,
}

impl ToastKind {
    fn color(self) -> Color {
        match self {
            ToastKind::Info => theme::THEME.blue,
            ToastKind::Success => theme::THEME.green,
            ToastKind::Warning => theme::THEME.yellow,
            ToastKind::Error => theme::THEME.red,
        }
    }

    fn title(self) -> &'static str {
        match self {
            ToastKind::Info => "Info",
            ToastKind::Success => "Success",
            ToastKind::Warning => "Warning",
            ToastKind::Error => "Error",
        }
    }

    /// How long the transient toast stays visible before it is pruned.
    fn ttl(self) -> Duration {
        match self {
            ToastKind::Info | ToastKind::Success => Duration::from_secs(6),
            ToastKind::Warning => Duration::from_secs(8),
            ToastKind::Error => Duration::from_secs(12),
        }
    }
}

/// Renders an elapsed duration as a compact "12s"/"3m"/"2h" age.
fn format_age(elapsed: Duration) -> String {
    let secs = elapsed.as_secs();
    if secs < 60 {
        format!("{secs}s")
    } else if secs < 3600 {
        format!("{}m", secs / 60)
    } else {
        format!("{}h", secs / 3600)
    }
}

pub struct Toast {
    title: String,
    color: Color,
    content: Text<'static>,
}

impl Toast {
    pub fn new(title: impl Into<String>, color: Color, message: impl Into<String>) -> Self {
        Self {
            title: title.into(),
            color,
            content: Text::from(message.into()),
        }
    }

    pub fn with_text(title: impl Into<String>, color: Color, content: Text<'static>) -> Self {
        Self {
            title: title.into(),
            color,
            content,
        }
    }

    /// Computes the popup size for `area` together with the wrapped content
    /// lines needed to fill it.
    fn layout(&self, area: Rect) -> (u16, u16, Vec<Line<'static>>) {
        let inner_width = area.width.saturating_sub(TOAST_MARGIN * 2);
        let max_width = TOAST_MAX_WIDTH.min(inner_width);

        let title_width = text_width(&self.title) as u16 + TOAST_TITLE_EXTRA;
        let popup_width = title_width.max(max_width).min(inner_width);
        let text_width = popup_width
            .saturating_sub(TOAST_BORDER + TOAST_PADDING * 2)
            .max(1) as usize;

        let mut wrapped_lines: Vec<Line<'static>> = Vec::new();
        for line in &self.content.lines {
            wrap_line(line, text_width, &mut wrapped_lines);
        }

        let popup_height = (wrapped_lines.len() as u16 + TOAST_BORDER + TOAST_PADDING * 2)
            .min(area.height.saturating_sub(TOAST_MARGIN * 2));

        (popup_width, popup_height, wrapped_lines)
    }

    /// Renders the toast centred inside `area`.
    pub fn render(&self, area: Rect, frame: &mut Frame) {
        let (popup_width, popup_height, wrapped_lines) = self.layout(area);
        let rect = centered_rect(area, popup_width, popup_height);
        self.render_at(rect, &wrapped_lines, frame);
    }

    /// Renders the toast at an explicit rectangle, using previously
    /// computed wrapped lines.
    fn render_at(&self, rect: Rect, wrapped_lines: &[Line<'static>], frame: &mut Frame) {
        let style = Style::default().fg(self.color);
        let title_style = Style::default().fg(self.color).add_modifier(Modifier::BOLD);

        let content = Paragraph::new(Text::from(wrapped_lines.to_vec()))
            .alignment(Alignment::Left)
            .block(
                Block::default()
                    .style(Style::new().bg(theme::THEME.bg))
                    .borders(Borders::ALL)
                    .border_type(BorderType::Rounded)
                    .border_style(style)
                    .title(Span::styled(format!(" {} ", self.title), title_style))
                    .title_alignment(Alignment::Center)
                    .padding(Padding::uniform(TOAST_PADDING)),
            );

        ratatui::widgets::Clear.render(rect, frame.buffer_mut());
        frame.render_widget(content, rect);
    }
}

/// A single notification, kept both as a transient toast and in the history.
#[derive(Debug, Clone)]
struct ToastEntry {
    kind: ToastKind,
    message: String,
    created: Instant,
}

impl ToastEntry {
    fn toast(&self) -> Toast {
        Toast::new(self.kind.title(), self.kind.color(), self.message.clone())
    }
}

/// Queue of transient notifications (rendered as a stack in the bottom-right
/// corner) plus a longer-lived history for the notification centre.
#[derive(Default)]
pub struct ToastQueue {
    entries: VecDeque<ToastEntry>,
    history: VecDeque<ToastEntry>,
}

impl ToastQueue {
    pub fn push(&mut self, kind: ToastKind, message: impl Into<String>) {
        let now = Instant::now();
        let message = message.into();

        self.history.push_back(ToastEntry {
            kind,
            message: message.clone(),
            created: now,
        });
        while self.history.len() > MAX_HISTORY {
            self.history.pop_front();
        }

        self.entries.push_back(ToastEntry {
            kind,
            message,
            created: now,
        });
        while self.entries.len() > MAX_TOASTS {
            self.entries.pop_front();
        }
    }

    /// Whether any transient notification is currently queued.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Drops notifications whose time-to-live has elapsed.
    pub fn prune(&mut self, now: Instant) {
        self.entries
            .retain(|e| now.saturating_duration_since(e.created) < e.kind.ttl());
    }

    /// Forgets every transient notification and the whole history.
    pub fn clear(&mut self) {
        self.entries.clear();
        self.history.clear();
    }

    /// The rectangles of the currently visible toasts, newest first, paired
    /// with their index in `entries`. Used for both rendering and hit-testing.
    fn visible_rects(&self, area: Rect) -> Vec<(Rect, usize)> {
        let mut rects = Vec::new();
        let mut y = area.y + area.height;
        for (idx, entry) in self.entries.iter().enumerate().rev() {
            let (width, height, _) = entry.toast().layout(area);
            y = y.saturating_sub(height);
            if y < area.y {
                break;
            }
            rects.push((
                Rect {
                    x: area.x + area.width.saturating_sub(width),
                    y,
                    width,
                    height,
                },
                idx,
            ));
            y = y.saturating_sub(1);
        }
        rects
    }

    /// Removes the toast whose rectangle contains `(column, row)`, returning
    /// whether one was hit. Used to dismiss a toast by clicking it.
    pub fn dismiss_at(&mut self, area: Rect, column: u16, row: u16) -> bool {
        let hit = self
            .visible_rects(area)
            .into_iter()
            .find(|(rect, _)| {
                column >= rect.x
                    && column < rect.x.saturating_add(rect.width)
                    && row >= rect.y
                    && row < rect.y.saturating_add(rect.height)
            })
            .map(|(_, idx)| idx);

        if let Some(idx) = hit {
            self.entries.remove(idx);
            true
        } else {
            false
        }
    }

    /// Renders the queue anchored to the bottom-right of `area`, newest
    /// entry at the bottom. Older entries that do not fit are skipped.
    pub fn render(&self, area: Rect, frame: &mut Frame) {
        for (rect, idx) in self.visible_rects(area) {
            let toast = self.entries[idx].toast();
            let (_, _, lines) = toast.layout(area);
            toast.render_at(rect, &lines, frame);
        }
    }

    /// Renders the full notification history as a centred overlay, newest
    /// first. `scroll` is clamped so it never points past the content.
    pub fn render_center(&self, area: Rect, frame: &mut Frame, scroll: &mut u16) {
        let mut lines: Vec<Line<'static>> = Vec::new();
        if self.history.is_empty() {
            lines.push(Line::from(Span::styled(
                "No notifications yet.",
                theme::THEME.footer,
            )));
        } else {
            for entry in self.history.iter().rev() {
                let kind_style = Style::default()
                    .fg(entry.kind.color())
                    .add_modifier(Modifier::BOLD);
                lines.push(Line::from(vec![
                    Span::styled(format!("{} ", entry.kind.title()), kind_style),
                    Span::styled(
                        format!("\u{b7} {} ago", format_age(entry.created.elapsed())),
                        Style::default().fg(theme::THEME.subtext_dim),
                    ),
                ]));
                lines.push(Line::from(vec![
                    Span::raw("  "),
                    Span::styled(
                        entry.message.clone(),
                        Style::default().fg(theme::THEME.subtext),
                    ),
                ]));
                lines.push(Line::from(""));
            }
        }
        lines.push(Line::from(Span::styled(
            "c clear \u{b7} Esc or Ctrl+N to close",
            theme::THEME.footer,
        )));

        let content_width = lines.iter().map(|l| l.width()).max().unwrap_or(0) as u16;
        let width = (content_width + 4).min(area.width);
        let wrap_width = (width.saturating_sub(3)).max(1) as usize;

        let mut wrapped: Vec<Line<'static>> = Vec::new();
        for line in &lines {
            wrap_line(line, wrap_width, &mut wrapped);
        }

        let height = (wrapped.len() as u16 + 2).min(area.height);
        let inner_height = height.saturating_sub(2) as usize;
        let max_scroll = wrapped.len().saturating_sub(inner_height) as u16;
        *scroll = (*scroll).min(max_scroll);

        let popup = centered_rect(area, width, height);
        let total = wrapped.len();

        frame.render_widget(Clear, popup);
        frame.render_widget(
            Paragraph::new(Text::from(wrapped))
                .block(
                    Block::default()
                        .style(Style::new().bg(theme::THEME.bg))
                        .borders(Borders::ALL)
                        .border_type(BorderType::Rounded)
                        .border_style(theme::THEME.border)
                        .title(Span::styled(" Notifications ", theme::THEME.header))
                        .title_alignment(Alignment::Center),
                )
                .scroll((*scroll, 0)),
            popup,
        );

        if max_scroll > 0 {
            theme::render_scrollbar(frame, popup, total, *scroll as usize);
        }
    }
}

/// Shared, thread-safe handle used by screens (and the tasks they spawn) to
/// post notifications that the app renders.
#[derive(Clone, Default)]
pub struct ToastSink {
    queue: Arc<Mutex<ToastQueue>>,
}

impl ToastSink {
    pub fn info(&self, message: impl Into<String>) {
        self.push(ToastKind::Info, message);
    }

    pub fn success(&self, message: impl Into<String>) {
        self.push(ToastKind::Success, message);
    }

    pub fn warning(&self, message: impl Into<String>) {
        self.push(ToastKind::Warning, message);
    }

    pub fn error(&self, message: impl Into<String>) {
        self.push(ToastKind::Error, message);
    }

    pub fn push(&self, kind: ToastKind, message: impl Into<String>) {
        self.with_queue(|queue| queue.push(kind, message));
    }

    /// Clears every transient notification and the whole history.
    pub fn clear(&self) {
        self.with_queue(|queue| queue.clear());
    }

    /// Runs `f` with exclusive access to the shared queue. A poisoned lock
    /// still yields the queue: a panic while holding it leaves no
    /// inconsistent state behind.
    pub(crate) fn with_queue<R>(&self, f: impl FnOnce(&mut ToastQueue) -> R) -> R {
        let mut queue = self.queue.lock().unwrap_or_else(|e| e.into_inner());
        f(&mut queue)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn queue_keeps_only_the_newest_entries() {
        let mut queue = ToastQueue::default();
        for i in 0..MAX_TOASTS + 2 {
            queue.push(ToastKind::Error, format!("msg-{i}"));
        }
        assert_eq!(queue.entries.len(), MAX_TOASTS);
        // Oldest entries were evicted, newest survived.
        assert_eq!(queue.entries.front().unwrap().message, "msg-2");
        assert_eq!(queue.entries.back().unwrap().message, "msg-4");
    }

    #[test]
    fn queue_prunes_entries_after_their_ttl() {
        let mut queue = ToastQueue::default();
        queue.push(ToastKind::Warning, "short-lived");
        queue.push(ToastKind::Error, "long-lived");

        let now = Instant::now();
        queue.prune(now);
        assert_eq!(queue.entries.len(), 2);

        // The warning TTL (8s) passes before the error TTL (12s).
        queue.prune(now + Duration::from_secs(9));
        assert_eq!(queue.entries.len(), 1);
        assert_eq!(queue.entries[0].message, "long-lived");

        queue.prune(now + Duration::from_secs(13));
        assert!(queue.entries.is_empty());
    }

    #[test]
    fn sink_shares_one_queue_across_clones() {
        let sink = ToastSink::default();
        let spawned = sink.clone();
        spawned.error("from a background task");
        sink.warning("from the screen");
        sink.with_queue(|queue| assert_eq!(queue.entries.len(), 2));
    }

    #[test]
    fn history_outlives_the_transient_toasts() {
        let mut queue = ToastQueue::default();
        for i in 0..MAX_TOASTS + 2 {
            queue.push(ToastKind::Info, format!("msg-{i}"));
        }
        // Only the newest few are shown, but every one is remembered.
        assert_eq!(queue.entries.len(), MAX_TOASTS);
        assert_eq!(queue.history.len(), MAX_TOASTS + 2);

        // Pruning the transient toasts must not touch the history.
        queue.prune(Instant::now() + Duration::from_secs(3600));
        assert!(queue.entries.is_empty());
        assert_eq!(queue.history.len(), MAX_TOASTS + 2);
    }

    #[test]
    fn dismiss_at_removes_the_clicked_toast() {
        let mut queue = ToastQueue::default();
        queue.push(ToastKind::Warning, "first");
        queue.push(ToastKind::Error, "second");

        let area = Rect::new(0, 0, 80, 24);
        let rects = queue.visible_rects(area);
        assert_eq!(rects.len(), 2);

        // Click the centre of the newest toast (index 1 in `entries`).
        let (rect, idx) = rects[0];
        assert!(queue.dismiss_at(area, rect.x + rect.width / 2, rect.y + rect.height / 2));
        assert_eq!(idx, 1);
        assert_eq!(queue.entries.len(), 1);
        assert_eq!(queue.entries[0].message, "first");

        // A click outside every toast is ignored.
        assert!(!queue.dismiss_at(area, 0, 0));
    }

    #[test]
    fn clear_forgets_history_and_toasts() {
        let mut queue = ToastQueue::default();
        queue.push(ToastKind::Success, "done");
        queue.clear();
        assert!(queue.is_empty());
        assert!(queue.history.is_empty());
    }

    #[test]
    fn notification_center_renders_history() {
        use crate::ui::tui::test_support::render_text;

        let mut queue = ToastQueue::default();
        queue.push(ToastKind::Success, "snapshot created");
        let mut scroll = 0;
        let text = render_text(60, 20, |frame| {
            queue.render_center(frame.area(), frame, &mut scroll);
        });

        assert!(text.contains("Notifications"));
        assert!(text.contains("Success"));
        assert!(text.contains("snapshot created"));
    }

    #[test]
    fn age_is_rendered_compactly() {
        assert_eq!(format_age(Duration::from_secs(5)), "5s");
        assert_eq!(format_age(Duration::from_secs(90)), "1m");
        assert_eq!(format_age(Duration::from_secs(7200)), "2h");
    }

    #[test]
    fn toast_never_exceeds_a_narrow_frame() {
        let toast = Toast::new(
            ToastKind::Error.title(),
            ToastKind::Error.color(),
            "a rather long error message that would not fit narrow terminals",
        );
        for width in [10u16, 20, 40, 80] {
            let area = Rect::new(0, 0, width, 24);
            let (popup_width, popup_height, _) = toast.layout(area);
            assert!(
                popup_width <= area.width,
                "toast width {popup_width} exceeds frame width {width}"
            );
            assert!(
                popup_height <= area.height,
                "toast height {popup_height} exceeds frame height"
            );
        }
    }
}
