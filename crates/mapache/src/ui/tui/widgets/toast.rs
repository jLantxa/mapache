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
    widgets::{Block, BorderType, Borders, Padding, Paragraph, Widget},
};

use crate::ui::tui::{
    theme,
    widgets::{centered_rect, wrap_line},
};

const TOAST_MAX_WIDTH: u16 = 70;
const TOAST_MARGIN: u16 = 2;
const TOAST_PADDING: u16 = 2;
const TOAST_BORDER: u16 = 2;
const TOAST_TITLE_EXTRA: u16 = 2;

/// How many notifications are kept on screen at once. When the queue is
/// full the oldest entry is dropped first.
const MAX_TOASTS: usize = 3;

/// Severity of a transient notification.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ToastKind {
    Warning,
    Error,
}

impl ToastKind {
    fn color(self) -> Color {
        match self {
            ToastKind::Warning => theme::THEME.yellow,
            ToastKind::Error => theme::THEME.red,
        }
    }

    fn title(self) -> &'static str {
        match self {
            ToastKind::Warning => "Warning",
            ToastKind::Error => "Error",
        }
    }

    /// How long the notification stays visible before it is pruned.
    fn ttl(self) -> Duration {
        match self {
            ToastKind::Warning => Duration::from_secs(8),
            ToastKind::Error => Duration::from_secs(12),
        }
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

        let title_width = self.title.chars().count() as u16 + TOAST_TITLE_EXTRA;
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

/// A single queued notification.
struct ToastEntry {
    kind: ToastKind,
    message: String,
    created: Instant,
}

/// Fixed-size queue of notifications rendered as a stack in the bottom-right
/// corner of the screen, newest entry at the bottom.
#[derive(Default)]
pub struct ToastQueue {
    entries: VecDeque<ToastEntry>,
}

impl ToastQueue {
    pub fn push(&mut self, kind: ToastKind, message: impl Into<String>) {
        self.entries.push_back(ToastEntry {
            kind,
            message: message.into(),
            created: Instant::now(),
        });
        while self.entries.len() > MAX_TOASTS {
            self.entries.pop_front();
        }
    }

    /// Drops notifications whose time-to-live has elapsed.
    pub fn prune(&mut self, now: Instant) {
        self.entries
            .retain(|e| now.saturating_duration_since(e.created) < e.kind.ttl());
    }

    /// Renders the queue anchored to the bottom-right of `area`, newest
    /// entry at the bottom. Older entries that do not fit are skipped.
    pub fn render(&self, area: Rect, frame: &mut Frame) {
        let mut y = area.y + area.height;
        for entry in self.entries.iter().rev() {
            let toast = Toast::new(
                entry.kind.title(),
                entry.kind.color(),
                entry.message.clone(),
            );
            let (width, height, lines) = toast.layout(area);
            y = y.saturating_sub(height);
            if y < area.y {
                break;
            }
            let rect = Rect {
                x: area.x + area.width.saturating_sub(width),
                y,
                width,
                height,
            };
            toast.render_at(rect, &lines, frame);
            y = y.saturating_sub(1);
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
    pub fn warning(&self, message: impl Into<String>) {
        self.push(ToastKind::Warning, message);
    }

    pub fn error(&self, message: impl Into<String>) {
        self.push(ToastKind::Error, message);
    }

    pub fn push(&self, kind: ToastKind, message: impl Into<String>) {
        self.with_queue(|queue| queue.push(kind, message));
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
