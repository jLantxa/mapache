use ratatui::{
    Frame,
    layout::{Alignment, Margin, Rect},
    style::{Color, Modifier, Style},
    text::{Line, Span},
    widgets::{Block, Borders, Paragraph, Scrollbar, ScrollbarOrientation, ScrollbarState},
};

/// Standard inner margin for content areas in TUI screens.
pub(crate) const CONTENT_MARGIN: Margin = Margin::new(2, 1);

/// Renders a bordered panel holding a single centred message. Empty and
/// loading states share this so they look the same across every screen.
pub(crate) fn empty_state(frame: &mut Frame, area: Rect, title: &str, message: &str) {
    let block = block(title);
    let inner = block.inner(area);
    frame.render_widget(block, area);
    if inner.height > 0 {
        frame.render_widget(
            Paragraph::new(Span::styled(message.to_string(), THEME.subtext))
                .alignment(Alignment::Center),
            Rect {
                y: inner.y + inner.height / 3,
                height: 1,
                ..inner
            },
        );
    }
}

/// Width of the muted label column in key/value summary rows.
pub(crate) const KV_LABEL_WIDTH: usize = 22;

/// A section heading line used to group key/value rows.
pub(crate) fn section(title: &str) -> Line<'static> {
    Line::from(Span::styled(title.to_string(), THEME.header))
}

/// A key/value summary row: a muted label column followed by a value.
pub(crate) fn kv(label: &str, value: impl Into<String>) -> Line<'static> {
    kv_styled(label, value, THEME.stat_value)
}

/// Like [`kv`], but lets the caller style the value (for warnings/errors).
pub(crate) fn kv_styled(label: &str, value: impl Into<String>, style: Style) -> Line<'static> {
    Line::from(vec![
        Span::styled(
            format!("  {:<width$}", label, width = KV_LABEL_WIDTH),
            THEME.stat_label,
        ),
        Span::styled(value.into(), style),
    ])
}

/// A label/value row for detail panels: a fixed-width muted key column followed
/// by pre-styled value spans. Unlike [`kv`], the caller chooses the column width
/// and the value spans may carry their own styling.
pub(crate) fn field_spans(
    label: &str,
    label_width: usize,
    value: Vec<Span<'static>>,
) -> Line<'static> {
    let mut spans = Vec::with_capacity(value.len() + 1);
    spans.push(Span::styled(
        format!("{label:<label_width$}"),
        THEME.menu_key,
    ));
    spans.extend(value);
    Line::from(spans)
}

/// A label/value row with a single unstyled value. See [`field_spans`].
pub(crate) fn field(label: &str, label_width: usize, value: impl Into<String>) -> Line<'static> {
    field_spans(label, label_width, vec![Span::raw(value.into())])
}

/// Electric Pastel colour palette used throughout the TUI.
pub(crate) struct Theme {
    // ── Core palette ────────────────────────────────────────────
    pub bg: Color,
    pub surface: Color,
    pub subtext_dim: Color,
    pub subtext: Color,
    // Accents
    pub blue: Color,
    pub green: Color,
    pub yellow: Color,
    pub red: Color,
    pub peach: Color,
    pub teal: Color,

    // ── Pre-built styles ─────────────────────────────────────────
    pub header: Style,         // Bold coloured header text
    pub border: Style,         // Default border colour
    pub border_focused: Style, // Border for focussed / active element
    pub selection: Style,      // Selected/highlighted row background
    pub menu_key: Style,       // Keyboard-hint key styling
    pub menu_selected: Style,  // Focused action in the command menu
    pub footer: Style,         // Footer / secondary text
    // Snapshot-table columns
    pub snap_id: Style,
    pub snap_date: Style,
    pub snap_host: Style,
    pub snap_size: Style,
    pub snap_active: Style,
    pub snap_inactive: Style,
    // File-tree
    pub dir_fg: Color,
    pub file_fg: Color,
    pub symlink_fg: Color,
    pub file_size: Style,
    pub breadcrumb: Style,
    // Progress
    pub progress_filled: Color,
    pub progress_empty: Color,
    // Status colours (use `.fg` on these for Toast, or pass whole Style)
    pub success: Style,
    pub error: Style,
    pub warning: Style,
    pub info: Style,
    // Stat cards
    pub stat_label: Style,
    pub stat_value: Style,
}

// ── Electric Pastel (dark) ──────────────────────────────────────
const ELECTRIC: Theme = {
    use Color::Rgb;
    let bg = Rgb(14, 24, 30);
    let surface = Rgb(22, 34, 42);
    let overlay = Rgb(34, 46, 54);
    let border_line = Rgb(56, 74, 88);
    let selection_bg = Rgb(70, 92, 106);
    let subtext_dim = Rgb(76, 100, 112);
    let subtext = Rgb(132, 155, 167);
    let text = Rgb(228, 235, 240);
    let blue = Rgb(110, 180, 255);
    let green = Rgb(110, 230, 150);
    let yellow = Rgb(255, 215, 110);
    let red = Rgb(255, 110, 135);
    let mauve = Rgb(170, 110, 255);
    let peach = Rgb(255, 170, 110);
    let teal = Rgb(110, 230, 210);
    let pink = Rgb(255, 110, 175);

    Theme {
        bg,
        surface,
        subtext_dim,
        subtext,
        blue,
        green,
        yellow,
        red,
        peach,
        teal,
        header: Style::new().fg(teal).add_modifier(Modifier::BOLD),
        border: Style::new().fg(border_line),
        border_focused: Style::new().fg(teal),
        selection: Style::new().bg(selection_bg),
        menu_key: Style::new().fg(teal).add_modifier(Modifier::BOLD),
        menu_selected: Style::new().bg(teal).fg(bg).add_modifier(Modifier::BOLD),
        footer: Style::new().fg(subtext),
        snap_id: Style::new().fg(green),
        snap_date: Style::new().fg(yellow),
        snap_host: Style::new().fg(pink),
        snap_size: Style::new().fg(mauve),
        snap_active: Style::new().fg(green),
        snap_inactive: Style::new().fg(subtext_dim),
        dir_fg: blue,
        file_fg: text,
        symlink_fg: pink,
        file_size: Style::new().fg(subtext_dim),
        breadcrumb: Style::new().fg(teal).add_modifier(Modifier::BOLD),
        progress_filled: teal,
        progress_empty: overlay,
        success: Style::new().fg(green).add_modifier(Modifier::BOLD),
        error: Style::new().fg(red).add_modifier(Modifier::BOLD),
        warning: Style::new().fg(yellow).add_modifier(Modifier::BOLD),
        info: Style::new().fg(blue).add_modifier(Modifier::BOLD),
        stat_label: Style::new().fg(subtext),
        stat_value: Style::new().fg(text).add_modifier(Modifier::BOLD),
    }
};

pub(crate) static THEME: Theme = ELECTRIC;

// ── Convenience helpers ─────────────────────────────────────────

/// A bordered block with consistent styling.
pub fn block(title: &str) -> Block<'static> {
    Block::default()
        .style(Style::new().bg(THEME.bg))
        .borders(Borders::ALL)
        .border_type(ratatui::widgets::BorderType::Rounded)
        .border_style(THEME.border)
        .title(format!(" {} ", title))
        .title_style(THEME.header)
}

/// Vertical scrollbar styled to match the theme.
pub fn scrollbar() -> Scrollbar<'static> {
    Scrollbar::new(ScrollbarOrientation::VerticalRight)
        .begin_symbol(None)
        .end_symbol(None)
        .track_symbol(Some("\u{2502}"))
        .thumb_symbol("\u{2588}")
        .style(THEME.border)
}

pub fn render_scrollbar(frame: &mut Frame, area: Rect, total: usize, position: usize) {
    if total == 0 {
        return;
    }
    let mut state = ScrollbarState::new(total).position(position);
    frame.render_stateful_widget(scrollbar(), area.inner(Margin::new(1, 1)), &mut state);
}

pub fn key_hint(key: &str, label: &str) -> Vec<Span<'static>> {
    vec![
        Span::styled(format!(" {} ", key), THEME.menu_key),
        Span::styled(format!(" {} ", label), Style::new().fg(THEME.subtext)),
    ]
}

pub fn key_hint_footer(hints: &[(&str, &str)]) -> Line<'static> {
    let mut spans = Vec::new();
    for (i, (key, label)) in hints.iter().enumerate() {
        if i > 0 {
            spans.push(Span::styled("  ", Style::new().fg(THEME.subtext_dim)));
        }
        spans.extend(key_hint(key, label));
    }
    Line::from(spans)
}

/// Splits a sequence of item widths into rows that each fit within `max_width`,
/// leaving a two-column gap between adjacent items. Returns the half-open index
/// range of the items on each row. Always yields at least one (possibly empty)
/// row so callers can render a blank line.
pub(crate) fn wrap_rows(item_widths: &[usize], max_width: usize) -> Vec<std::ops::Range<usize>> {
    let mut rows = Vec::new();
    let mut start = 0usize;
    let mut cur = 0usize;
    for (i, &width) in item_widths.iter().enumerate() {
        if i > start && cur + 2 + width > max_width {
            rows.push(start..i);
            start = i;
            cur = width;
        } else {
            if i > start {
                cur += 2;
            }
            cur += width;
        }
    }
    rows.push(start..item_widths.len());
    rows
}

/// Lay out key hints into one or more rows that wrap at `width`, breaking
/// between hints so a key/label pair is never split. Footers use this instead
/// of the single-line [`key_hint_footer`] so navigation hints stay visible on
/// narrow terminals instead of being clipped past the right edge.
pub fn key_hint_lines(hints: &[(&str, &str)], width: u16) -> Vec<Line<'static>> {
    // `key_hint` renders " key " + " label ", i.e. key+label+4 columns.
    let widths: Vec<usize> = hints
        .iter()
        .map(|(key, label)| key.chars().count() + label.chars().count() + 4)
        .collect();
    wrap_rows(&widths, width as usize)
        .into_iter()
        .map(|row| {
            let mut spans = Vec::new();
            for (offset, (key, label)) in hints[row].iter().enumerate() {
                if offset > 0 {
                    spans.push(Span::styled("  ", Style::new().fg(THEME.subtext_dim)));
                }
                spans.extend(key_hint(key, label));
            }
            Line::from(spans)
        })
        .collect()
}

pub fn format_tags(tags: impl IntoIterator<Item = impl AsRef<str>>) -> String {
    let mut result = String::new();
    let mut iter = tags.into_iter();
    if let Some(first) = iter.next() {
        result.push_str(first.as_ref());
        for tag in iter {
            result.push_str(", ");
            result.push_str(tag.as_ref());
        }
    }
    result
}

/// Characters used for the loading spinner animation in TUI screens.
pub const SPINNER_CHARS: &[char] = &['\u{25D0}', '\u{25D3}', '\u{25D1}', '\u{25D2}'];

#[cfg(test)]
mod tests {
    use super::*;

    fn srgb_to_linear(component: u8) -> f64 {
        let c = component as f64 / 255.0;
        if c <= 0.04045 {
            c / 12.92
        } else {
            ((c + 0.055) / 1.055).powf(2.4)
        }
    }

    fn luminance(color: Color) -> f64 {
        let Color::Rgb(r, g, b) = color else {
            panic!("theme colours must be RGB");
        };
        0.2126 * srgb_to_linear(r) + 0.7152 * srgb_to_linear(g) + 0.0722 * srgb_to_linear(b)
    }

    fn contrast(a: Color, b: Color) -> f64 {
        let (la, lb) = (luminance(a), luminance(b));
        let (hi, lo) = if la > lb { (la, lb) } else { (lb, la) };
        (hi + 0.05) / (lo + 0.05)
    }

    #[test]
    fn selection_is_visible_against_the_surface() {
        // WCAG 1.4.11 wants >= 3.0 for non-text UI; the palette aims for at
        // least 2.0 here so selection is obvious without shouting.
        let selection = THEME.selection.bg.expect("selection sets a background");
        assert!(
            contrast(selection, THEME.surface) >= 2.0,
            "selection background blends into the surface"
        );
    }

    #[test]
    fn footer_text_is_readable() {
        // WCAG AA body-text contrast is 4.5:1.
        let footer = THEME.footer.fg.expect("footer sets a foreground");
        assert!(
            contrast(footer, THEME.bg) >= 4.5,
            "footer text contrast is too low"
        );
    }

    #[test]
    fn key_hint_lines_wrap_without_clipping_hints() {
        let hints = [
            ("Esc", "back"),
            ("\u{2191}\u{2193}", "navigate"),
            ("\u{2192}", "open"),
            ("\u{2190}", "up"),
            ("Space", "page down"),
            ("r", "restore"),
            ("q", "quit"),
        ];

        for width in [80u16, 60, 40, 24] {
            let lines = key_hint_lines(&hints, width);
            for line in &lines {
                let rendered: usize = line.spans.iter().map(|s| s.content.chars().count()).sum();
                assert!(
                    rendered <= width as usize,
                    "hint row exceeds {width} cols: {rendered} ({line:?})"
                );
            }

            let text: String = lines
                .iter()
                .flat_map(|l| l.spans.iter())
                .map(|s| s.content.as_ref())
                .collect();
            for (_, label) in hints {
                assert!(
                    text.contains(label),
                    "hint '{label}' missing at {width} cols"
                );
            }
        }
    }

    #[test]
    fn empty_state_centres_its_message() {
        let rendered = crate::ui::tui::test_support::render_text(40, 10, |frame| {
            empty_state(frame, frame.area(), "Snapshots", "Nothing here");
        });

        assert!(rendered.contains("Snapshots"), "panel title is missing");
        assert!(rendered.contains("Nothing here"), "message is missing");
    }
}
