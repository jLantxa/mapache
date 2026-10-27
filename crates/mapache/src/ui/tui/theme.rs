use std::sync::{LazyLock, OnceLock};

use super::widgets::{text_width, wrap_line};
use ratatui::{
    Frame,
    layout::{Alignment, Constraint, Direction, Layout, Margin, Rect},
    style::{Color, Modifier, Style},
    text::{Line, Span, Text},
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

    if inner.width > 0 && inner.height > 0 {
        let lines = wrap_message(message, inner.width as usize);
        let height = lines.len().min(inner.height as usize) as u16;
        let y = inner.y + inner.height.saturating_sub(height) / 2;
        frame.render_widget(
            Paragraph::new(Text::from(lines)).alignment(Alignment::Center),
            Rect { y, height, ..inner },
        );
    }
}

fn wrap_message(message: &str, max_width: usize) -> Vec<Line<'static>> {
    let mut lines = Vec::new();
    let mut current = String::new();
    let mut current_width = 0;

    for word in message.split_whitespace() {
        let word_width = text_width(word);
        if !current.is_empty() && current_width + 1 + word_width > max_width {
            lines.push(Line::from(Span::styled(
                std::mem::take(&mut current),
                THEME.subtext,
            )));
            current_width = 0;
        }

        if word_width > max_width {
            if !current.is_empty() {
                lines.push(Line::from(Span::styled(
                    std::mem::take(&mut current),
                    THEME.subtext,
                )));
                current_width = 0;
            }
            wrap_line(
                &Line::from(Span::styled(word.to_string(), THEME.subtext)),
                max_width,
                &mut lines,
            );
            continue;
        }

        if !current.is_empty() {
            current.push(' ');
            current_width += 1;
        }
        current.push_str(word);
        current_width += word_width;
    }

    if !current.is_empty() {
        lines.push(Line::from(Span::styled(current, THEME.subtext)));
    }
    if lines.is_empty() {
        lines.push(Line::from(""));
    }
    lines
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

/// A label/value row with a single standard text-styled value. See [`field_spans`].
pub(crate) fn field(label: &str, label_width: usize, value: impl Into<String>) -> Line<'static> {
    field_spans(
        label,
        label_width,
        vec![Span::styled(value.into(), THEME.stat_value)],
    )
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

/// The colour scheme the TUI draws with.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum ThemeVariant {
    #[default]
    Dark,
    Light,
}

impl ThemeVariant {
    /// Best-effort detection of a light terminal background.
    ///
    /// Terminals that export `COLORFGBG` (rxvt, Konsole and others) report the
    /// foreground and background palette indices; a light background index means
    /// the light theme. Everything else falls back to the dark theme, so the
    /// historic look is preserved when nothing can be detected.
    pub fn detect() -> Self {
        let Ok(value) = std::env::var("COLORFGBG") else {
            return Self::Dark;
        };
        let Some(background) = value.rsplit(';').next() else {
            return Self::Dark;
        };
        match background.trim().parse::<u8>() {
            // 7 is light grey and 9..=15 are the bright colours; 8 is dark grey.
            Ok(index) if (7..=15).contains(&index) && index != 8 => Self::Light,
            _ => Self::Dark,
        }
    }
}

/// How many colours the terminal can render.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum ColorMode {
    /// 24-bit RGB, used as-is.
    #[default]
    TrueColor,
    /// The 256-colour xterm palette.
    Ansi256,
    /// The 16 base ANSI colours.
    Ansi16,
    /// No colour at all (`NO_COLOR`); only text modifiers are kept.
    None,
}

impl ColorMode {
    /// Detects the best colour depth for the current terminal.
    ///
    /// Honours the `NO_COLOR` convention and steps down to 256 or 16 colours
    /// when the terminal does not advertise 24-bit support, so a truecolor theme
    /// is never sent to a terminal that cannot render it.
    pub fn detect() -> Self {
        if std::env::var_os("NO_COLOR").is_some_and(|value| !value.is_empty()) {
            return Self::None;
        }
        if std::env::var_os("CLICOLOR_FORCE").is_some() {
            return Self::TrueColor;
        }
        if std::env::var("TERM").as_deref() == Ok("dumb") {
            return Self::None;
        }
        if matches!(
            std::env::var("COLORTERM").as_deref(),
            Ok("truecolor") | Ok("24bit")
        ) {
            return Self::TrueColor;
        }

        #[cfg(windows)]
        {
            // Windows Terminal advertises itself through `WT_SESSION` and, like
            // modern conhost with VT processing enabled (`init_console`),
            // handles 24-bit colour. Older consoles get the 256-colour cube.
            if std::env::var_os("WT_SESSION").is_some() {
                Self::TrueColor
            } else {
                Self::Ansi256
            }
        }

        #[cfg(not(windows))]
        {
            if std::env::var("TERM").is_ok_and(|term| term.contains("256color")) {
                Self::Ansi256
            } else {
                Self::Ansi16
            }
        }
    }
}

/// Maps a theme colour onto the palette the terminal can actually render.
fn degrade(color: Color, mode: ColorMode) -> Color {
    let Color::Rgb(r, g, b) = color else {
        return color;
    };
    match mode {
        ColorMode::TrueColor => color,
        ColorMode::Ansi256 => rgb_to_ansi256(r, g, b),
        ColorMode::Ansi16 => rgb_to_ansi16(r, g, b),
        ColorMode::None => Color::Reset,
    }
}

fn channel_distance(a: u8, b: u8) -> i32 {
    let delta = a as i32 - b as i32;
    delta * delta
}

/// Nearest xterm-256 index for an RGB colour (colour cube plus grey ramp).
fn rgb_to_ansi256(r: u8, g: u8, b: u8) -> Color {
    const CUBE: [u8; 6] = [0, 95, 135, 175, 215, 255];

    let nearest = |value: u8| {
        CUBE.iter()
            .enumerate()
            .min_by_key(|&(_, &level)| channel_distance(level, value))
            .map(|(index, &level)| (index, level))
            .expect("the colour cube is never empty")
    };
    let (ri, rc) = nearest(r);
    let (gi, gc) = nearest(g);
    let (bi, bc) = nearest(b);
    let cube_distance = channel_distance(r, rc) + channel_distance(g, gc) + channel_distance(b, bc);

    // The grey ramp (indices 232..=255) matches greys better than the sparse
    // cube levels, so it wins whenever it is closer.
    let average = (r as i32 + g as i32 + b as i32) / 3;
    let step = ((average - 8 + 5).max(0) / 10).min(23) as u8;
    let grey = 8 + 10 * step;
    let grey_distance =
        channel_distance(r, grey) + channel_distance(g, grey) + channel_distance(b, grey);

    if grey_distance < cube_distance {
        Color::Indexed(232 + step)
    } else {
        Color::Indexed(16 + 36 * ri as u8 + 6 * gi as u8 + bi as u8)
    }
}

/// Nearest base-16 ANSI colour for an RGB colour.
fn rgb_to_ansi16(r: u8, g: u8, b: u8) -> Color {
    const PALETTE: [(u8, u8, u8); 16] = [
        (0, 0, 0),
        (128, 0, 0),
        (0, 128, 0),
        (128, 128, 0),
        (0, 0, 128),
        (128, 0, 128),
        (0, 128, 128),
        (192, 192, 192),
        (128, 128, 128),
        (255, 0, 0),
        (0, 255, 0),
        (255, 255, 0),
        (0, 0, 255),
        (255, 0, 255),
        (0, 255, 255),
        (255, 255, 255),
    ];
    const COLORS: [Color; 16] = [
        Color::Black,
        Color::Red,
        Color::Green,
        Color::Yellow,
        Color::Blue,
        Color::Magenta,
        Color::Cyan,
        Color::Gray,
        Color::DarkGray,
        Color::LightRed,
        Color::LightGreen,
        Color::LightYellow,
        Color::LightBlue,
        Color::LightMagenta,
        Color::LightCyan,
        Color::White,
    ];

    let index = PALETTE
        .iter()
        .enumerate()
        .min_by_key(|&(_, &(pr, pg, pb))| {
            channel_distance(r, pr) + channel_distance(g, pg) + channel_distance(b, pb)
        })
        .map(|(index, _)| index)
        .expect("the ANSI palette is never empty");
    COLORS[index]
}

/// Raw 24-bit colours for one theme variant, before colour-depth degradation.
struct Palette {
    bg: Color,
    surface: Color,
    overlay: Color,
    border_line: Color,
    selection_bg: Color,
    subtext_dim: Color,
    subtext: Color,
    text: Color,
    blue: Color,
    green: Color,
    yellow: Color,
    red: Color,
    mauve: Color,
    peach: Color,
    teal: Color,
    pink: Color,
}

/// Electric Pastel (dark).
const DARK: Palette = {
    use Color::Rgb;
    Palette {
        bg: Rgb(14, 24, 30),
        surface: Rgb(22, 34, 42),
        overlay: Rgb(34, 46, 54),
        border_line: Rgb(56, 74, 88),
        selection_bg: Rgb(70, 92, 106),
        subtext_dim: Rgb(76, 100, 112),
        subtext: Rgb(132, 155, 167),
        text: Rgb(228, 235, 240),
        blue: Rgb(110, 180, 255),
        green: Rgb(110, 230, 150),
        yellow: Rgb(255, 215, 110),
        red: Rgb(255, 110, 135),
        mauve: Rgb(170, 110, 255),
        peach: Rgb(255, 170, 110),
        teal: Rgb(110, 230, 210),
        pink: Rgb(255, 110, 175),
    }
};

/// Electric Pastel (light): darker text and accents target at least 6:1
/// contrast on the page, panels, and selected rows.
const LIGHT: Palette = {
    use Color::Rgb;
    Palette {
        bg: Rgb(250, 250, 248),
        surface: Rgb(238, 240, 238),
        overlay: Rgb(125, 138, 136),
        border_line: Rgb(125, 138, 136),
        selection_bg: Rgb(235, 242, 249),
        subtext_dim: Rgb(78, 88, 94),
        subtext: Rgb(76, 89, 96),
        text: Rgb(26, 36, 42),
        blue: Rgb(20, 80, 170),
        green: Rgb(10, 104, 54),
        yellow: Rgb(119, 84, 0),
        red: Rgb(176, 28, 48),
        mauve: Rgb(105, 50, 175),
        peach: Rgb(149, 65, 0),
        teal: Rgb(0, 101, 97),
        pink: Rgb(168, 29, 106),
    }
};

fn palette_for(variant: ThemeVariant) -> &'static Palette {
    match variant {
        ThemeVariant::Dark => &DARK,
        ThemeVariant::Light => &LIGHT,
    }
}

/// Builds the full theme for a variant, mapping every colour onto `mode`.
fn build(variant: ThemeVariant, mode: ColorMode) -> Theme {
    let palette = palette_for(variant);
    let color = |color: Color| degrade(color, mode);
    let bg = color(palette.bg);

    // With colours disabled a background tint can no longer mark the selection,
    // so fall back to reverse video, which every terminal understands.
    let (selection, menu_selected) = if mode == ColorMode::None {
        (
            Style::new().add_modifier(Modifier::REVERSED),
            Style::new().add_modifier(Modifier::REVERSED | Modifier::BOLD),
        )
    } else {
        let selection = Style::new().bg(color(palette.selection_bg));
        let selection = if variant == ThemeVariant::Light {
            selection.add_modifier(Modifier::BOLD)
        } else {
            selection
        };
        (
            selection,
            Style::new()
                .bg(color(palette.teal))
                .fg(bg)
                .add_modifier(Modifier::BOLD),
        )
    };

    Theme {
        bg,
        surface: color(palette.surface),
        subtext_dim: color(palette.subtext_dim),
        subtext: color(palette.subtext),

        blue: color(palette.blue),
        green: color(palette.green),
        yellow: color(palette.yellow),
        red: color(palette.red),
        peach: color(palette.peach),
        teal: color(palette.teal),

        header: Style::new()
            .fg(color(palette.teal))
            .add_modifier(Modifier::BOLD),
        border: Style::new().fg(color(palette.border_line)),
        border_focused: Style::new().fg(color(palette.teal)),
        selection,
        menu_key: Style::new()
            .fg(color(palette.teal))
            .add_modifier(Modifier::BOLD),
        menu_selected,
        footer: Style::new().fg(color(palette.subtext)),

        snap_id: Style::new().fg(color(palette.green)),
        snap_date: Style::new().fg(color(palette.yellow)),
        snap_host: Style::new().fg(color(palette.pink)),
        snap_size: Style::new().fg(color(palette.mauve)),
        snap_active: Style::new().fg(color(palette.green)),
        snap_inactive: Style::new().fg(color(palette.subtext_dim)),

        dir_fg: color(palette.blue),
        file_fg: color(palette.text),
        symlink_fg: color(palette.pink),
        file_size: Style::new().fg(color(palette.subtext_dim)),
        breadcrumb: Style::new()
            .fg(color(palette.teal))
            .add_modifier(Modifier::BOLD),

        progress_filled: color(palette.teal),
        progress_empty: color(palette.overlay),

        success: Style::new()
            .fg(color(palette.green))
            .add_modifier(Modifier::BOLD),
        error: Style::new()
            .fg(color(palette.red))
            .add_modifier(Modifier::BOLD),
        warning: Style::new()
            .fg(color(palette.yellow))
            .add_modifier(Modifier::BOLD),
        info: Style::new()
            .fg(color(palette.blue))
            .add_modifier(Modifier::BOLD),

        stat_label: Style::new().fg(color(palette.subtext)),
        stat_value: Style::new()
            .fg(color(palette.text))
            .add_modifier(Modifier::BOLD),
    }
}

/// Theme selection made once, before the first [`THEME`] access. The defaults
/// keep the historic dark truecolor look for tests and for any caller that
/// never initialises the theme.
static SELECTION: OnceLock<(ThemeVariant, ColorMode)> = OnceLock::new();

/// Selects the theme. Call before the TUI starts drawing; a later call is
/// ignored because the theme is built lazily on first use.
pub fn init(variant: ThemeVariant, mode: ColorMode) {
    let _ = SELECTION.set((variant, mode));
}

/// The active theme, built once on first use from the selection made by [`init`].
pub(crate) static THEME: LazyLock<Theme> = LazyLock::new(|| {
    let (variant, mode) = SELECTION
        .get()
        .copied()
        .unwrap_or((ThemeVariant::Dark, ColorMode::TrueColor));
    build(variant, mode)
});

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
        .map(|(key, label)| text_width(key) + text_width(label) + 4)
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

/// Lays out the content area (inset by [`CONTENT_MARGIN`]) as the given `body`
/// rows followed by a footer holding the wrapped `hints`, renders that footer,
/// and returns the body rects in order.
///
/// Screens share this instead of repeating the margin/layout/footer dance, so
/// every screen's "body plus hint footer" frame looks the same.
pub fn body_with_footer(
    frame: &mut Frame,
    hints: &[(&str, &str)],
    body: &[Constraint],
) -> Vec<Rect> {
    let inner = frame.area().inner(CONTENT_MARGIN);
    let footer = key_hint_lines(hints, inner.width);

    let mut constraints = body.to_vec();
    constraints.push(Constraint::Length(footer.len() as u16));
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints(constraints)
        .split(inner);

    frame.render_widget(Paragraph::new(Text::from(footer)), chunks[body.len()]);
    chunks.iter().take(body.len()).copied().collect()
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

    const VARIANTS: [ThemeVariant; 2] = [ThemeVariant::Dark, ThemeVariant::Light];

    #[test]
    fn selection_is_visible_against_the_surface() {
        // WCAG 1.4.11 wants >= 3.0 for non-text UI; the palette aims for at
        // least 2.0 here so selection is obvious without shouting. The light
        // theme uses a subtle highlight instead, with bold text preserving the
        // selected-row cue while keeping colored text readable.
        for variant in VARIANTS {
            let theme = build(variant, ColorMode::TrueColor);
            let selection = theme.selection.bg.expect("selection sets a background");
            if variant == ThemeVariant::Light {
                assert!(theme.selection.add_modifier.contains(Modifier::BOLD));
                assert!(contrast(selection, theme.surface) > 1.0);
            } else {
                assert!(
                    contrast(selection, theme.surface) >= 2.0,
                    "{variant:?}: selection background blends into the surface"
                );
            }
        }
    }

    #[test]
    fn field_values_use_the_theme_text_style() {
        let line = field("Path", 8, "C:\\backup");
        assert_eq!(line.spans[1].style, THEME.stat_value);
    }

    #[test]
    fn empty_state_wraps_long_messages_on_narrow_terminals() {
        let content = crate::ui::tui::test_support::render_text(20, 8, |frame| {
            empty_state(
                frame,
                frame.area(),
                "Snapshots",
                "No snapshots yet - press 1 to create one.",
            );
        });

        assert!(content.contains("No snapshots"));
        assert!(content.contains("press 1"));
        assert!(content.contains("create"));
        assert!(content.contains("one."));
    }

    #[test]
    fn footer_text_is_readable() {
        // Light-theme body text targets 6:1 on both the page and panel surfaces.
        for variant in VARIANTS {
            let theme = build(variant, ColorMode::TrueColor);
            let footer = theme.footer.fg.expect("footer sets a foreground");
            assert!(
                contrast(footer, theme.bg) >= 6.0,
                "{variant:?}: footer text contrast is too low"
            );
            if variant == ThemeVariant::Light {
                for (name, color) in [
                    ("footer", footer),
                    ("dim text", theme.subtext_dim),
                    (
                        "stat label",
                        theme.stat_label.fg.expect("stat label sets a foreground"),
                    ),
                ] {
                    for (surface, background) in [("page", theme.bg), ("panel", theme.surface)] {
                        assert!(
                            contrast(color, background) >= 6.0,
                            "{variant:?}: {name} contrast is too low on the {surface} background"
                        );
                    }
                }
                for (name, color) in [
                    ("text", theme.file_fg),
                    ("subtext", theme.subtext),
                    ("dim text", theme.subtext_dim),
                    ("blue", theme.blue),
                    ("green", theme.green),
                    ("yellow", theme.yellow),
                    ("red", theme.red),
                    ("peach", theme.peach),
                    ("teal", theme.teal),
                    (
                        "mauve",
                        theme.snap_size.fg.expect("snapshot size sets a foreground"),
                    ),
                    ("pink", theme.symlink_fg),
                ] {
                    for background in [
                        theme.bg,
                        theme.surface,
                        theme.selection.bg.expect("selection sets a background"),
                    ] {
                        assert!(
                            contrast(color, background) >= 6.0,
                            "light-theme {name} color {color:?} is below 6:1 on {background:?}"
                        );
                    }
                }
                assert!(
                    contrast(
                        theme
                            .menu_selected
                            .fg
                            .expect("selected menu sets a foreground"),
                        theme
                            .menu_selected
                            .bg
                            .expect("selected menu sets a background"),
                    ) >= 6.0,
                    "selected menu text contrast is below 6:1"
                );
            }
        }
    }

    #[test]
    fn light_progress_track_is_visible_on_page_and_panel_backgrounds() {
        let theme = build(ThemeVariant::Light, ColorMode::TrueColor);
        for background in [theme.bg, theme.surface] {
            assert!(
                contrast(theme.progress_empty, background) >= 3.0,
                "empty progress track is hard to distinguish from its background"
            );
            assert!(
                contrast(
                    theme.border.fg.expect("border sets a foreground"),
                    background
                ) >= 3.0,
                "border is hard to distinguish from its background"
            );
        }
    }

    #[test]
    fn accents_stay_readable_on_the_selection_background() {
        for variant in VARIANTS {
            let theme = build(variant, ColorMode::TrueColor);
            let selection = theme.selection.bg.expect("selection sets a background");
            let minimum_contrast = if variant == ThemeVariant::Light {
                6.0
            } else {
                2.0
            };
            for (name, style) in [
                ("snap_id", theme.snap_id),
                ("snap_date", theme.snap_date),
                ("snap_host", theme.snap_host),
                ("snap_size", theme.snap_size),
                ("blue", Style::new().fg(theme.blue)),
                ("green", Style::new().fg(theme.green)),
                ("yellow", Style::new().fg(theme.yellow)),
                ("red", Style::new().fg(theme.red)),
                ("peach", Style::new().fg(theme.peach)),
                ("teal", Style::new().fg(theme.teal)),
            ] {
                let fg = style.fg.expect("column styles set a foreground");
                assert!(
                    contrast(fg, selection) >= minimum_contrast,
                    "{variant:?}: {name} is hard to read on a selected row"
                );
            }
            if variant == ThemeVariant::Light {
                assert!(
                    contrast(theme.subtext_dim, selection) >= minimum_contrast,
                    "{variant:?}: dim text is hard to read on a selected row"
                );
            }
        }
    }

    #[test]
    fn colour_modes_degrade_the_palette() {
        let truecolor = build(ThemeVariant::Dark, ColorMode::TrueColor);
        assert!(matches!(truecolor.bg, Color::Rgb(..)));

        let ansi256 = build(ThemeVariant::Dark, ColorMode::Ansi256);
        assert!(matches!(ansi256.bg, Color::Indexed(_)));
        assert!(matches!(ansi256.header.fg, Some(Color::Indexed(_))));

        let ansi16 = build(ThemeVariant::Dark, ColorMode::Ansi16);
        assert!(!matches!(ansi16.bg, Color::Rgb(..) | Color::Indexed(_)));

        let none = build(ThemeVariant::Dark, ColorMode::None);
        assert_eq!(none.bg, Color::Reset);
        assert_eq!(none.footer.fg, Some(Color::Reset));
        assert!(none.selection.add_modifier.contains(Modifier::REVERSED));
    }

    #[test]
    fn ansi256_picks_the_nearest_cube_and_grey_levels() {
        // Exact cube levels map to themselves: (95,95,95) is index 59.
        assert_eq!(rgb_to_ansi256(95, 95, 95), Color::Indexed(59));
        assert_eq!(rgb_to_ansi256(255, 255, 255), Color::Indexed(231));
        assert_eq!(rgb_to_ansi256(0, 0, 0), Color::Indexed(16));
        // Mid greys land on the 232..=255 ramp instead of the colour cube.
        assert!(matches!(rgb_to_ansi256(128, 128, 128), Color::Indexed(i) if i >= 232));
    }

    #[test]
    fn ansi16_picks_the_nearest_base_colour() {
        assert_eq!(rgb_to_ansi16(255, 255, 255), Color::White);
        assert_eq!(rgb_to_ansi16(0, 0, 0), Color::Black);
        assert_eq!(rgb_to_ansi16(200, 20, 30), Color::LightRed);
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
}
