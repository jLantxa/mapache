use std::{
    path::PathBuf,
    sync::{Arc, atomic::AtomicBool},
};

use async_trait::async_trait;
use crossterm::event::{KeyCode, KeyEvent, MouseButton, MouseEvent};
use ratatui::{
    Frame,
    layout::{Constraint, Direction, Layout, Margin, Rect},
    style::{Modifier, Style},
    text::{Line, Span, Text},
    widgets::{List, ListItem, ListState, Paragraph},
};

use crate::{
    common::{ID, error::Result},
    fs::{node::Node, tree::Tree},
    repository::{repo::Repository, snapshot::SnapshotEntry},
    ui::tui::{
        app::{Screen, Transition},
        background::BackgroundTask,
        screens::restore::RestoreScreen,
        theme,
        widgets::{
            FilterState, Spinner, StateNavigation, ToastSink, click_to_index, impl_attach_toasts,
            truncate_with_ellipsis, wrap_line,
        },
    },
    utils,
};

const METADATA_HEIGHT: u16 = 8;

/// A directory open in flight: remembers what was selected and where so the
/// finished tree can land in the right place on the navigation stack.
struct LoadingNav {
    node_name: String,
    previous_selection: usize,
}

struct PathStackEntry {
    tree: Tree,
    previous_selection: usize,
}

pub struct FileExplorerScreen {
    repo: Arc<Repository>,
    snapshot: SnapshotEntry,
    current_tree: Tree,
    path_stack: Vec<PathStackEntry>,
    current_path: PathBuf,
    list_state: ListState,
    last_height: u16,
    last_list_area: Rect,
    filter: FilterState,
    loading: bool,
    spinner: Spinner,
    pending_nav: Option<LoadingNav>,
    load_task: Option<BackgroundTask<Tree>>,
    toasts: ToastSink,
}

impl FileExplorerScreen {
    pub async fn new(
        repo: Arc<Repository>,
        snapshot: SnapshotEntry,
        root_tree_id: &ID,
    ) -> Result<Self> {
        let mut root_tree = Tree::load_from_repo(&repo, root_tree_id).await?;
        Self::sort_nodes(&mut root_tree.nodes);
        let mut list_state = ListState::default();
        if !root_tree.nodes.is_empty() {
            list_state.select(Some(0));
        }

        Ok(Self {
            repo,
            snapshot,
            current_tree: root_tree,
            path_stack: Vec::new(),
            current_path: PathBuf::from("/"),
            list_state,
            last_height: 0,
            last_list_area: Rect::default(),
            filter: FilterState::new(),
            loading: false,
            spinner: Spinner::default(),
            pending_nav: None,
            load_task: None,
            toasts: ToastSink::default(),
        })
    }

    fn sort_nodes(nodes: &mut [Node]) {
        nodes.sort_unstable_by(|a, b| {
            if a.is_dir() && !b.is_dir() {
                std::cmp::Ordering::Less
            } else if !a.is_dir() && b.is_dir() {
                std::cmp::Ordering::Greater
            } else {
                a.name.cmp(&b.name)
            }
        });
    }

    /// The nodes currently listed: the whole directory when no filter text is
    /// being typed or committed, otherwise the ones whose name contains it.
    /// While the user is typing (`FilterState::is_active`) the live text is
    /// used, matching the diff screen; a committed query is used otherwise.
    fn visible_nodes<'s>(nodes: &'s [Node], filter: &'s FilterState) -> Vec<&'s Node> {
        let text = filter.active_text().or_else(|| filter.query());
        match text {
            Some(text) if !text.is_empty() => {
                let lower = text.to_lowercase();
                nodes
                    .iter()
                    .filter(|n| n.name.to_lowercase().contains(&lower))
                    .collect()
            }
            _ => nodes.iter().collect(),
        }
    }

    /// Starts loading the subtree for `tree_id` in the background. The event
    /// loop keeps running; `poll_loads` applies the result once it arrives.
    fn start_loading(&mut self, tree_id: ID, node_name: &str) {
        self.loading = true;
        self.spinner.reset();
        self.pending_nav = Some(LoadingNav {
            node_name: node_name.to_string(),
            previous_selection: self.list_state.selected().unwrap_or(0),
        });

        let repo = self.repo.clone();
        let shutdown = Arc::new(AtomicBool::new(false));
        self.load_task = Some(BackgroundTask::spawn_async(shutdown, async move {
            Tree::load_from_repo(&repo, &tree_id)
                .await
                .map_err(|error| error.to_string())
        }));
    }

    /// Applies a finished background load, if any, and clears the loading
    /// state. Called at the top of every frame.
    fn poll_loads(&mut self) {
        let Some(result) = self.load_task.as_mut().and_then(BackgroundTask::poll) else {
            return;
        };
        self.load_task = None;

        let pending = self.pending_nav.take();
        match result {
            Ok(mut new_tree) => {
                Self::sort_nodes(&mut new_tree.nodes);
                let old_tree = std::mem::replace(&mut self.current_tree, new_tree);
                if let Some(nav) = pending {
                    self.path_stack.push(PathStackEntry {
                        tree: old_tree,
                        previous_selection: nav.previous_selection,
                    });
                    self.current_path.push(&nav.node_name);
                }
                self.list_state.select(Some(0));
                self.filter.clear();
            }
            Err(e) => {
                tracing::warn!(
                    "Failed to load tree for '{}': {}",
                    pending
                        .as_ref()
                        .map(|n| n.node_name.as_str())
                        .unwrap_or("?"),
                    e
                );
                if let Some(nav) = pending {
                    self.toasts
                        .warning(format!("Failed to load tree for '{}': {e}", nav.node_name));
                }
            }
        }
        self.loading = false;
    }

    /// Build the breadcrumb trail as one or more lines, wrapping at `width` so
    /// deep paths do not run off the right edge on narrow terminals. Components
    /// are kept whole where possible; a single component wider than the row
    /// falls back to character wrapping so it is still readable.
    fn breadcrumb_lines(&self, width: u16) -> Vec<Line<'static>> {
        let width = (width as usize).max(1);
        let mut lines: Vec<Line<'static>> = Vec::new();
        let mut spans = vec![Span::styled(" \u{2302} ", theme::THEME.breadcrumb)];
        let mut cur = 3usize; // width of the home glyph span

        for part in self
            .current_path
            .iter()
            .filter_map(|c| c.to_str())
            // The root directory component reports itself as "/" on Unix; the
            // home glyph already stands for the root, so drop it or the trail
            // reads "⌂  / /  / foo" instead of "⌂  / foo".
            .filter(|part| *part != "/")
        {
            // " / " plus the component name.
            let token_width = 3 + part.chars().count();

            if !spans.is_empty() && cur + token_width > width {
                lines.push(Line::from(std::mem::take(&mut spans)));
                cur = 0;
            }

            if token_width > width {
                // The component cannot fit on a row of its own.
                let token = Line::from(vec![
                    Span::styled(" / ", theme::THEME.footer),
                    Span::styled(part.to_string(), theme::THEME.snap_host),
                ]);
                let mut wrapped = Vec::new();
                wrap_line(&token, width, &mut wrapped);
                let last = wrapped.pop().unwrap_or_default();
                cur = last.spans.iter().map(|s| s.content.chars().count()).sum();
                lines.extend(wrapped);
                spans = last.spans;
            } else {
                spans.push(Span::styled(" / ", theme::THEME.footer));
                spans.push(Span::styled(part.to_string(), theme::THEME.snap_host));
                cur += token_width;
            }
        }

        if !spans.is_empty() {
            lines.push(Line::from(spans));
        }
        if lines.is_empty() {
            lines.push(Line::from(Span::raw("")));
        }
        lines
    }

    fn render_metadata(&self, frame: &mut Frame, area: Rect, node: &Node) {
        let mut lines = Vec::with_capacity(5);
        let lw = 10;

        lines.push(theme::field_spans(
            "Size",
            lw,
            vec![Span::styled(
                utils::format_size_binary(node.metadata.size, 3),
                theme::THEME.snap_size,
            )],
        ));

        if let Some(mtime) = node.metadata.modified_time {
            lines.push(theme::field_spans(
                "Modified",
                lw,
                vec![Span::styled(
                    utils::pretty_print_timestamp(&mtime.into(), None),
                    theme::THEME.snap_date,
                )],
            ));
        }

        if let Some(ctime) = node.metadata.created_time {
            lines.push(theme::field_spans(
                "Created",
                lw,
                vec![Span::styled(
                    utils::pretty_print_timestamp(&ctime.into(), None),
                    theme::THEME.subtext,
                )],
            ));
        }

        if let Some(mode) = node.metadata.mode {
            lines.push(theme::field_spans(
                "Mode",
                lw,
                vec![Span::styled(format!("{:o}", mode), theme::THEME.footer)],
            ));
        }

        if node.is_symlink() {
            lines.push(theme::field_spans(
                "Target",
                lw,
                vec![Span::styled(
                    node.symlink_info
                        .as_ref()
                        .map(|s| s.target_path.display().to_string())
                        .unwrap_or_else(|| "?".to_string()),
                    theme::THEME.symlink_fg,
                )],
            ));
        }

        let widget = Paragraph::new(Text::from(lines)).block(theme::block("Info"));
        frame.render_widget(widget, area);
    }
}

#[async_trait]
impl Screen for FileExplorerScreen {
    fn render(&mut self, frame: &mut Frame) {
        self.poll_loads();
        self.spinner.tick();

        let inner = frame.area().inner(Margin::new(2, 0));

        let nodes = Self::visible_nodes(&self.current_tree.nodes, &self.filter);
        let selected_is_file = self
            .list_state
            .selected()
            .and_then(|i| nodes.get(i))
            .is_some_and(|n| n.is_file());

        let metadata_height = if selected_is_file { METADATA_HEIGHT } else { 0 };

        // Breadcrumb and footer wrap to as many rows as the width needs, so on
        // a narrow terminal hints like "r restore" / "q quit" are no longer
        // clipped past the right edge.
        let breadcrumb = self.breadcrumb_lines(inner.width);
        let footer = theme::key_hint_lines(&self.help_hints(), inner.width);
        let breadcrumb_height = (breadcrumb.len() as u16).min(inner.height);
        let footer_height = (footer.len() as u16).min(inner.height);
        let filter_height = if self.filter.is_active() { 3 } else { 0 };

        let chunks = Layout::default()
            .direction(Direction::Vertical)
            .constraints([
                Constraint::Length(breadcrumb_height),
                Constraint::Min(3),
                Constraint::Length(filter_height),
                Constraint::Length(metadata_height),
                Constraint::Length(footer_height),
            ])
            .split(inner);

        self.last_height = chunks[1].height.saturating_sub(2);
        self.last_list_area = chunks[1];
        frame.render_widget(Paragraph::new(Text::from(breadcrumb)), chunks[0]);

        if self.filter.is_active() {
            self.filter.render(frame, chunks[2], "Filter by name");
        }

        if self.loading {
            let spinner = self.spinner.glyph();
            let msg = Paragraph::new(Line::from(Span::styled(
                format!(" {} Loading directory\u{2026} ", spinner),
                theme::THEME.info,
            )))
            .block(theme::block("Files"));
            frame.render_widget(msg, chunks[1]);
        } else if nodes.is_empty() {
            let hint = if self.filter.has_query() {
                "No entries match the filter."
            } else {
                "This directory is empty."
            };
            theme::empty_state(frame, chunks[1], "Files", hint);
        } else {
            let title = if self.filter.has_query() {
                format!("Files matched ({})", nodes.len())
            } else {
                "Files".to_string()
            };
            // Rows cannot wrap, so shorten names that would overflow the list.
            // Reserve two columns for the highlight symbol, two for the
            // symlink arrow and the block borders, plus the size suffix on
            // file rows.
            let name_max = (chunks[1].width as usize)
                .saturating_sub(2) // block borders
                .saturating_sub(2); // highlight symbol
            let items: Vec<ListItem<'_>> = nodes
                .iter()
                .map(|node| {
                    let sym = if node.is_symlink() { "\u{21C4} " } else { "" };

                    let name_style = if node.is_dir() {
                        Style::default()
                            .fg(theme::THEME.dir_fg)
                            .add_modifier(Modifier::BOLD)
                    } else if node.is_symlink() {
                        Style::default()
                            .fg(theme::THEME.symlink_fg)
                            .add_modifier(Modifier::ITALIC)
                    } else {
                        Style::default().fg(theme::THEME.file_fg)
                    };

                    let display_name = if node.is_dir() {
                        format!("{}/", node.name)
                    } else {
                        node.name.clone()
                    };
                    let name_max = name_max
                        .saturating_sub(if node.is_symlink() { 2 } else { 0 })
                        .saturating_sub(if node.is_file() { 14 } else { 0 });

                    let mut spans = vec![
                        Span::styled(sym, name_style),
                        Span::styled(
                            truncate_with_ellipsis(&display_name, name_max.max(1)),
                            name_style,
                        ),
                    ];

                    if node.is_file() {
                        spans.push(Span::raw(" "));
                        spans.push(Span::styled(
                            utils::format_size_binary(node.metadata.size, 3),
                            theme::THEME.file_size,
                        ));
                    }

                    ListItem::new(Line::from(spans))
                })
                .collect();

            let list = List::new(items)
                .block(theme::block(&title))
                .highlight_style(theme::THEME.selection)
                .highlight_symbol("  ");

            frame.render_stateful_widget(list, chunks[1], &mut self.list_state);

            if nodes.len() > self.last_height as usize {
                theme::render_scrollbar(
                    frame,
                    chunks[1],
                    nodes.len(),
                    self.list_state.selected().unwrap_or(0),
                );
            }
        }

        if let Some(i) = self.list_state.selected()
            && selected_is_file
        {
            self.render_metadata(frame, chunks[3], nodes[i]);
        }

        frame.render_widget(Paragraph::new(Text::from(footer)), chunks[4]);
    }

    async fn handle_key(&mut self, key: KeyEvent) -> Option<Transition> {
        if self.loading {
            // The subtree load happens in the background; let the user back
            // out while it runs. The task finishes harmlessly once its
            // receiver is dropped.
            return match key.code {
                KeyCode::Esc => Some(Transition::Pop),
                KeyCode::Char('q') => Some(Transition::Quit),
                _ => None,
            };
        }

        if self.filter.is_active() {
            self.filter.handle_key(key.code);
            return None;
        }

        let nodes = Self::visible_nodes(&self.current_tree.nodes, &self.filter);
        match key.code {
            KeyCode::Esc => {
                if self.filter.has_query() {
                    self.filter.clear();
                    None
                } else {
                    Some(Transition::Pop)
                }
            }
            KeyCode::Char('q') => Some(Transition::Quit),
            KeyCode::Char('/') => {
                self.filter.open();
                None
            }
            KeyCode::Char(' ') => {
                self.list_state
                    .page_next(nodes.len(), self.last_height as usize);
                None
            }
            key if self
                .list_state
                .handle_nav_keys(key, nodes.len(), self.last_height as usize) =>
            {
                None
            }
            KeyCode::Enter | KeyCode::Right | KeyCode::Char('l') => {
                if let Some(i) = self.list_state.selected()
                    && let Some(node) = nodes.get(i)
                    && let Some(tree_id) = node.tree
                {
                    let node_name = node.name.clone();
                    self.start_loading(tree_id, &node_name);
                }
                None
            }
            KeyCode::Char('r') => {
                if let Some(i) = self.list_state.selected() {
                    let node_name = &nodes[i].name;
                    let mut path = self.current_path.clone();
                    if let Ok(stripped) = path.strip_prefix("/") {
                        path = stripped.to_path_buf();
                    }
                    let restore_path = path.join(node_name);

                    Some(Transition::Push(Box::new(RestoreScreen::new(
                        self.repo.clone(),
                        self.snapshot.clone(),
                        Some(vec![restore_path]),
                    ))))
                } else {
                    None
                }
            }
            KeyCode::Backspace | KeyCode::Left | KeyCode::Char('h') => {
                if let Some(entry) = self.path_stack.pop() {
                    self.current_tree = entry.tree;
                    self.current_path.pop();
                    self.list_state.select(Some(entry.previous_selection));
                    self.filter.clear();
                }
                None
            }
            _ => None,
        }
    }

    async fn handle_mouse(&mut self, mouse: MouseEvent) -> bool {
        if self.loading
            || self.filter.is_active()
            || !matches!(
                mouse.kind,
                crossterm::event::MouseEventKind::Down(MouseButton::Left)
            )
        {
            return false;
        }
        let len = Self::visible_nodes(&self.current_tree.nodes, &self.filter).len();
        let Some(idx) = click_to_index(mouse.row, self.list_state.offset(), self.last_list_area, 0)
        else {
            return false;
        };
        if idx < len {
            self.list_state.select(Some(idx));
            return true;
        }
        false
    }

    fn help_hints(&self) -> Vec<(&'static str, &'static str)> {
        if self.loading {
            return vec![("Esc", "back"), ("q", "back")];
        }
        if self.filter.is_active() {
            return vec![("Enter", "apply filter"), ("Esc", "cancel")];
        }
        vec![
            ("Esc", "back"),
            ("\u{2191}\u{2193} j/k", "navigate"),
            ("\u{2192} l", "open"),
            ("\u{2190} h", "up"),
            ("/", "filter"),
            ("Space", "page down"),
            ("r", "restore"),
            ("q", "back"),
        ]
    }

    fn text_input_active(&self) -> bool {
        self.filter.is_active()
    }

    impl_attach_toasts!(toasts);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        backend::{StorageBackend, mock::MockBackend},
        common::{defaults::TEST_REPO_CONFIG, traits::BlobSaver},
        repository::{
            repo::{Auth, THIS_REPOSITORY_VERSION},
            snapshot::Snapshot,
        },
    };
    use zeroize::Zeroizing;

    use crate::ui::tui::widgets::FilterAction;
    use crossterm::event::{KeyModifiers, MouseButton, MouseEvent, MouseEventKind};

    async fn make_repo() -> Result<(Arc<Repository>, ID)> {
        let auth = Auth {
            username: "test".to_string(),
            password: Zeroizing::new("password".to_string()),
        };
        let backend: Arc<dyn StorageBackend> = Arc::new(MockBackend::new());
        Repository::init(
            THIS_REPOSITORY_VERSION,
            &auth,
            None,
            backend.clone(),
            None,
            false,
        )
        .await?;
        let (repo, _) =
            Repository::try_open_unlocked(&auth, None, backend.clone(), TEST_REPO_CONFIG).await?;

        repo.init_pack_saver(2)?;
        let mut root = Tree::new(Vec::new());
        let root_id = root
            .save_to_store(repo.clone() as Arc<dyn BlobSaver>)
            .await?;
        repo.flush_and_finalize_pack_saver().await?;
        Ok((repo, root_id))
    }

    async fn make_screen() -> Result<FileExplorerScreen> {
        let (repo, root_id) = make_repo().await?;

        let entry = SnapshotEntry {
            id: ID::default(),
            snapshot: Snapshot::default(),
            active: true,
        };
        FileExplorerScreen::new(repo, entry, &root_id).await
    }

    #[tokio::test]
    async fn navigation_controls_wrap_on_a_narrow_terminal() -> Result<()> {
        let mut screen = make_screen().await?;
        screen.current_path = PathBuf::from("/very/deeply/nested/directory/path");

        let rendered = crate::ui::tui::test_support::render_text(40, 20, |frame| {
            screen.render(frame);
        });

        // The trailing hints used to be clipped past the right edge.
        for hint in ["page down", "restore", "back"] {
            assert!(
                rendered.contains(hint),
                "footer hint '{hint}' was clipped on a 40-column terminal"
            );
        }
        // The breadcrumb is wrapped at component boundaries, not cut off.
        assert!(
            rendered.contains("directory"),
            "breadcrumb component was clipped on a 40-column terminal"
        );
        Ok(())
    }

    #[tokio::test]
    async fn breadcrumb_does_not_repeat_the_separator_at_the_root() -> Result<()> {
        let mut screen = make_screen().await?;
        for path in ["/", "/foo/bar"] {
            screen.current_path = PathBuf::from(path);
            let lines = screen.breadcrumb_lines(40);
            let text: String = lines
                .iter()
                .flat_map(|l| l.spans.iter().map(|s| s.content.as_ref()))
                .collect();
            assert!(
                !text.contains("/ /"),
                "breadcrumb for '{path}' repeated the separator: {text:?}"
            );
        }
        assert!(
            screen.breadcrumb_lines(40)[0].spans[0]
                .content
                .contains('\u{2302}')
        );
        Ok(())
    }

    #[tokio::test]
    async fn opening_a_directory_is_asynchronous() -> Result<()> {
        let (repo, root_id) = make_repo().await?;
        let entry = SnapshotEntry {
            id: ID::default(),
            snapshot: Snapshot::default(),
            active: true,
        };
        let mut screen = FileExplorerScreen::new(repo.clone(), entry, &root_id).await?;

        // Give the root a subdirectory backed by a real stored tree.
        let mut sub = Tree::new(Vec::new());
        sub.nodes.push(Node {
            name: "file.txt".into(),
            ..Default::default()
        });
        repo.init_pack_saver(2)?;
        let sub_id = sub
            .save_to_store(repo.clone() as Arc<dyn BlobSaver>)
            .await?;
        repo.flush_and_finalize_pack_saver().await?;

        screen.current_tree.nodes.push(Node {
            name: "subdir".into(),
            tree: Some(sub_id),
            ..Default::default()
        });
        screen.list_state.select(Some(0));

        // 'l' starts the load in the background instead of freezing the loop.
        let transition = screen.handle_key(KeyEvent::from(KeyCode::Char('l'))).await;
        assert!(transition.is_none());
        assert!(
            screen.loading,
            "opening a subtree must not block the event loop"
        );

        // Poll (bounded) until the background load lands.
        for _ in 0..50 {
            tokio::task::yield_now().await;
            screen.poll_loads();
            if !screen.loading {
                break;
            }
        }
        assert!(!screen.loading, "subtree load never completed");
        assert_eq!(screen.current_tree.nodes.len(), 1);
        assert_eq!(screen.current_tree.nodes[0].name, "file.txt");
        assert_eq!(screen.current_path, PathBuf::from("/subdir"));
        Ok(())
    }

    #[tokio::test]
    async fn click_selects_the_item_under_the_cursor() -> Result<()> {
        let mut screen = make_screen().await?;
        screen.current_tree.nodes = vec![
            Node {
                name: "a".into(),
                ..Default::default()
            },
            Node {
                name: "b".into(),
                ..Default::default()
            },
            Node {
                name: "c".into(),
                ..Default::default()
            },
        ];

        // Render once so `last_list_area` is populated before simulating clicks.
        crate::ui::tui::test_support::render_text(60, 30, |frame| {
            screen.render(frame);
        });

        let click_on = |row: u16| MouseEvent {
            kind: MouseEventKind::Down(MouseButton::Left),
            column: 4,
            row,
            modifiers: KeyModifiers::NONE,
        };

        // The first item sits directly under the block border of the list.
        let first_item_row = screen.last_list_area.y + 1;
        assert!(screen.handle_mouse(click_on(first_item_row)).await);
        assert_eq!(screen.list_state.selected(), Some(0));

        assert!(screen.handle_mouse(click_on(first_item_row + 2)).await);
        assert_eq!(screen.list_state.selected(), Some(2));

        // A click on the block border or title selects nothing.
        assert!(!screen.handle_mouse(click_on(screen.last_list_area.y)).await);
        Ok(())
    }

    #[test]
    fn filter_narrows_visible_nodes_by_name() {
        let nodes = vec![
            Node {
                name: "alice.txt".into(),
                ..Default::default()
            },
            Node {
                name: "bob.bin".into(),
                ..Default::default()
            },
            Node {
                name: "album".into(),
                ..Default::default()
            },
        ];

        let mut filter = FilterState::new();
        assert_eq!(
            FileExplorerScreen::visible_nodes(&nodes, &filter).len(),
            3,
            "no filter list everything"
        );

        // Typing narrows the listing live, before the query is committed.
        filter.open();
        for ch in ['a', 'l'] {
            filter.handle_key(KeyCode::Char(ch));
        }
        assert!(filter.is_active());
        assert_eq!(FileExplorerScreen::visible_nodes(&nodes, &filter).len(), 2);

        // Enter commits the query; the narrowing survives a cancelled edit.
        assert!(matches!(
            filter.handle_key(KeyCode::Enter),
            FilterAction::Apply
        ));
        assert!(!filter.is_active());
        filter.open();
        filter.handle_key(KeyCode::Esc);
        assert!(!filter.is_active());
        assert_eq!(FileExplorerScreen::visible_nodes(&nodes, &filter).len(), 2);

        // Clearing the filter restores the full listing.
        filter.clear();
        assert_eq!(FileExplorerScreen::visible_nodes(&nodes, &filter).len(), 3);
    }
}
