use std::{path::PathBuf, sync::Arc};

use anyhow::Result;
use async_trait::async_trait;
use crossterm::event::{KeyCode, KeyEvent};
use ratatui::{
    Frame,
    layout::{Constraint, Direction, Layout, Margin, Rect},
    style::{Modifier, Style},
    text::{Line, Span, Text},
    widgets::{List, ListItem, ListState, Paragraph},
};

use crate::{
    fs::{node::Node, tree::Tree},
    mapache::ID,
    repository::{repo::Repository, snapshot::SnapshotEntry},
    ui::tui::{
        app::{Screen, Transition},
        screens::restore::RestoreScreen,
        theme,
    },
    utils,
};

const METADATA_HEIGHT: u16 = 8;
const TITLE_HEIGHT: u16 = 1;

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

    fn render_breadcrumb(&self, frame: &mut Frame, area: Rect) {
        let parts: Vec<&str> = self
            .current_path
            .iter()
            .filter_map(|c| c.to_str())
            .collect();
        let mut spans = vec![Span::styled(" \u{2302} ", theme::THEME.breadcrumb)];
        for part in parts.iter() {
            spans.push(Span::styled(" / ", theme::THEME.footer));
            spans.push(Span::styled(*part, theme::THEME.snap_host));
        }
        let breadcrumb = Paragraph::new(Line::from(spans));
        frame.render_widget(breadcrumb, area);
    }

    fn render_metadata(&self, frame: &mut Frame, area: Rect, node: &Node) {
        let mut lines = Vec::with_capacity(5);
        let lw = 10;

        lines.push(Line::from(vec![
            Span::styled(format!("{:lw$}", "Size", lw = lw), theme::THEME.menu_key),
            Span::styled(
                utils::format_size_binary(node.metadata.size, 2),
                theme::THEME.snap_size,
            ),
        ]));

        if let Some(mtime) = node.metadata.modified_time {
            lines.push(Line::from(vec![
                Span::styled(
                    format!("{:lw$}", "Modified", lw = lw),
                    theme::THEME.menu_key,
                ),
                Span::styled(
                    utils::pretty_print_timestamp(&mtime.into(), None),
                    theme::THEME.snap_date,
                ),
            ]));
        }

        if let Some(ctime) = node.metadata.created_time {
            lines.push(Line::from(vec![
                Span::styled(format!("{:lw$}", "Created", lw = lw), theme::THEME.menu_key),
                Span::styled(
                    utils::pretty_print_timestamp(&ctime.into(), None),
                    theme::THEME.subtext,
                ),
            ]));
        }

        if let Some(mode) = node.metadata.mode {
            lines.push(Line::from(vec![
                Span::styled(format!("{:lw$}", "Mode", lw = lw), theme::THEME.menu_key),
                Span::styled(format!("{:o}", mode), theme::THEME.footer),
            ]));
        }

        if node.is_symlink() {
            lines.push(Line::from(vec![
                Span::styled(format!("{:lw$}", "Target", lw = lw), theme::THEME.menu_key),
                Span::styled(
                    node.symlink_info
                        .as_ref()
                        .map(|s| s.target_path.display().to_string())
                        .unwrap_or_else(|| "?".to_string()),
                    theme::THEME.symlink_fg,
                ),
            ]));
        }

        let widget = Paragraph::new(Text::from(lines)).block(theme::block("Info"));
        frame.render_widget(widget, area);
    }

    fn render_title(&self, frame: &mut Frame, area: Rect) {
        let footer = theme::key_hint_footer(&[
            ("Esc", "back"),
            ("\u{2192}", "open"),
            ("\u{2190}", "up"),
            ("r", "restore"),
            ("q", "quit"),
        ]);
        frame.render_widget(Paragraph::new(footer), area);
    }
}

#[async_trait]
impl Screen for FileExplorerScreen {
    fn render(&mut self, frame: &mut Frame) {
        let area = frame.area();
        let inner = area.inner(Margin::new(2, 0));

        let selected_is_file = self
            .list_state
            .selected()
            .and_then(|i| self.current_tree.nodes.get(i))
            .is_some_and(|n| n.is_file());

        let metadata_height = if selected_is_file { METADATA_HEIGHT } else { 0 };

        let chunks = Layout::default()
            .direction(Direction::Vertical)
            .constraints([
                Constraint::Length(1),
                Constraint::Min(3),
                Constraint::Length(metadata_height),
                Constraint::Length(TITLE_HEIGHT),
            ])
            .split(inner);

        self.last_height = chunks[1].height.saturating_sub(2);
        self.render_breadcrumb(frame, chunks[0]);

        let items: Vec<ListItem<'_>> = self
            .current_tree
            .nodes
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

                let mut spans = vec![
                    Span::styled(sym, name_style),
                    Span::styled(display_name, name_style),
                ];

                if node.is_file() {
                    spans.push(Span::raw(" "));
                    spans.push(Span::styled(
                        utils::format_size_binary(node.metadata.size, 1),
                        theme::THEME.file_size,
                    ));
                }

                ListItem::new(Line::from(spans))
            })
            .collect();

        let list = List::new(items)
            .block(theme::block("Files"))
            .highlight_style(theme::THEME.selection)
            .highlight_symbol("  ");

        frame.render_stateful_widget(list, chunks[1], &mut self.list_state);

        if self.current_tree.nodes.len() > self.last_height as usize {
            theme::render_scrollbar(
                frame,
                chunks[1],
                self.current_tree.nodes.len(),
                self.list_state.selected().unwrap_or(0),
            );
        }

        if let Some(i) = self.list_state.selected()
            && selected_is_file
        {
            self.render_metadata(frame, chunks[2], &self.current_tree.nodes[i]);
        }

        self.render_title(frame, chunks[3]);
    }

    async fn handle_key(&mut self, key: KeyEvent) -> Option<Transition> {
        use crate::ui::tui::widgets::StateNavigation;
        match key.code {
            KeyCode::Esc => Some(Transition::Pop),
            KeyCode::Char('q') => Some(Transition::Quit),
            KeyCode::Up => {
                self.list_state.previous(self.current_tree.nodes.len());
                None
            }
            KeyCode::Down => {
                self.list_state.next(self.current_tree.nodes.len());
                None
            }
            KeyCode::PageDown | KeyCode::Char(' ') => {
                self.list_state
                    .page_next(self.current_tree.nodes.len(), self.last_height as usize);
                None
            }
            KeyCode::PageUp => {
                self.list_state
                    .page_previous(self.current_tree.nodes.len(), self.last_height as usize);
                None
            }
            KeyCode::Home => {
                self.list_state.home(self.current_tree.nodes.len());
                None
            }
            KeyCode::End => {
                self.list_state.end(self.current_tree.nodes.len());
                None
            }
            KeyCode::Enter | KeyCode::Right => {
                if let Some(i) = self.list_state.selected() {
                    let (tree_id, node_name) = {
                        let node = &self.current_tree.nodes[i];
                        (node.tree, node.name.clone())
                    };

                    if let Some(tree_id) = tree_id {
                        match Tree::load_from_repo(&self.repo, &tree_id).await {
                            Ok(mut new_tree) => {
                                Self::sort_nodes(&mut new_tree.nodes);
                                let old_tree = std::mem::replace(&mut self.current_tree, new_tree);
                                let prev_sel = self.list_state.selected().unwrap_or(0);
                                self.path_stack.push(PathStackEntry {
                                    tree: old_tree,
                                    previous_selection: prev_sel,
                                });
                                self.current_path.push(&node_name);
                                self.list_state.select(Some(0));
                            }
                            Err(e) => {
                                tracing::warn!("Failed to load tree for '{}': {}", node_name, e);
                            }
                        }
                    }
                }
                None
            }
            KeyCode::Char('r') => {
                if let Some(i) = self.list_state.selected() {
                    let node_name = &self.current_tree.nodes[i].name;
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
            KeyCode::Backspace | KeyCode::Left => {
                if let Some(entry) = self.path_stack.pop() {
                    self.current_tree = entry.tree;
                    self.current_path.pop();
                    self.list_state.select(Some(entry.previous_selection));
                }
                None
            }
            _ => None,
        }
    }
}
