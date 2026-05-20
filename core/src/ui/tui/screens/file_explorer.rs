use std::{path::PathBuf, sync::Arc};

use anyhow::Result;
use async_trait::async_trait;
use crossterm::event::{KeyCode, KeyEvent};
use ratatui::{
    Frame,
    layout::{Constraint, Direction, Layout, Rect},
    style::{Color, Modifier, Style},
    text::{Line, Span, Text},
    widgets::{List, ListItem, ListState, Paragraph, ScrollbarState},
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

const METADATA_HEIGHT: u16 = 7;
const TITLE_HEIGHT: u16 = 1;
const BREADCRUMB_HEIGHT: u16 = 1;
const LIST_HEIGHT_ESTIMATE: u16 = 10;

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
            last_height: LIST_HEIGHT_ESTIMATE,
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
        let breadcrumb = Paragraph::new(format!(" Path: {}", self.current_path.display())).style(
            Style::default()
                .fg(Color::Yellow)
                .add_modifier(Modifier::BOLD),
        );
        frame.render_widget(breadcrumb, area);
    }

    fn render_metadata(&self, frame: &mut Frame, area: Rect, node: &Node) {
        let mut lines = Vec::new();
        lines.push(Line::from(vec![
            Span::styled(" Name: ", Style::default().add_modifier(Modifier::BOLD)),
            Span::raw(&node.name),
        ]));
        lines.push(Line::from(vec![
            Span::styled(" Size: ", Style::default().add_modifier(Modifier::BOLD)),
            Span::raw(utils::format_size_binary(node.metadata.size, 3)),
        ]));
        lines.push(Line::from(vec![
            Span::styled(" Mode: ", Style::default().add_modifier(Modifier::BOLD)),
            Span::raw(format!("{:o}", node.metadata.mode.unwrap_or(0))),
        ]));
        lines.push(Line::from(vec![
            Span::styled(" UID: ", Style::default().add_modifier(Modifier::BOLD)),
            Span::raw(
                node.metadata
                    .owner_uid
                    .map_or("unknown".to_string(), |u| u.to_string()),
            ),
        ]));
        lines.push(Line::from(vec![
            Span::styled(" GID: ", Style::default().add_modifier(Modifier::BOLD)),
            Span::raw(
                node.metadata
                    .owner_gid
                    .map_or("unknown".to_string(), |g| g.to_string()),
            ),
        ]));

        if let Some(mtime) = node.metadata.modified_time {
            let ts = utils::pretty_print_timestamp(&mtime.into(), None);
            lines.push(Line::from(vec![
                Span::styled(" Modified: ", Style::default().add_modifier(Modifier::BOLD)),
                Span::raw(ts.to_string()),
            ]));
        }

        let widget = Paragraph::new(Text::from(lines)).block(theme::themed_block("Metadata"));
        frame.render_widget(widget, area);
    }

    fn render_title(&self, frame: &mut Frame, area: Rect) {
        let footer = Line::from(vec![
            Span::styled("[Esc]", Style::default().fg(theme::MENU_KEY).bold()),
            Span::raw(" back"),
            Span::raw("    "),
            Span::styled("[Enter/→]", Style::default().fg(theme::MENU_KEY).bold()),
            Span::raw(" open"),
            Span::raw("    "),
            Span::styled("[Backsp/←]", Style::default().fg(theme::MENU_KEY).bold()),
            Span::raw(" up"),
            Span::raw("    "),
            Span::styled("[r]", Style::default().fg(theme::MENU_KEY).bold()),
            Span::raw(" restore"),
            Span::raw("    "),
            Span::styled("[q]", Style::default().fg(theme::MENU_KEY).bold()),
            Span::raw(" quit"),
        ]);
        frame.render_widget(Paragraph::new(footer), area);
    }
}

#[async_trait]
impl Screen for FileExplorerScreen {
    fn render(&mut self, frame: &mut Frame) {
        let area = frame.area();
        let inner = area.inner(ratatui::layout::Margin::new(2, 0));

        let selected_is_file = self
            .list_state
            .selected()
            .is_some_and(|i| self.current_tree.nodes.get(i).is_some_and(|n| n.is_file()));

        let metadata_height = if selected_is_file { METADATA_HEIGHT } else { 0 };

        let chunks = Layout::default()
            .direction(Direction::Vertical)
            .constraints([
                Constraint::Length(BREADCRUMB_HEIGHT),
                Constraint::Min(3),
                Constraint::Length(metadata_height),
                Constraint::Length(TITLE_HEIGHT),
            ])
            .split(inner);

        self.last_height = chunks[1].height.saturating_sub(2);
        self.render_breadcrumb(frame, chunks[0]);

        let items: Vec<ListItem> = self
            .current_tree
            .nodes
            .iter()
            .map(|node| {
                let icon = if node.is_dir() { "📁 " } else { "📄 " };
                let name = &node.name;
                let size = if node.is_file() {
                    format!(" ({})", utils::format_size_binary(node.metadata.size, 1))
                } else {
                    String::new()
                };

                let color = if node.is_dir() {
                    Color::Cyan
                } else {
                    Color::White
                };

                ListItem::new(Line::from(vec![
                    Span::styled(icon, Style::default().fg(Color::Yellow)),
                    Span::styled(name.to_string(), Style::default().fg(color)),
                    Span::styled(size, Style::default().fg(Color::DarkGray)),
                ]))
            })
            .collect();

        let list = List::new(items)
            .block(theme::themed_block("File Explorer"))
            .highlight_style(theme::selected_row_style())
            .highlight_symbol(">> ");

        frame.render_stateful_widget(list, chunks[1], &mut self.list_state);

        if self.current_tree.nodes.len() > self.last_height as usize {
            let scrollbar = theme::create_scrollbar();
            let mut scrollbar_state = ScrollbarState::new(self.current_tree.nodes.len())
                .position(self.list_state.selected().unwrap_or(0))
                .viewport_content_length(self.last_height as usize);

            frame.render_stateful_widget(
                scrollbar,
                chunks[1].inner(ratatui::layout::Margin::new(1, 1)),
                &mut scrollbar_state,
            );
        }

        if selected_is_file && let Some(i) = self.list_state.selected() {
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
                    if path.starts_with("/") {
                        path = path.strip_prefix("/").unwrap().to_path_buf();
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
