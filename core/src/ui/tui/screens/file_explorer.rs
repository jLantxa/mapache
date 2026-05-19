use std::{path::PathBuf, sync::Arc};

use anyhow::Result;
use crossterm::event::KeyCode;
use ratatui::{
    Frame,
    layout::{Constraint, Direction, Layout, Rect},
    style::{Color, Modifier, Style},
    text::{Line, Span},
    widgets::{
        Block, Borders, List, ListItem, ListState, Paragraph, Scrollbar, ScrollbarOrientation,
        ScrollbarState,
    },
};

use crate::{
    fs::{node::Node, tree::Tree},
    mapache::ID,
    repository::repo::Repository,
    ui::tui::theme,
    utils,
};

#[derive(Debug)]
pub enum FileExplorerAction {
    Back,
    Quit,
}

pub struct FileExplorerScreen {
    repo: Arc<Repository>,
    current_tree: Tree,
    path_stack: Vec<(String, Tree)>, // (folder_name, tree)
    current_path: PathBuf,
    list_state: ListState,
    last_height: u16,
}

impl FileExplorerScreen {
    pub async fn new(repo: Arc<Repository>, root_tree_id: &ID) -> Result<Self> {
        let mut root_tree = Tree::load_from_repo(&repo, root_tree_id).await?;
        Self::sort_nodes(&mut root_tree.nodes);
        let mut list_state = ListState::default();
        if !root_tree.nodes.is_empty() {
            list_state.select(Some(0));
        }

        Ok(Self {
            repo,
            current_tree: root_tree,
            path_stack: Vec::new(),
            current_path: PathBuf::from("/"),
            list_state,
            last_height: 10,
        })
    }

    pub async fn handle_key(&mut self, key: KeyCode) -> Option<FileExplorerAction> {
        match key {
            KeyCode::Esc => Some(FileExplorerAction::Back),
            KeyCode::Char('q') => Some(FileExplorerAction::Quit),
            KeyCode::Up => {
                if let Some(i) = self.list_state.selected() {
                    let prev = if i == 0 {
                        self.current_tree.nodes.len().saturating_sub(1)
                    } else {
                        i - 1
                    };
                    self.list_state.select(Some(prev));
                }
                None
            }
            KeyCode::Down => {
                let i = match self.list_state.selected() {
                    Some(i) => {
                        if i >= self.current_tree.nodes.len().saturating_sub(1) {
                            0
                        } else {
                            i + 1
                        }
                    }
                    None => 0,
                };
                self.list_state.select(Some(i));
                None
            }
            KeyCode::PageDown => {
                if let Some(i) = self.list_state.selected() {
                    let next = (i + self.last_height as usize)
                        .min(self.current_tree.nodes.len().saturating_sub(1));
                    self.list_state.select(Some(next));
                }
                None
            }
            KeyCode::PageUp => {
                if let Some(i) = self.list_state.selected() {
                    let prev = i.saturating_sub(self.last_height as usize);
                    self.list_state.select(Some(prev));
                }
                None
            }
            KeyCode::Enter => {
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
                                self.path_stack.push((node_name.clone(), old_tree));
                                self.current_path.push(&node_name);
                                self.list_state.select(Some(0));
                            }
                            Err(_) => {
                                // Handle error?
                            }
                        }
                    }
                }
                None
            }
            KeyCode::Backspace => {
                if let Some((_name, prev_tree)) = self.path_stack.pop() {
                    self.current_tree = prev_tree;
                    self.current_path.pop();
                    self.list_state.select(Some(0)); // Maybe restore previous selection?
                }
                None
            }
            _ => None,
        }
    }

    pub fn render(&mut self, frame: &mut Frame) {
        let area = frame.area();
        let inner = area.inner(ratatui::layout::Margin::new(2, 1));

        let chunks = Layout::default()
            .direction(Direction::Vertical)
            .constraints([Constraint::Length(1), Constraint::Min(3)])
            .split(inner);

        self.last_height = chunks[1].height.saturating_sub(2);
        self.render_title(frame, chunks[0]);

        let items: Vec<ListItem> = self
            .current_tree
            .nodes
            .iter()
            .map(|node| {
                let icon = if node.is_dir() { "📁 " } else { "📄 " };
                let name = node.name.clone();
                let size = if node.is_file() {
                    format!(" ({})", utils::format_size_binary(node.metadata.size, 1))
                } else {
                    "".to_string()
                };

                let color = if node.is_dir() {
                    Color::Cyan
                } else {
                    Color::White
                };

                ListItem::new(Line::from(vec![
                    Span::styled(icon, Style::default()),
                    Span::styled(name, Style::default().fg(color)),
                    Span::styled(size, Style::default().fg(theme::SNAPSHOT_SIZE)),
                ]))
            })
            .collect();

        let list = List::new(items)
            .block(
                Block::default()
                    .borders(Borders::ALL)
                    .title(format!(" Explorer: {} ", self.current_path.display()))
                    .border_style(theme::border_style()),
            )
            .highlight_style(
                Style::default()
                    .bg(Color::DarkGray)
                    .add_modifier(Modifier::BOLD),
            )
            .highlight_symbol(">> ");

        frame.render_stateful_widget(list, chunks[1], &mut self.list_state);

        let total_items = self.current_tree.nodes.len();
        if total_items > chunks[1].height.saturating_sub(2) as usize {
            let scrollbar = Scrollbar::new(ScrollbarOrientation::VerticalRight)
                .begin_symbol(None)
                .end_symbol(None)
                .track_symbol(Some("│"))
                .thumb_symbol("█")
                .style(theme::border_style());

            let mut scrollbar_state =
                ScrollbarState::new(total_items).position(self.list_state.selected().unwrap_or(0));

            frame.render_stateful_widget(
                scrollbar,
                chunks[1].inner(ratatui::layout::Margin::new(0, 1)),
                &mut scrollbar_state,
            );
        }
    }

    fn sort_nodes(nodes: &mut [Node]) {
        nodes.sort_by(|a, b| match (a.is_dir(), b.is_dir()) {
            (true, false) => std::cmp::Ordering::Less,
            (false, true) => std::cmp::Ordering::Greater,
            _ => a.name.to_lowercase().cmp(&b.name.to_lowercase()),
        });
    }

    fn render_title(&self, frame: &mut Frame, area: Rect) {
        let title = Line::from(vec![
            Span::styled("[Esc]", Style::default().fg(theme::MENU_KEY).bold()),
            Span::raw(" back"),
            Span::raw("    "),
            Span::styled("[Enter]", Style::default().fg(theme::MENU_KEY).bold()),
            Span::raw(" open"),
            Span::raw("    "),
            Span::styled("[Backspace]", Style::default().fg(theme::MENU_KEY).bold()),
            Span::raw(" up"),
            Span::raw("    "),
            Span::styled("[q]", Style::default().fg(theme::MENU_KEY).bold()),
            Span::raw(" close"),
        ]);
        frame.render_widget(Paragraph::new(title), area);
    }
}
