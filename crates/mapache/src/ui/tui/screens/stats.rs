use std::sync::{Arc, atomic::AtomicBool};

use async_trait::async_trait;
use crossterm::event::{KeyCode, KeyEvent};
use ratatui::{
    Frame,
    layout::{Constraint, Direction, Layout, Rect},
    text::{Line, Span, Text},
    widgets::Paragraph,
};

use crate::{
    commands::cmd_stats::{FooterReport, StatsReport, collect_footers, collect_stats},
    repository::repo::Repository,
    ui::tui::{
        app::{Screen, Transition},
        background::BackgroundTask,
        theme::{self, kv, kv_styled, section},
        widgets::{Dialog, ScrollState, Spinner, ToastSink, impl_attach_toasts},
    },
    utils::{format_count as count, format_size_binary},
};

/// Read-only repository statistics screen. Collection runs on a background
/// task so the UI stays responsive while the backend is queried.
///
/// On entry the screen asks whether to collect *quick* stats (directory sizes,
/// index and snapshot analysis) or *full* stats (which additionally parse every
/// pack footer). The footer pass can be toggled later with `f` without
/// re-reading the index or the snapshots.
pub struct StatsScreen {
    repo: Arc<Repository>,
    /// Waiting for the user to pick quick or full before any collection starts.
    choosing: bool,
    /// Whether the currently shown report includes parsed pack footers.
    full: bool,
    base_loading: bool,
    footers_loading: bool,
    error: Option<String>,
    base_task: Option<BackgroundTask<StatsReport>>,
    footers_task: Option<BackgroundTask<FooterReport>>,
    report: Option<StatsReport>,
    lines: Vec<Line<'static>>,
    scroll: ScrollState,
    spinner: Spinner,
    toasts: ToastSink,
}

impl StatsScreen {
    pub fn new(repo: Arc<Repository>) -> Self {
        Self {
            repo,
            choosing: true,
            full: false,
            base_loading: false,
            footers_loading: false,
            error: None,
            base_task: None,
            footers_task: None,
            report: None,
            lines: Vec::new(),
            scroll: ScrollState::default(),
            spinner: Spinner::default(),
            toasts: ToastSink::default(),
        }
    }

    /// Collects the base report (directories, index, snapshots) without
    /// parsing pack footers.
    async fn start_base(&mut self) {
        self.request_shutdown();
        self.shutdown().await;
        self.base_task = None;
        self.footers_task = None;
        self.base_loading = true;
        self.footers_loading = false;
        self.error = None;
        self.report = None;
        self.lines.clear();
        self.scroll = ScrollState::default();
        self.spinner.reset();

        let repo = self.repo.clone();
        let secure_storage = repo.secure_storage();
        let backend = repo.backend();
        let shutdown = Arc::new(AtomicBool::new(false));
        self.base_task = Some(BackgroundTask::spawn_async(shutdown.clone(), async move {
            repo.reload_master_index()
                .await
                .map_err(|error| error.to_string())?;
            collect_stats(repo, secure_storage, backend, false, shutdown, &|_msg| {})
                .await
                .map_err(|error| error.to_string())
        }));
    }

    /// Parses every pack footer and merges the result into the report that is
    /// already on screen. This never re-lists the index, the snapshots or the
    /// objects directory: it reuses the pack IDs from the base report.
    fn start_footers(&mut self) {
        if self.base_loading || self.footers_loading || self.report.is_none() {
            // The base collection will chain into the footer pass when it lands.
            return;
        }
        let pack_ids = self
            .report
            .as_ref()
            .map(|report| report.pack_ids.clone())
            .unwrap_or_default();
        self.footers_loading = true;
        self.spinner.reset();

        let repo = self.repo.clone();
        let secure_storage = repo.secure_storage();
        let backend = repo.backend();
        let shutdown = Arc::new(AtomicBool::new(false));
        self.footers_task = Some(BackgroundTask::spawn_async(shutdown.clone(), async move {
            collect_footers(
                repo,
                secure_storage,
                backend,
                &pack_ids,
                shutdown,
                &|_msg| {},
            )
            .await
            .map_err(|error| error.to_string())
        }));
    }

    /// Toggles full stats. Turning full *off* cancels its task; turning it *on* only
    /// runs the pack-footer pass on top of the data already collected.
    async fn toggle_full(&mut self) {
        if self.full {
            self.full = false;
            if let Some(mut task) = self.footers_task.take() {
                task.shutdown().await;
            }
            self.footers_loading = false;
            if let Some(report) = self.report.as_mut() {
                report.footers = None;
                report.full = false;
            }
            self.rebuild_lines();
        } else {
            self.full = true;
            if self.report.is_none() && !self.base_loading {
                self.start_base().await;
            } else {
                self.start_footers();
            }
        }
    }

    fn poll(&mut self) {
        if let Some(task) = self.base_task.as_mut()
            && let Some(result) = task.poll()
        {
            self.base_loading = false;
            match result {
                Ok(mut report) => {
                    report.full = false;
                    report.footers = None;
                    self.report = Some(report);
                    self.rebuild_lines();
                    self.scroll.reset();
                    if self.full {
                        self.start_footers();
                    }
                }
                Err(e) => {
                    self.error = Some(e.clone());
                    self.toasts.error(format!("Failed to collect stats: {e}"));
                }
            }
        }

        if let Some(task) = self.footers_task.as_mut()
            && let Some(result) = task.poll()
        {
            self.footers_loading = false;
            match result {
                Ok(footers) => {
                    if let Some(report) = self.report.as_mut() {
                        report.footers = Some(footers);
                        report.full = true;
                    }
                    self.full = true;
                    self.rebuild_lines();
                }
                Err(e) => {
                    self.full = false;
                    self.toasts
                        .error(format!("Failed to parse pack footers: {e}"));
                }
            }
        }
    }

    fn rebuild_lines(&mut self) {
        self.lines = self.report.as_ref().map(build_lines).unwrap_or_default();
    }

    fn title(&self) -> String {
        if self.footers_loading {
            "Repository stats (parsing pack footers\u{2026})".to_string()
        } else if self.full {
            "Repository stats (full)".to_string()
        } else {
            "Repository stats".to_string()
        }
    }

    fn render_choose(&self, frame: &mut Frame) {
        let text = vec![
            Line::from("Collect repository statistics:"),
            Line::from(""),
            Line::from(vec![
                Span::styled("[Enter]", theme::THEME.menu_key),
                Span::raw(" quick \u{2014} fast, no pack footers"),
            ]),
            Line::from(vec![
                Span::styled("[f]", theme::THEME.menu_key),
                Span::raw(" full \u{2014} parse every pack footer"),
            ]),
            Line::from(""),
            Line::from(vec![
                Span::styled("[Esc]", theme::THEME.menu_key),
                Span::raw(" back"),
            ]),
        ];
        Dialog::with_text("Repository Stats", theme::THEME.border, Text::from(text))
            .render(frame.area(), frame);
    }

    fn render_body(&mut self, frame: &mut Frame, area: Rect) {
        let title = self.title();
        let block = theme::block(&title);

        if self.base_loading {
            let ch = self.spinner.tick();
            let text = Text::from(vec![
                Line::from(""),
                Line::from(Span::styled(
                    format!("  {ch} Collecting statistics\u{2026}"),
                    theme::THEME.info,
                )),
            ]);
            frame.render_widget(Paragraph::new(text).block(block), area);
            return;
        }

        if let Some(err) = &self.error {
            let text = Text::from(vec![
                Line::from(""),
                Line::from(Span::styled(format!("  {err}"), theme::THEME.error)),
            ]);
            frame.render_widget(Paragraph::new(text).block(block), area);
            return;
        }

        let mut lines = self.lines.clone();
        if self.footers_loading {
            let ch = self.spinner.tick();
            lines.push(Line::from(""));
            lines.push(Line::from(Span::styled(
                format!("  {ch} Parsing pack footers\u{2026}"),
                theme::THEME.footer,
            )));
        }

        // The block consumes one column and one row on each side.
        self.scroll.page_size = area.height.saturating_sub(2) as usize;
        self.scroll.max_offset = lines.len().saturating_sub(self.scroll.page_size);
        self.scroll.offset = self.scroll.offset.min(self.scroll.max_offset);

        let paragraph = Paragraph::new(Text::from(lines.clone()))
            .block(block)
            .scroll((self.scroll.offset as u16, 0));
        frame.render_widget(paragraph, area);
        theme::render_scrollbar(frame, area, lines.len(), self.scroll.offset);
    }
}

#[async_trait]
impl Screen for StatsScreen {
    fn render(&mut self, frame: &mut Frame) {
        self.poll();

        if self.choosing {
            self.render_choose(frame);
            return;
        }

        let inner = frame.area().inner(theme::CONTENT_MARGIN);
        let footer = theme::key_hint_lines(&self.help_hints(), inner.width);
        let chunks = Layout::default()
            .direction(Direction::Vertical)
            .constraints([Constraint::Min(3), Constraint::Length(footer.len() as u16)])
            .split(inner);

        self.render_body(frame, chunks[0]);
        frame.render_widget(Paragraph::new(Text::from(footer)), chunks[1]);
    }

    async fn handle_key(&mut self, key: KeyEvent) -> Option<Transition> {
        if self.choosing {
            return match key.code {
                KeyCode::Enter => {
                    self.choosing = false;
                    self.full = false;
                    self.start_base().await;
                    None
                }
                KeyCode::Char('f') => {
                    self.choosing = false;
                    self.full = true;
                    self.start_base().await;
                    None
                }
                KeyCode::Esc => Some(Transition::Pop),
                KeyCode::Char('q') => Some(Transition::Quit),
                _ => None,
            };
        }

        if self.scroll.handle_key(key.code) {
            return None;
        }

        match key.code {
            KeyCode::Esc => Some(Transition::Pop),
            KeyCode::Char('q') => Some(Transition::Quit),
            KeyCode::Char('f') => {
                self.toggle_full().await;
                None
            }
            KeyCode::Char('r') => {
                self.start_base().await;
                None
            }
            _ => None,
        }
    }

    fn help_hints(&self) -> Vec<(&'static str, &'static str)> {
        if self.choosing {
            return vec![("Enter", "quick"), ("f", "full"), ("Esc", "back")];
        }
        let mut hints = Vec::new();
        if !self.base_loading && self.error.is_none() {
            hints.push(("\u{2191}\u{2193}", "scroll"));
        }
        hints.push(("r", "refresh"));
        hints.push(("f", if self.full { "quick" } else { "full" }));
        hints.push(("Esc", "back"));
        hints.push(("q", "back"));
        hints
    }

    impl_attach_toasts!(toasts);

    fn request_shutdown(&mut self) {
        if let Some(task) = &self.base_task {
            task.cancel();
        }
        if let Some(task) = &self.footers_task {
            task.cancel();
        }
    }

    async fn shutdown(&mut self) {
        if let Some(task) = &mut self.base_task {
            task.shutdown().await;
        }
        if let Some(task) = &mut self.footers_task {
            task.shutdown().await;
        }
    }
}

// ---------------------------------------------------------------------------
// Report rendering
// ---------------------------------------------------------------------------

fn build_lines(report: &StatsReport) -> Vec<Line<'static>> {
    let size = |bytes| format_size_binary(bytes, 3);
    let mut lines = vec![
        section("Packs"),
        kv(
            "Pack files",
            count_and_size(report.packs_count, "pack", "packs", report.packs_bytes),
        ),
    ];
    if report.packs_ecc_count > 0 {
        lines.push(kv(
            "ECC sidecars",
            files(report.packs_ecc_count, report.packs_ecc_bytes),
        ));
    }
    if report.leftover_count > 0 {
        lines.push(kv(
            "Leftover files",
            files(report.leftover_count, report.leftover_bytes),
        ));
    }
    if let Some(f) = &report.footers {
        lines.extend([
            kv("Footer blobs", count(f.blobs, "blob", "blobs")),
            kv(
                "Footer raw / encoded",
                raw_over_encoded(f.raw_bytes, f.encoded_bytes),
            ),
            kv_styled(
                "Footer dangling blobs",
                count(f.dangling, "blob", "blobs"),
                if f.dangling > 0 {
                    theme::THEME.warning
                } else {
                    theme::THEME.stat_value
                },
            ),
        ]);
        if f.duplicate_blobs > 0 {
            lines.push(kv_styled(
                "Footer duplicate blobs",
                count(f.duplicate_blobs, "blob", "blobs"),
                theme::THEME.error,
            ));
        }
    }
    lines.extend([
        Line::from(""),
        section("Index"),
        kv("Index files", files(report.index_count, report.index_bytes)),
    ]);
    if report.index_ecc_count > 0 {
        lines.push(kv(
            "ECC sidecars",
            files(report.index_ecc_count, report.index_ecc_bytes),
        ));
    }
    lines.extend([
        kv(
            "Indexed blobs",
            count(report.indexed_blobs, "blob", "blobs"),
        ),
        kv(
            "Raw / encoded",
            raw_over_encoded(report.indexed_raw_bytes, report.indexed_encoded_bytes),
        ),
        Line::from(""),
        section("Snapshots"),
        kv(
            "Snapshots",
            count_and_size(
                report.snapshots_count,
                "snapshot",
                "snapshots",
                report.snapshots_bytes,
            ),
        ),
    ]);
    if report.snapshots_ecc_count > 0 {
        lines.push(kv(
            "ECC sidecars",
            files(report.snapshots_ecc_count, report.snapshots_ecc_bytes),
        ));
    }
    lines.extend([
        kv(
            "Referenced blobs",
            format!(
                "{} (data: {}, tree: {})",
                report.referenced_blobs, report.referenced_data_blobs, report.referenced_tree_blobs
            ),
        ),
        kv(
            "Raw / encoded",
            raw_over_encoded(report.referenced_raw_bytes, report.referenced_encoded_bytes),
        ),
        kv(
            "Data (raw / encoded)",
            raw_over_encoded(
                report.referenced_raw_bytes_data,
                report.referenced_encoded_bytes_data,
            ),
        ),
        kv(
            "Tree (raw / encoded)",
            raw_over_encoded(
                report.referenced_raw_bytes_tree,
                report.referenced_encoded_bytes_tree,
            ),
        ),
        kv(
            "Compression ratio",
            format!(
                "{:.2}x (data: {:.2}x, tree: {:.2}x)",
                report.ratio_total, report.ratio_data, report.ratio_tree
            ),
        ),
        kv("Restorable size", size(report.total_restorable_bytes)),
    ]);
    if report.unreferenced_blobs > 0 {
        lines.push(kv_styled(
            "Unreferenced blobs",
            format!(
                "{} ({} reclaimable)",
                count(report.unreferenced_blobs, "blob", "blobs"),
                size(report.unreferenced_encoded_bytes)
            ),
            theme::THEME.warning,
        ));
    }
    lines.extend([
        Line::from(""),
        section("Keys"),
        kv(
            "Key files",
            count_and_size(report.keys_count, "key", "keys", report.keys_bytes),
        ),
        Line::from(""),
        section("Repository"),
        kv("Manifest", size(report.manifest_bytes)),
        kv("Total size", size(report.total_repo_bytes)),
    ]);
    lines
}

fn count_and_size(items: usize, singular: &str, plural: &str, bytes: u64) -> String {
    format!(
        "{} ({})",
        count(items, singular, plural),
        format_size_binary(bytes, 3)
    )
}

fn raw_over_encoded(raw: u64, encoded: u64) -> String {
    format!(
        "{} / {}",
        format_size_binary(raw, 3),
        format_size_binary(encoded, 3)
    )
}

fn files(items: usize, bytes: u64) -> String {
    count_and_size(items, "file", "files", bytes)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::error::{MapacheError, Result};
    use crate::{
        backend::{StorageBackend, mock::MockBackend},
        commands::cmd_snapshot::{self, SnapshotRunOptions},
        common::defaults::TEST_REPO_CONFIG,
        repository::repo::{Auth, THIS_REPOSITORY_VERSION},
    };
    use zeroize::Zeroizing;

    #[test]
    fn report_rows_preserve_sections_and_problem_styles() {
        let report = StatsReport {
            footers: Some(FooterReport {
                dangling: 1,
                duplicate_blobs: 1,
                ..Default::default()
            }),
            ..Default::default()
        };
        let lines = build_lines(&report);
        let headers: Vec<_> = lines
            .iter()
            .map(ToString::to_string)
            .filter(|line| {
                ["Packs", "Index", "Snapshots", "Keys", "Repository"].contains(&line.as_str())
            })
            .collect();
        assert_eq!(
            headers,
            vec!["Packs", "Index", "Snapshots", "Keys", "Repository"]
        );
        let dangling = lines
            .iter()
            .find(|line| line.to_string().contains("Footer dangling blobs"))
            .unwrap();
        let duplicates = lines
            .iter()
            .find(|line| line.to_string().contains("Footer duplicate blobs"))
            .unwrap();
        assert_eq!(dangling.spans.last().unwrap().style, theme::THEME.warning);
        assert_eq!(duplicates.spans.last().unwrap().style, theme::THEME.error);
        assert!(
            !lines
                .iter()
                .any(|line| line.to_string().contains("ECC sidecars"))
        );
    }

    /// Opens a repository, writes one snapshot through a second instance, and
    /// returns the reader so a caller can observe the writer's index.
    async fn repo_with_one_snapshot() -> Result<Arc<Repository>> {
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
        let (reader, _) =
            Repository::try_open_unlocked(&auth, None, backend.clone(), TEST_REPO_CONFIG).await?;
        reader.reload_master_index().await?;
        let (writer, _) =
            Repository::try_open_unlocked(&auth, None, backend, TEST_REPO_CONFIG).await?;
        let source = tempfile::tempdir()?;
        std::fs::write(source.path().join("file.txt"), "new repository data")?;
        let args = cmd_snapshot::CmdArgs {
            paths: vec![source.path().to_path_buf()],
            as_root: Some(true),
            no_scan: Some(true),
            num_readers: Some(1),
            num_packers: Some(1),
            ..Default::default()
        };
        cmd_snapshot::run_with_repo(
            writer,
            None,
            SnapshotRunOptions::from(&args),
            Arc::new(|_| {}),
            None,
            None,
        )
        .await
        .map_err(|error| MapacheError::Internal(error.to_string()))?;
        Ok(reader)
    }

    #[tokio::test]
    async fn refresh_loads_index_written_by_another_repository_instance() -> Result<()> {
        let mut screen = StatsScreen::new(repo_with_one_snapshot().await?);
        screen.start_base().await;
        screen.base_task.as_mut().unwrap().wait().await;
        screen.poll();
        let report = screen.report.as_ref().unwrap();
        assert_eq!(report.snapshots_count, 1);
        assert!(report.indexed_blobs > 0);
        assert!(screen.error.is_none());
        Ok(())
    }

    #[tokio::test]
    async fn base_report_hands_pack_ids_to_the_footer_pass() -> Result<()> {
        // The quick scan must keep the pack IDs so the `f` toggle can parse
        // footers without listing the objects directory again.
        let repo = repo_with_one_snapshot().await?;
        let mut screen = StatsScreen::new(repo.clone());
        screen.start_base().await;
        screen.base_task.as_mut().unwrap().wait().await;
        screen.poll();

        let pack_ids = screen.report.as_ref().unwrap().pack_ids.clone();
        assert!(
            !pack_ids.is_empty(),
            "a quick scan must still collect the pack IDs"
        );

        let footers = crate::commands::cmd_stats::collect_footers(
            repo.clone(),
            repo.secure_storage(),
            repo.backend(),
            &pack_ids,
            Arc::new(AtomicBool::new(false)),
            &|_| {},
        )
        .await
        .map_err(|error| MapacheError::Internal(error.to_string()))?;
        assert!(footers.blobs > 0, "the stored pack IDs must be parseable");
        Ok(())
    }
}
