use ratatui::widgets::{ListState, TableState};

pub trait StateNavigation {
    fn next(&mut self, len: usize);
    fn previous(&mut self, len: usize);
    fn page_next(&mut self, len: usize, page_size: usize);
    fn page_previous(&mut self, len: usize, page_size: usize);
    fn home(&mut self, len: usize);
    fn end(&mut self, len: usize);
}

impl StateNavigation for TableState {
    fn next(&mut self, len: usize) {
        if len == 0 {
            return;
        }
        let i = match self.selected() {
            Some(i) => {
                if i >= len.saturating_sub(1) {
                    0
                } else {
                    i + 1
                }
            }
            None => 0,
        };
        self.select(Some(i));
    }

    fn previous(&mut self, len: usize) {
        if len == 0 {
            return;
        }
        let i = match self.selected() {
            Some(i) => {
                if i == 0 {
                    len.saturating_sub(1)
                } else {
                    i - 1
                }
            }
            None => 0,
        };
        self.select(Some(i));
    }

    fn page_next(&mut self, len: usize, page_size: usize) {
        if len == 0 {
            return;
        }
        let i = match self.selected() {
            Some(i) => (i + page_size).min(len.saturating_sub(1)),
            None => 0,
        };
        self.select(Some(i));
    }

    fn page_previous(&mut self, len: usize, page_size: usize) {
        if len == 0 {
            return;
        }
        let i = match self.selected() {
            Some(i) => i.saturating_sub(page_size),
            None => 0,
        };
        self.select(Some(i));
    }

    fn home(&mut self, len: usize) {
        if len > 0 {
            self.select(Some(0));
        }
    }

    fn end(&mut self, len: usize) {
        if len > 0 {
            self.select(Some(len.saturating_sub(1)));
        }
    }
}

impl StateNavigation for ListState {
    fn next(&mut self, len: usize) {
        if len == 0 {
            return;
        }
        let i = match self.selected() {
            Some(i) => {
                if i >= len.saturating_sub(1) {
                    0
                } else {
                    i + 1
                }
            }
            None => 0,
        };
        self.select(Some(i));
    }

    fn previous(&mut self, len: usize) {
        if len == 0 {
            return;
        }
        let i = match self.selected() {
            Some(i) => {
                if i == 0 {
                    len.saturating_sub(1)
                } else {
                    i - 1
                }
            }
            None => 0,
        };
        self.select(Some(i));
    }

    fn page_next(&mut self, len: usize, page_size: usize) {
        if len == 0 {
            return;
        }
        let i = match self.selected() {
            Some(i) => (i + page_size).min(len.saturating_sub(1)),
            None => 0,
        };
        self.select(Some(i));
    }

    fn page_previous(&mut self, len: usize, page_size: usize) {
        if len == 0 {
            return;
        }
        let i = match self.selected() {
            Some(i) => i.saturating_sub(page_size),
            None => 0,
        };
        self.select(Some(i));
    }

    fn home(&mut self, len: usize) {
        if len > 0 {
            self.select(Some(0));
        }
    }

    fn end(&mut self, len: usize) {
        if len > 0 {
            self.select(Some(len.saturating_sub(1)));
        }
    }
}
