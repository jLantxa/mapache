// [backup] is an incremental backup tool
// Copyright (C) 2025  Javier Lancha Vázquez <javier.lancha@gmail.com>
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program.  If not, see <https://www.gnu.org/licenses/>.

// Helper function to calculate the "visible" length of a string, ignoring ANSI escape codes.
fn visible_string_len(s: &str) -> usize {
    let mut len = 0;
    let mut in_ansi_escape = false;

    for c in s.chars() {
        if in_ansi_escape {
            // Check for the end of an ANSI sequence (typically 'm')
            if c == 'm' {
                in_ansi_escape = false;
            }
            // Skip other characters within the escape sequence
            continue;
        } else if c == '\x1b' {
            // Start of an ANSI escape sequence (ESC character)
            in_ansi_escape = true;
            continue;
        } else {
            len += 1;
        }
    }
    len
}

#[derive(Debug, PartialEq, Clone, Copy)]
pub enum Alignment {
    Left,
    Center,
    Right,
}

/// A helper struct to contruct a Table
pub struct Table {
    headers: Vec<String>,
    data: Vec<Vec<String>>,
    column_widths: Vec<usize>,
    column_alignments: Vec<Alignment>,
    padding: usize,
}

impl Table {
    /// Creates a Table with default alignments
    pub fn new() -> Self {
        Self {
            headers: Vec::new(),
            data: Vec::new(),
            column_widths: Vec::new(),
            column_alignments: Vec::new(),
            padding: 1,
        }
    }

    /// Creates a Table with alignments.
    pub fn new_with_alignments(alignments: Vec<Alignment>) -> Self {
        let num_alignments = alignments.len();
        Self {
            headers: Vec::new(),
            data: Vec::new(),
            column_widths: vec![0; num_alignments],
            column_alignments: alignments,
            padding: 1,
        }
    }

    pub fn set_headers(&mut self, headers: Vec<String>) {
        self.headers = headers;
        self.calculate_column_widths();

        self.column_alignments
            .resize(self.headers.len(), Alignment::Left);
    }

    pub fn add_row(&mut self, row: Vec<String>) {
        let row_len = row.len();
        self.data.push(row);

        self.calculate_column_widths();
        let current_max_cols = self.column_alignments.len().max(row_len);
        self.column_alignments
            .resize(current_max_cols, Alignment::Left);
    }

    pub fn set_padding(&mut self, padding: usize) {
        self.padding = padding;
        self.calculate_column_widths();
    }

    pub fn set_column_alignments(&mut self, alignments: Vec<Alignment>) {
        self.column_alignments = alignments;
        self.calculate_column_widths();
    }

    pub fn set_column_alignment(&mut self, column_index: usize, alignment: Alignment) {
        if column_index >= self.column_alignments.len() {
            self.column_alignments
                .resize(column_index + 1, Alignment::Left);
        }
        self.column_alignments[column_index] = alignment;
        self.calculate_column_widths();
    }

    fn calculate_column_widths(&mut self) {
        let num_columns = if !self.headers.is_empty() {
            self.headers.len()
        } else if !self.data.is_empty() {
            self.data.iter().map(|row| row.len()).max().unwrap_or(0)
        } else {
            self.column_alignments.len()
        };

        if num_columns == 0 {
            self.column_widths.clear();
            self.column_alignments.clear();
            return;
        }

        self.column_widths.clear();
        self.column_widths.resize(num_columns, 0);

        self.column_alignments.resize(num_columns, Alignment::Left);

        for (i, header) in self.headers.iter().enumerate() {
            if i < num_columns {
                self.column_widths[i] = self.column_widths[i].max(visible_string_len(header));
            }
        }

        for row in &self.data {
            for (i, cell) in row.iter().enumerate() {
                if i < num_columns {
                    self.column_widths[i] = self.column_widths[i].max(visible_string_len(cell));
                }
            }
        }
    }

    /// Renders the table to a String.
    pub fn render(&self) -> String {
        if self.column_widths.is_empty() {
            return String::from("No data to display in table.");
        }

        let mut output = String::new();

        if !self.headers.is_empty() || !self.data.is_empty() {
            output.push_str(&self.draw_horizontal_line());
        }

        // Headers
        if !self.headers.is_empty() {
            for (i, header) in self.headers.iter().enumerate() {
                let column_display_width = self.column_widths[i];
                let cell_total_width = column_display_width + self.padding * 2;
                let visible_len = visible_string_len(header);

                let (spaces_before, spaces_after);

                let alignment = self
                    .column_alignments
                    .get(i)
                    .copied()
                    .unwrap_or(Alignment::Center);

                match alignment {
                    Alignment::Left => {
                        spaces_before = self.padding;
                        spaces_after = cell_total_width - visible_len - spaces_before;
                    }
                    Alignment::Right => {
                        spaces_after = self.padding;
                        spaces_before = cell_total_width - visible_len - spaces_after;
                    }
                    Alignment::Center => {
                        let total_extra_spaces = cell_total_width - visible_len;
                        spaces_before = total_extra_spaces / 2;
                        spaces_after = total_extra_spaces - spaces_before;
                    }
                }

                output.push_str(&" ".repeat(spaces_before));
                output.push_str(header);
                output.push_str(&" ".repeat(spaces_after));

                if i < self.headers.len() - 1 {
                    output.push_str("  ");
                }
            }
            output.push('\n');
            output.push_str(&self.draw_horizontal_line());
        }

        // Data rows
        for row in &self.data {
            for (i, cell) in row.iter().enumerate() {
                let column_display_width = if i < self.column_widths.len() {
                    self.column_widths[i]
                } else {
                    visible_string_len(cell)
                };
                let cell_total_width = column_display_width + self.padding * 2;
                let visible_len = visible_string_len(cell);

                let (spaces_before, spaces_after);

                let alignment = self
                    .column_alignments
                    .get(i)
                    .copied()
                    .unwrap_or(Alignment::Left);

                match alignment {
                    Alignment::Left => {
                        spaces_before = self.padding;
                        spaces_after = cell_total_width - visible_len - spaces_before;
                    }
                    Alignment::Right => {
                        spaces_after = self.padding;
                        spaces_before = cell_total_width - visible_len - spaces_after;
                    }
                    Alignment::Center => {
                        let total_extra_spaces = cell_total_width - visible_len;
                        spaces_before = total_extra_spaces / 2;
                        spaces_after = total_extra_spaces - spaces_before;
                    }
                }

                output.push_str(&" ".repeat(spaces_before));
                output.push_str(cell);
                output.push_str(&" ".repeat(spaces_after));

                if i < row.len() - 1 {
                    output.push_str("  ");
                }
            }
            output.push('\n');
        }

        if !self.data.is_empty() || !self.headers.is_empty() {
            output.push_str(&self.draw_horizontal_line());
        }

        output
    }

    /// Prints the Table.
    pub fn print(&self) {
        println!("{}", self.render());
    }

    fn draw_horizontal_line(&self) -> String {
        let mut line = String::new();
        let total_content_width: usize = self
            .column_widths
            .iter()
            .map(|&w| w + self.padding * 2)
            .sum::<usize>();

        let total_line_width =
            total_content_width + (self.column_widths.len().saturating_sub(1) * 2);

        line.push_str(&"-".repeat(total_line_width));
        line.push('\n');
        line
    }
}
