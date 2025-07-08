// mapache is an incremental backup tool
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

/// A simple enum to represent column alignments.
#[derive(Debug, PartialEq, Clone, Copy)]
pub enum Alignment {
    Left,
    Center,
    Right,
}

/// A table row. It can contain cells or be a separator.
enum Row {
    Values(Vec<String>),
    Separator,
}

/// A helper struct to contruct a Table
pub struct Table {
    headers: Vec<String>,
    data: Vec<Row>,
    column_widths: Vec<usize>,
    column_alignments: Vec<Alignment>,
    padding: usize,
}

impl Default for Table {
    fn default() -> Self {
        Self::new()
    }
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

    /// Adds a header to the table.
    pub fn set_headers(&mut self, headers: Vec<String>) {
        self.headers = headers;
        self.calculate_column_widths();

        self.column_alignments
            .resize(self.headers.len(), Alignment::Left);
    }

    /// Adds a row to the table.
    pub fn add_row(&mut self, row: Vec<String>) {
        let row_len = row.len();
        self.data.push(Row::Values(row));

        self.calculate_column_widths();
        let current_max_cols = self.column_alignments.len().max(row_len);
        self.column_alignments
            .resize(current_max_cols, Alignment::Left);
    }

    /// Adds a separator row
    pub fn add_separator(&mut self) {
        self.data.push(Row::Separator);
    }

    /// Sets the padding for all cells in the table.
    pub fn set_padding(&mut self, padding: usize) {
        self.padding = padding;
        self.calculate_column_widths();
    }

    /// Sets the column alignments for all columns.
    pub fn set_column_alignments(&mut self, alignments: Vec<Alignment>) {
        self.column_alignments = alignments;
        self.calculate_column_widths();
    }

    /// Sets the alignment for a specific column.
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
            self.data
                .iter()
                .map(|row| match row {
                    Row::Values(items) => items.len(),
                    Row::Separator => 0,
                })
                .max()
                .unwrap_or(0)
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
            match row {
                Row::Values(items) => {
                    for (i, cell) in items.iter().enumerate() {
                        if i < num_columns {
                            self.column_widths[i] =
                                self.column_widths[i].max(visible_string_len(cell));
                        }
                    }
                }
                Row::Separator => continue,
            }
        }
    }

    /// Renders the table to a String.
    pub fn render(&self) -> String {
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
            match row {
                Row::Separator => {
                    output.push_str(&self.draw_horizontal_line());
                }
                Row::Values(items) => {
                    for (i, cell) in items.iter().enumerate() {
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

                        if i < items.len() - 1 {
                            output.push_str("  ");
                        }
                    }

                    output.push('\n');
                }
            }
        }

        if !self.data.is_empty() || !self.headers.is_empty() {
            output.push_str(&self.draw_horizontal_line());
        }

        output
    }

    /// Draws a horizontal line based on the current column widths and padding.
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_table_new() {
        let table = Table::new();
        assert!(table.headers.is_empty());
        assert!(table.data.is_empty());
        assert!(table.column_widths.is_empty());
        assert!(table.column_alignments.is_empty());
        assert_eq!(table.padding, 1);
    }

    #[test]
    fn test_table_new_with_alignments() {
        let alignments = vec![Alignment::Left, Alignment::Right];
        let table = Table::new_with_alignments(alignments.clone());
        assert!(table.headers.is_empty());
        assert!(table.data.is_empty());
        assert_eq!(table.column_widths.len(), 2);
        assert_eq!(table.column_alignments, alignments);
        assert_eq!(table.padding, 1);
    }

    #[test]
    fn test_set_headers() {
        let mut table = Table::new();
        let headers = vec!["Header1".to_string(), "Header2".to_string()];
        table.set_headers(headers.clone());

        assert_eq!(table.headers, headers);
        assert_eq!(table.column_widths, vec![7, 7]); // "Header1" and "Header2" have length 7
        assert_eq!(
            table.column_alignments,
            vec![Alignment::Left, Alignment::Left]
        );
    }

    #[test]
    fn test_add_row() {
        let mut table = Table::new();
        table.set_headers(vec!["Col1".to_string(), "Col2".to_string()]);
        table.add_row(vec!["Data1".to_string(), "Data2".to_string()]);

        assert_eq!(table.data.len(), 1);
        if let Row::Values(values) = &table.data[0] {
            assert_eq!(values, &vec!["Data1".to_string(), "Data2".to_string()]);
        } else {
            panic!("Expected a Row::Values variant");
        }
        assert_eq!(table.column_widths, vec![5, 5]); // Max length of "Col1", "Data1" and "Col2", "Data2"
        assert_eq!(table.column_alignments.len(), 2);
    }

    #[test]
    fn test_render_empty_table() {
        let table = Table::new();
        assert_eq!(table.render(), "\n".to_string());
    }
}
