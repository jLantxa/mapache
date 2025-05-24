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

use crate::utils::size;

pub const SHORT_ID_LENGTH: usize = 4;

// Index
/// This is a approximate number. Whole packs are stored in the same index
/// file, so this is a minimum.
pub const BLOBS_PER_INDEX_FILE: u32 = 65536;

// Packing
/// Minimum pack size before flushing to the backend.
pub const MAX_PACK_SIZE: u64 = 16 * size::MiB;

// Chunking
/// Minimum chunk size
pub const MIN_CHUNK_SIZE: u32 = 512 * size::KiB as u32;
/// Average chunk size
pub const AVG_CHUNK_SIZE: u32 = 1 * size::MiB as u32;
/// Maximum chunk size
pub const MAX_CHUNK_SIZE: u32 = 8 * size::MiB as u32;
