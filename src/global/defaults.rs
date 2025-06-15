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

// -- Concurrency --
pub const DEFAULT_READ_CONCURRENCY: usize = 4;
pub const DEFAULT_WRITE_CONCURRENCY: usize = 5;

// -- Index --
// These are approximate numbers. Whole packs are stored in the same index
// file and the packs don't contain a fix number of blobs.

// These constants dictate index flushing during the snapshot process.
//
// Approximate total size referenced by an index when flushing. During the snapshot
// process, we flush the index periodically in order to commit packs frequently. If the
// snapshot process is interrupted, the packs referenced by an index are not lost when
// resuming the snapshot. However, flushing often means saving a lot of small indexes
// to file.
// The garbage collector should merge all small indexes and consolidate them
// into bigger index files.
pub const INDEX_FLUSH_REFERENCE_SIZE_HINT: u64 = 4 * size::GiB;
pub const PACKS_PER_FLUSHED_INDEX_FILE: usize =
    (INDEX_FLUSH_REFERENCE_SIZE_HINT / MAX_PACK_SIZE) as usize;

// Number of packs per index file when merging indexes during garbage collection.
pub const INDEX_REFERENCE_SIZE_HINT: u64 = 32 * size::GiB;
pub const PACKS_PER_INDEX_FILE: usize = (INDEX_REFERENCE_SIZE_HINT / MAX_PACK_SIZE) as usize;

// Packing
/// Minimum pack size before flushing to the backend.
pub const MAX_PACK_SIZE: u64 = 16 * size::MiB;

// Chunking
/// Minimum chunk size
pub const MIN_CHUNK_SIZE: u64 = 512 * size::KiB;
/// Average chunk size
pub const AVG_CHUNK_SIZE: u64 = 1 * size::MiB;
/// Maximum chunk size
pub const MAX_CHUNK_SIZE: u64 = 8 * size::MiB;

// Display
pub const SHORT_REPO_ID_LEN: usize = 5;
pub const SHORT_SNAPSHOT_ID_LEN: usize = 4;

pub const DEFAULT_VERBOSITY: u32 = 1;
