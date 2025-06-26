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

use std::{sync::Arc, thread::JoinHandle};

use anyhow::{Context, Result};
use blake3::Hasher;
use crossbeam_channel::Sender;

use crate::global::{ID, ObjectType};

/// Describes a single blob's location and size within a packed file.
/// This metadata is crucial for retrieving individual blobs from a pack.
#[derive(Debug, Clone)]
pub struct PackedBlobDescriptor {
    pub id: ID,
    pub blob_type: ObjectType,
    pub offset: u64,
    pub length: u64,
}

/// A tuple representing the flushed contents of a `Packer`:
/// (packed data, list of blob descriptors, pack ID).
pub type FlushedPack = (Vec<u8>, Vec<PackedBlobDescriptor>, ID);

/// The `Packer` is an in-memory buffer designed to efficiently accumulate
/// multiple blob objects and their raw data. When `flush` is called, it
/// releases the combined data and a list of descriptors, ready to be written
/// as a single pack file.
///
/// This design helps minimize memory reallocations by consolidating all blob
/// data into a single `Vec<u8>` and tracking individual blob locations.
#[derive(Debug)]
pub struct Packer {
    data: Vec<u8>,
    blob_descriptors: Vec<PackedBlobDescriptor>,
    hasher: Hasher,
}

impl Packer {
    /// Creates a new, empty `Packer` with default capacities.
    ///
    /// For better performance, consider using `Packer::with_capacity` if you have
    /// an estimate of the total data size and number of blobs you intend to add.
    pub fn new() -> Self {
        Self {
            data: Vec::new(),
            blob_descriptors: Vec::new(),
            hasher: Hasher::new(),
        }
    }

    /// Creates a new `Packer` with pre-allocated capacity for its internal data buffer
    /// and blob descriptor list.
    ///
    /// Using this constructor can improve performance by reducing the number of memory
    /// reallocations if you have a good estimate of the total data size and number of
    /// blobs you intend to add. For fix pack sizes, using this can significantly reduce
    /// memory churn.
    pub fn with_capacity(data_capacity: usize, blobs_capacity: usize) -> Self {
        Self {
            data: Vec::with_capacity(data_capacity),
            blob_descriptors: Vec::with_capacity(blobs_capacity),
            hasher: Hasher::new(),
        }
    }

    /// Returns the current total byte size of all raw data accumulated in the packer.
    #[inline]
    pub fn size(&self) -> u64 {
        self.data.len() as u64
    }

    /// Returns `true` if the packer contains no blob data and no descriptors.
    pub fn is_empty(&self) -> bool {
        self.data.is_empty() && self.blob_descriptors.is_empty()
    }

    /// Returns the number of individual blob objects currently stored in the packer.
    #[inline]
    pub fn num_objects(&self) -> usize {
        self.blob_descriptors.len()
    }

    /// Appends a new blob's data to the packer and records its corresponding descriptor.
    ///
    /// The `blob_data` `Vec<u8>` is efficiently moved into the packer's internal
    /// buffer using `Vec::append`, avoiding a costly copy. After this call, `blob_data`
    /// will be empty.
    pub fn add_blob(&mut self, id: ID, blob_type: ObjectType, mut blob_data: Vec<u8>) {
        let offset = self.data.len() as u64;
        let length = blob_data.len() as u64;

        if !blob_data.is_empty() {
            self.hasher.update(&blob_data);
        }

        self.data.append(&mut blob_data);

        // Record the descriptor for the newly added blob
        self.blob_descriptors.push(PackedBlobDescriptor {
            id,
            blob_type,
            offset,
            length,
        });
    }

    /// Flushes the contents of the packer, returning the accumulated raw data
    /// and the list of `PackedBlobDescriptor`s.
    ///
    /// After calling `flush`, the `Packer` instance will be reset to an empty state,
    /// ready to accumulate new blobs. Ownership of the `Vec<u8>` and `Vec<PackedBlobDescriptor>`
    /// is transferred to the caller, making this an efficient way to extract the packed content.
    /// The internal vectors retain their allocated capacity for future use.
    pub fn flush(&mut self) -> FlushedPack {
        let hash = self.hasher.finalize();
        self.hasher.reset();

        // Take ownership of the vectors, effectively swapping them with new empty ones.
        let data = std::mem::take(&mut self.data);
        let descriptors = std::mem::take(&mut self.blob_descriptors);

        // Reset the internal vectors to be empty, but crucially,
        // they *retain their allocated capacity* for the next pack.
        self.data.clear();
        self.blob_descriptors.clear();

        (data, descriptors, ID::from_bytes(hash.into()))
    }
}

pub type QueueFn = Arc<dyn Fn(Vec<u8>, ID) + Send + Sync + 'static>;

pub struct PackSaver {
    tx: Sender<(Vec<u8>, ID)>,
    join_handle: JoinHandle<()>,
}

impl PackSaver {
    pub fn new(concurrency: usize, queue_fn: QueueFn) -> Self {
        let (tx, rx) = crossbeam_channel::bounded(concurrency);

        let worker_queue_fn = Arc::clone(&queue_fn);

        let join_handle = std::thread::spawn(move || {
            let pool = rayon::ThreadPoolBuilder::new()
                .num_threads(concurrency)
                .build()
                .expect("Failed to build thread pool");

            while let Ok((data, id)) = rx.recv() {
                pool.scope(|s| {
                    s.spawn(|_| {
                        worker_queue_fn(data, id);
                    });
                });
            }
        });

        PackSaver { tx, join_handle }
    }

    pub fn save_pack(&self, packer_data: Vec<u8>) -> Result<ID> {
        let pack_id = ID::from_content(&packer_data);

        self.tx
            .send((packer_data, pack_id.clone()))
            .with_context(|| "Failed to send pack data to PackSaver channel")?;

        Ok(pack_id)
    }

    pub fn finish(self) {
        drop(self.tx);
        self.join_handle
            .join()
            .expect("Packer saver thread panicked");
    }
}
