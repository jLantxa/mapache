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

use crate::backup::ObjectId;

#[derive(Debug)]
pub struct PackedBlobDescriptor {
    pub id: ObjectId,
    pub offset: u64,
    pub length: u64,
}

/// The packer is an object buffer that accumulates objects to be flushed together
/// in a single pack file.
#[derive(Debug)]
pub struct Packer {
    data: Vec<u8>,
    blob_descriptors: Vec<PackedBlobDescriptor>,
}

impl Packer {
    pub fn new() -> Self {
        Self {
            data: Vec::new(),
            blob_descriptors: Vec::new(),
        }
    }

    #[inline]
    pub fn size(&self) -> u64 {
        self.data.len() as u64
    }

    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    #[inline]
    pub fn num_objects(&self) -> usize {
        self.blob_descriptors.len()
    }

    /// Append data to the packer
    pub fn add_blob(&mut self, id: &ObjectId, mut blob_data: Vec<u8>) {
        let offset = self.data.len() as u64;
        let length = blob_data.len() as u64;
        self.data.append(&mut blob_data);

        self.blob_descriptors.push(PackedBlobDescriptor {
            id: id.clone(),
            offset,
            length,
        });
    }

    /// Returns the contents of the packer with ownership, replacing the data vector
    /// with a new, empty vector.
    pub fn flush(&mut self) -> (Vec<u8>, Vec<PackedBlobDescriptor>) {
        let data = std::mem::take(&mut self.data);
        let descriptors = std::mem::take(&mut self.blob_descriptors);

        (data, descriptors)
    }
}
