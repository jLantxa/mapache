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

use anyhow::{Context, Result};
use crossbeam_channel::Sender;
use std::sync::Arc;

use crate::global::ID;

pub type QueueFn = Arc<dyn Fn(Vec<u8>, ID) + Send + Sync + 'static>;

pub(crate) struct PackSaver {
    tx: Sender<(Vec<u8>, ID)>,
}

impl PackSaver {
    pub fn new(concurrency: usize, queue_fn: QueueFn) -> Self {
        let (tx, rx) = crossbeam_channel::bounded(concurrency);

        let worker_queue_fn = Arc::clone(&queue_fn);

        rayon::spawn(move || {
            while let Ok((data, id)) = rx.recv() {
                worker_queue_fn(data, id);
            }
        });

        PackSaver { tx }
    }

    pub fn save_pack(&self, packer_data: Vec<u8>) -> Result<ID> {
        let pack_id = ID::from_content(&packer_data);

        self.tx
            .send((packer_data, pack_id.clone()))
            .with_context(|| "Failed to send pack data to PackSaver channel")?;

        Ok(pack_id)
    }
}
