/*
 * [backup] is an incremental backup tool
 * Copyright (C) 2025  Javier Lancha Vázquez <javier.lancha@gmail.com>
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
*/

use blake3::Hasher;

pub type Hash = String;

/// Calculates the 256-bit hash of a byte array
fn calculate_hash<T: AsRef<[u8]>>(data: T) -> Hash {
    let mut hasher = Hasher::new();
    hasher.update(data.as_ref());
    let hash = hasher.finalize();
    format!("{}", hash)
}

pub trait Hashable {
    fn hash(&self) -> Hash;
}
