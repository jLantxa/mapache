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

use blake3::Hasher;

pub type Hash = String;

/// Calculates the 256-bit hash of a byte array
pub fn calculate_hash<T: AsRef<[u8]>>(data: T) -> Hash {
    let mut hasher = Hasher::new();
    hasher.update(data.as_ref());
    let hash = hasher.finalize();
    format!("{}", hash)
}

pub trait Hashable {
    fn hash(&self) -> Hash;
}

#[cfg(test)]
mod tests {
    use super::calculate_hash;

    /// Test consistency of the hash function
    #[test]
    fn test_hash_function() {
        let data = br#"
             Lorem ipsum dolor sit amet, consectetur adipisici elit, sed eiusmod tempor incidunt
             ut labore et dolore magna aliqua. Ut enim ad minim veniam, quis nostrud exercitation
             ullamco laboris nisi ut aliquid ex ea commodi consequat. Quis aute iure reprehenderit in
             voluptate velit esse cillum dolore eu fugiat nulla pariatur. Excepteur sint obcaecat
             cupiditat non proident, sunt in culpa qui officia deserunt mollit anim id est laborum.
             "#;
        let hash = calculate_hash(data);

        assert_eq!(
            hash,
            "28ff314ca7c551552d4d2f4be86fd2348749ace0fbda1a051038bdb493c10a4d"
        );
    }
}
