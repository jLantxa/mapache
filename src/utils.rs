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

use std::{fs::File, path::Path};

use anyhow::{Context, Result};
use blake3::Hasher;
use rayon::ThreadPoolBuilder;
use serde::{Serialize, de::DeserializeOwned};

pub type Hash = String;

/// Calculates the 256-bit hash of a byte array
pub fn calculate_hash<T: AsRef<[u8]>>(data: T) -> Hash {
    let mut hasher = Hasher::new();
    hasher.update(data.as_ref());
    let hash = hasher.finalize();
    format!("{}", hash)
}

/// Serializes a struct to JSON and save it to a file
/// The output has no special formatting.
pub fn save_json<T: Serialize>(data: &T, path: &Path) -> Result<()> {
    let file = File::create(path)?;
    serde_json::to_writer(file, data)?;
    Ok(())
}

/// Serializes a struct to JSON and save it to a file
/// The output is formatted to be legible.
pub fn save_json_pretty<T: Serialize>(data: &T, path: &Path) -> Result<()> {
    let file = File::create(path)?;
    serde_json::to_writer_pretty(file, data)?;
    Ok(())
}

/// Deserializes a JSON from a file
pub fn load_json<T: DeserializeOwned>(path: &Path) -> Result<T> {
    let file = File::open(path)?;
    let data = serde_json::from_reader(file)?;
    Ok(data)
}

#[allow(non_upper_case_globals)]
pub mod size {
    pub const KiB: usize = 1024;
    pub const MiB: usize = KiB * 1024;
    pub const GiB: usize = MiB * 1024;
    pub const TiB: usize = GiB * 1024;
}

#[allow(non_upper_case_globals)]
pub fn format_size(bytes: usize) -> String {
    if bytes >= size::TiB {
        return format!("{:.2} TiB", (bytes as f64) / (size::TiB as f64));
    } else if bytes >= size::GiB {
        return format!("{:.2} GiB", (bytes as f64) / (size::GiB as f64));
    } else if bytes >= size::MiB {
        return format!("{:.2} MiB", (bytes as f64) / (size::MiB as f64));
    } else if bytes >= size::KiB {
        return format!("{:.2} KiB", (bytes as f64) / (size::KiB as f64));
    } else if bytes != 1 {
        return format!("{} bytes", bytes);
    } else {
        return format!("1 byte");
    }
}

pub fn configure_rayon(num_threads: usize) -> Result<()> {
    ThreadPoolBuilder::new()
        .num_threads(num_threads)
        .build_global()
        .with_context(|| "Failed to configure rayon: {}")
}

#[cfg(test)]
mod tests {
    use super::*;

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

    #[test]
    fn test_format_size() {
        assert_eq!(format_size(1), "1 byte");
        assert_eq!(format_size(324), "324 bytes");
        assert_eq!(format_size(1_205), "1.18 KiB");
        assert_eq!(format_size(124_112), "121.20 KiB");
        assert_eq!(format_size(1_045_024), "1020.53 KiB");
        assert_eq!(format_size(12_995_924), "12.39 MiB");
        assert_eq!(format_size(1_500_000_000), "1.40 GiB");
        assert_eq!(format_size(2_100_000_100_000), "1.91 TiB");
    }
}
