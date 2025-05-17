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

pub mod url;

use std::path::PathBuf;

use blake3::Hasher;

pub type Hash = String;

/// Calculates the 256-bit hash of a byte array
pub fn calculate_hash<T: AsRef<[u8]>>(data: T) -> Hash {
    let mut hasher = Hasher::new();
    hasher.update(data.as_ref());
    hasher.finalize().to_string()
}

#[allow(non_upper_case_globals)]
pub mod size {
    pub const KiB: u64 = 1024;
    pub const MiB: u64 = KiB * 1024;
    pub const GiB: u64 = MiB * 1024;
    pub const TiB: u64 = GiB * 1024;
}

#[allow(non_upper_case_globals)]
pub fn format_size(bytes: u64) -> String {
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

/// Calculates the longest common prefix for a set of paths
pub fn calculate_lcp(paths: &[PathBuf]) -> PathBuf {
    if paths.is_empty() {
        return PathBuf::new();
    }
    if paths.len() == 1 {
        return paths[0].clone();
    }

    let first_path = &paths[0];
    let mut common_prefix = PathBuf::new();

    for (i, first_comp) in first_path.components().enumerate() {
        let is_common = paths[1..].iter().all(|other_path| {
            other_path
                .components()
                .nth(i)
                .map_or(false, |other_comp| first_comp.eq(&other_comp))
        });

        if is_common {
            common_prefix.push(first_comp);
        } else {
            break;
        }
    }

    common_prefix
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

    #[test]
    fn test_calculate_lcp() {
        let paths: Vec<PathBuf> = vec![];
        assert_eq!(calculate_lcp(&paths), PathBuf::new());

        let paths = vec![PathBuf::from("/home/user/docs")];
        assert_eq!(calculate_lcp(&paths), PathBuf::from("/home/user/docs"));

        let paths = vec![
            PathBuf::from("/home/user/a"),
            PathBuf::from("/home/user/b/file.txt"),
            PathBuf::from("/home/user/c"),
        ];
        assert_eq!(calculate_lcp(&paths), PathBuf::from("/home/user"));

        let paths = vec![
            PathBuf::from("/home/user/docs"),
            PathBuf::from("/etc"),
            PathBuf::from("/var/log"),
        ];
        assert_eq!(calculate_lcp(&paths), PathBuf::from("/"));

        let paths = vec![
            PathBuf::from("a/b/c"),
            PathBuf::from("a/b/d"),
            PathBuf::from("a/b"),
        ];
        assert_eq!(calculate_lcp(&paths), PathBuf::from("a/b"));

        let paths = vec![PathBuf::from("a/b"), PathBuf::from("x/y")];
        assert_eq!(calculate_lcp(&paths), PathBuf::new()); // LCP of a/b and x/y is ""

        let paths = vec![PathBuf::from("/home/user/a"), PathBuf::from("a")];
        assert_eq!(calculate_lcp(&paths), PathBuf::new());
    }
}
