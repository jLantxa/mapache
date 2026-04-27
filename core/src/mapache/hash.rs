use crate::mapache::{Hash256, ID};
use anyhow::Result;
use std::io;
use std::path::Path;

/// A wrapper around the underlying hashing implementation.
///
/// This allows us to change the hashing algorithm without affecting the rest
/// of the codebase.
pub struct Hasher(blake3::Hasher);

impl Hasher {
    /// Creates a new hasher instance.
    pub fn new() -> Self {
        Self(blake3::Hasher::new())
    }

    /// Updates the hasher with the provided data.
    pub fn update(&mut self, data: &[u8]) {
        self.0.update(data);
    }

    /// Updates the hasher by reading from a reader.
    pub fn update_reader<R: io::Read>(&mut self, mut reader: R) -> io::Result<u64> {
        io::copy(&mut reader, &mut self.0)
    }

    /// Finalizes the hashing process and returns the resulting ID.
    pub fn finalize(self) -> ID {
        ID::from_bytes(self.0.finalize().into())
    }
}

impl Default for Hasher {
    fn default() -> Self {
        Self::new()
    }
}

/// Calculates the hash of the provided data and returns it as an ID.
pub fn hash<T: AsRef<[u8]>>(data: T) -> ID {
    let mut hasher = Hasher::new();
    hasher.update(data.as_ref());
    hasher.finalize()
}

/// Calculates the hash of the provided data and returns it as a raw byte array.
pub fn calculate_raw_hash<T: AsRef<[u8]>>(data: T) -> Hash256 {
    let mut hasher = Hasher::new();
    hasher.update(data.as_ref());
    hasher.finalize().0
}

/// Calculates the hash of a file by reading it.
pub fn calculate_from_path(path: &Path) -> Result<ID> {
    let file = std::fs::File::open(path)?;
    let mut reader = std::io::BufReader::new(file);
    let mut hasher = Hasher::new();
    hasher.update_reader(&mut reader)?;
    Ok(hasher.finalize())
}

impl io::Write for Hasher {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.update(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::NamedTempFile;

    #[test]
    fn test_one_shot_hash() {
        let data = b"mapache backup";
        let id1 = hash(data);
        let id2 = hash(data);
        assert_eq!(id1, id2);
        assert_ne!(id1, hash(b"something else"));
    }

    #[test]
    fn test_incremental_hash() {
        let mut hasher = Hasher::new();
        hasher.update(b"mapache ");
        hasher.update(b"backup");
        let id = hasher.finalize();
        assert_eq!(id, hash(b"mapache backup"));
    }

    #[test]
    fn test_hasher_as_writer() {
        let mut hasher = Hasher::new();
        write!(hasher, "mapache backup").unwrap();
        let id = hasher.finalize();
        assert_eq!(id, hash(b"mapache backup"));
    }

    #[test]
    fn test_hash_from_path() -> Result<()> {
        let mut tmp_file = NamedTempFile::new()?;
        tmp_file.write_all(b"content of the file")?;

        let id = calculate_from_path(tmp_file.path())?;
        assert_eq!(id, hash(b"content of the file"));
        Ok(())
    }

    #[test]
    fn test_raw_hash() {
        let data = b"raw data";
        let raw = calculate_raw_hash(data);
        let id = hash(data);
        assert_eq!(raw, id.0);
    }
}
