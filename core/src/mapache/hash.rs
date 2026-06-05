use std::io;

use crate::mapache::ID;

/// A wrapper around the underlying hashing implementation.
///
/// This allows us to change the hashing algorithm without affecting the rest
/// of the codebase.
pub(crate) struct Hasher(blake3::Hasher);

impl Hasher {
    /// Creates a new hasher instance.
    pub fn new() -> Self {
        Self(blake3::Hasher::new())
    }

    /// Updates the hasher with the provided data.
    pub fn update(&mut self, data: &[u8]) {
        self.0.update(data);
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
    use std::io::Write;

    use super::*;

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
}
