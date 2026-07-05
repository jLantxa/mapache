use thiserror::Error;

use crate::common::ID;

pub type Result<T> = std::result::Result<T, MapacheError>;

#[derive(Debug, Error)]
pub enum MapacheError {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("backend error: {0}")]
    Backend(String),

    #[error("authentication error: {0}")]
    Auth(String),

    #[error("cryptographic error: {0}")]
    Crypto(String),

    #[error("compression error: {0}")]
    Compression(String),

    #[error("chunking error: {0}")]
    Chunking(String),

    #[error("snapshot not found: {0}")]
    SnapshotNotFound(String),

    #[error("not found in index: {0}")]
    NotInIndex(ID),

    #[error("not found: {0}")]
    NotFound(String),

    #[error("repository already exists: {0}")]
    RepoAlreadyExists(String),

    #[error("repository error: {0}")]
    Repo(String),

    #[error("hook error: {0}")]
    Hook(String),

    #[error("resource locked: {0}")]
    Locked(String),

    #[error("lock expired: {0}")]
    LockExpired(String),

    #[error("data integrity error: {0}")]
    Integrity(String),

    #[error("configuration error: {0}")]
    Config(String),

    #[error("format error: {0}")]
    Format(String),

    #[error("FUSE error: {0}")]
    Fuse(String),

    #[error("serialization error: {0}")]
    Serialization(#[from] serde_json::Error),

    #[error("operation interrupted")]
    Interrupted,

    #[error("{0}")]
    Internal(String),
}
