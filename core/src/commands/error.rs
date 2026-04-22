use std::fmt;

/// Trait that any command-specific error enum must implement to be used as an exit code.
pub trait ToExitCode {
    fn to_exit_code(&self) -> i32;
}

/// A structured error that carries a message and an exit code.
/// This can be wrapped in anyhow::Error.
#[derive(Debug)]
pub struct MapacheError {
    pub message: String,
    pub exit_code: i32,
}

impl fmt::Display for MapacheError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.message)
    }
}

impl std::error::Error for MapacheError {}

/// Helper to create a structured error that can be returned from commands.
/// Usage: return Err(fail("Authentication failed", CmdInitError::AuthFail));
pub fn fail<S: Into<String>, E: ToExitCode>(msg: S, code: E) -> anyhow::Error {
    anyhow::Error::new(MapacheError {
        message: msg.into(),
        exit_code: code.to_exit_code(),
    })
}

/// Fallback exit code for errors that are not MapacheError.
pub const GENERIC_ERROR_CODE: i32 = -1;
