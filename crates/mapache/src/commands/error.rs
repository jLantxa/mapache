use std::fmt;

/// Trait that any command-specific error enum must implement to be used as an exit code.
pub trait ToExitCode: std::error::Error {
    fn to_exit_code(&self) -> i32;
}

/// Mechanical wrapper that carries a command error's exit code for `parse_and_run()`.
#[derive(Debug)]
pub struct CmdError {
    exit_code: i32,
    source: Box<dyn std::error::Error + Send + Sync + 'static>,
}

impl CmdError {
    pub fn new<E>(e: E) -> Self
    where
        E: ToExitCode + Send + Sync + 'static,
    {
        let exit_code = e.to_exit_code();
        Self {
            exit_code,
            source: Box::new(e),
        }
    }

    pub fn exit_code(&self) -> i32 {
        self.exit_code
    }
}

impl fmt::Display for CmdError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.source, f)
    }
}

impl std::error::Error for CmdError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&*self.source)
    }
}
