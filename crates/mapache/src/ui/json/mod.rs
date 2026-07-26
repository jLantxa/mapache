use std::{
    io::{BufWriter, Write},
    sync::LazyLock,
};

use parking_lot::Mutex;
use serde::Serialize;

pub mod restore;
pub mod snapshot;

// A global instance to power the static helper
static GLOBAL_JSON_REPORTER: LazyLock<JsonReporter> = LazyLock::new(|| JsonReporter::new(true));

pub(crate) struct JsonReporter {
    auto_flush: bool,
    writer: Mutex<Box<dyn Write + Send>>,
}

impl JsonReporter {
    pub(crate) fn new(auto_flush: bool) -> Self {
        Self {
            auto_flush,
            writer: Mutex::new(Box::new(BufWriter::with_capacity(8192, std::io::stdout()))),
        }
    }

    #[cfg(test)]
    pub(crate) fn sink() -> Self {
        Self {
            auto_flush: false,
            writer: Mutex::new(Box::new(std::io::sink())),
        }
    }

    pub(crate) fn emit<T: Serialize>(&self, msg_type: &str, msg: &T) {
        #[derive(Serialize)]
        struct Envelope<'a, T: Serialize> {
            msg_type: &'a str,
            #[serde(flatten)]
            payload: &'a T,
        }

        let mut guard = self.writer.lock();
        let res = (|| -> Result<(), Box<dyn std::error::Error>> {
            serde_json::to_writer(
                &mut *guard,
                &Envelope {
                    msg_type,
                    payload: msg,
                },
            )?;
            guard.write_all(b"\n")?;
            if self.auto_flush {
                guard.flush()?;
            }
            Ok(())
        })();

        if let Err(e) = res {
            tracing::error!(target: "json", "JSON Reporter error: {e}");
        }
    }

    pub(crate) fn flush(&self) {
        let _ = self.writer.lock().flush();
    }
}

pub(crate) fn emit_static<T: Serialize>(msg_type: &str, msg: &T) {
    GLOBAL_JSON_REPORTER.emit(msg_type, msg);
}
