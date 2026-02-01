use parking_lot::Mutex;
use serde::Serialize;
use std::{
    io::{BufWriter, Write},
    sync::LazyLock,
};

// A global instance to power the static helper
static GLOBAL_JSON_REPORTER: LazyLock<JsonReporter> = LazyLock::new(|| JsonReporter::new(true));

pub(crate) struct JsonReporter {
    auto_flush: bool,
    writer: Mutex<BufWriter<std::io::Stdout>>,
}

impl JsonReporter {
    pub(crate) fn new(auto_flush: bool) -> Self {
        Self {
            auto_flush,
            writer: Mutex::new(BufWriter::with_capacity(8192, std::io::stdout())),
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
            debug_assert!(false, "JSON Reporter error: {e}");
        }
    }

    pub(crate) fn flush(&self) {
        let _ = self.writer.lock().flush();
    }
}

pub(crate) fn emit_static<T: Serialize>(msg_type: &str, msg: &T) {
    GLOBAL_JSON_REPORTER.emit(msg_type, msg);
}
