use parking_lot::Mutex;
use serde::Serialize;
use std::io::{BufWriter, Write};

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

        let envelope = Envelope {
            msg_type,
            payload: msg,
        };

        let mut guard = self.writer.lock();

        let res = (|| -> Result<(), std::io::Error> {
            serde_json::to_writer(&mut *guard, &envelope)?;
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
    #[derive(Serialize)]
    struct Envelope<'a, T: Serialize> {
        msg_type: &'a str,
        #[serde(flatten)]
        payload: &'a T,
    }

    let envelope = Envelope {
        msg_type,
        payload: msg,
    };
    let stdout = std::io::stdout();
    let mut handle = stdout.lock(); // Lock once for efficiency

    let res = (|| -> Result<(), Box<dyn std::error::Error>> {
        serde_json::to_writer(&mut handle, &envelope)?;
        handle.write_all(b"\n")?;
        Ok(())
    })();

    if let Err(e) = res {
        debug_assert!(false, "JSON Static Reporter error: {e}");
    }
}
