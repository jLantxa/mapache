use serde::Serialize;
use std::io::{self, BufWriter, Write};
use std::sync::Mutex;

#[macro_export]
macro_rules! json_msg {
    ($enabled:expr, $reporter:expr, $type_str:expr, $msg:expr) => {
        if $enabled {
            $reporter.emit($type_str, &$msg);
        }
    };
}

pub(crate) struct JsonReporter {
    auto_flush: bool,
    writer: Mutex<BufWriter<io::Stdout>>,
}

impl JsonReporter {
    pub(crate) fn new(auto_flush: bool) -> Self {
        Self {
            auto_flush,
            writer: Mutex::new(BufWriter::with_capacity(8192, io::stdout())),
        }
    }

    pub(crate) fn emit<T: Serialize>(&self, msg_type: &'static str, msg: &T) {
        #[derive(Serialize)]
        struct Envelope<'a, T: Serialize> {
            msg_type: &'static str,
            #[serde(flatten)]
            payload: &'a T,
        }

        let envelope = Envelope {
            msg_type,
            payload: msg,
        };

        if let Ok(mut guard) = self.writer.lock() {
            let res = (|| -> Result<(), Box<dyn std::error::Error>> {
                serde_json::to_writer(&mut *guard, &envelope).map_err(Box::new)?;
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
    }

    pub(crate) fn emit_static<T: Serialize>(msg_type: &'static str, msg: &T) {
        #[derive(Serialize)]
        struct Envelope<'a, T: Serialize> {
            msg_type: &'static str,
            #[serde(flatten)]
            payload: &'a T,
        }

        let envelope = Envelope {
            msg_type,
            payload: msg,
        };

        match serde_json::to_string(&envelope) {
            Ok(json) => println!("{json}"),
            Err(e) => {
                debug_assert!(false, "JSON Static Reporter error: {e}");
            }
        }
    }

    pub(crate) fn flush(&self) {
        if let Ok(mut guard) = self.writer.lock() {
            let _ = guard.flush();
        }
    }
}

impl Drop for JsonReporter {
    fn drop(&mut self) {
        self.flush();
    }
}
