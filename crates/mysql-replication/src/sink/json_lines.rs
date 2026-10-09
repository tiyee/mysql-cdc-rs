use super::TransactionSink;
use crate::{Error, OwnedChange, Result, engine::checkpoint::State};
use serde::Serialize;
use std::{io::Write, sync::Mutex};

#[derive(Serialize)]
struct SourcedChange<'a> {
    source: &'a str,
    #[serde(flatten)]
    change: &'a OwnedChange,
}
struct WriterState<W> {
    writer: W,
    failed: bool,
}
pub struct JsonLinesSink<W> {
    inner: Mutex<WriterState<W>>,
}
impl<W: Write + Send> JsonLinesSink<W> {
    pub fn new(writer: W) -> Self {
        Self {
            inner: Mutex::new(WriterState {
                writer,
                failed: false,
            }),
        }
    }
}
impl<W: Write + Send> TransactionSink for JsonLinesSink<W> {
    fn commit(&self, source_id: &str, _state: &State, changes: &[OwnedChange]) -> Result<()> {
        let mut guard = self
            .inner
            .lock()
            .map_err(|_| Error::Sink("output lock poisoned".into()))?;
        if guard.failed {
            return Err(Error::Sink(
                "output failed previously; refusing further commits".into(),
            ));
        }
        // One lock per transaction: lines from different sources never interleave.
        let result = (|| -> Result<()> {
            for change in changes {
                serde_json::to_writer(
                    &mut guard.writer,
                    &SourcedChange {
                        source: source_id,
                        change,
                    },
                )?;
                guard.writer.write_all(b"\n")?;
            }
            guard.writer.flush()?;
            Ok(())
        })();
        if result.is_err() {
            guard.failed = true;
        }
        result
    }
}
