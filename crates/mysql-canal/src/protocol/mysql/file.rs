//! CDC-facing views over the replication library's validated raw frames.
use crate::Result;
use std::io::Read;

#[derive(Debug, Clone, Copy)]
pub struct EventView<'a> {
    pub event_type: u8,
    pub flags: u16,
    pub log_pos: u32,
    pub data: &'a [u8],
}
impl<'a> From<mysql_replication::BinlogFrame<'a>> for EventView<'a> {
    fn from(frame: mysql_replication::BinlogFrame<'a>) -> Self {
        Self {
            event_type: frame.header.event_type,
            flags: frame.header.flags,
            log_pos: frame.header.log_pos,
            data: frame.body,
        }
    }
}

impl<'a> From<mysql_replication::EventView<'a>> for EventView<'a> {
    fn from(event: mysql_replication::EventView<'a>) -> Self {
        Self {
            event_type: event.header.event_type,
            flags: event.header.flags,
            log_pos: event.header.log_pos,
            data: event.body,
        }
    }
}

/// Uses frame-only reading so CDC adapters control parsing and can skip payloads.
pub struct BinlogFile<R> {
    inner: mysql_replication::file::BinlogFile<R>,
}
impl<R: Read> BinlogFile<R> {
    pub fn new(reader: R) -> Result<Self> {
        Ok(Self {
            inner: mysql_replication::file::BinlogFile::new(reader)?,
        })
    }
    pub fn next_event(&mut self) -> Result<Option<EventView<'_>>> {
        Ok(self.inner.next_frame()?.map(EventView::from))
    }
}
pub fn validate_format(body: &[u8]) -> Result<()> {
    let format = crate::protocol::mysql::version::FormatDescription::parse(body)?;
    crate::protocol::mysql::version::VersionRegistry::default()
        .resolve(&format.server, None)?
        .validate_format(&format)
}
