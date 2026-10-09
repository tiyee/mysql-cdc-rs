use super::Event;
use crate::{Result, error::invalid, wire::Cursor};
use std::io::Write;

/// The complete MySQL binlog v4 common header. Unknown codes and flags survive.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct EventHeader {
    pub timestamp: u32,
    pub event_type: u8,
    pub server_id: u32,
    pub event_size: u32,
    pub log_pos: u32,
    pub flags: u16,
}
impl EventHeader {
    pub const LEN: usize = 19;
    pub fn parse(data: &[u8]) -> Result<Self> {
        let mut header = Self::default();
        header.decode(data)?;
        Ok(header)
    }
}
impl<'a> Event<'a> for EventHeader {
    fn dump(&self, writer: &mut dyn Write) -> Result<()> {
        writeln!(writer, "Timestamp: {}", self.timestamp)?;
        writeln!(writer, "Event type: {}", self.event_type)?;
        writeln!(writer, "Server ID: {}", self.server_id)?;
        writeln!(writer, "Event size: {}", self.event_size)?;
        writeln!(writer, "Log position: {}", self.log_pos)?;
        writeln!(writer, "Flags: {}", self.flags)?;
        writeln!(writer)?;
        Ok(())
    }

    fn decode(&mut self, data: &'a [u8]) -> Result<()> {
        let mut input = Cursor::new(data);
        let parsed = Self {
            timestamp: input.le(4)? as u32,
            event_type: input.byte()?,
            server_id: input.le(4)? as u32,
            event_size: input.le(4)? as u32,
            log_pos: input.le(4)? as u32,
            flags: input.le(2)? as u16,
        };
        input.finish()?;
        if parsed.event_size < Self::LEN as u32 {
            return Err(invalid("event smaller than common header"));
        }
        *self = parsed;
        Ok(())
    }
}
