use super::Event;
use std::{fmt, io::Write};

/// The FDE trailer is supplied by the frame parser, outside the decoded body.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum ChecksumAlgorithm {
    #[default]
    Unknown,
    None,
    Crc32,
}
impl fmt::Display for ChecksumAlgorithm {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Unknown => "UNKNOWN",
            Self::None => "NONE",
            Self::Crc32 => "CRC32",
        })
    }
}
use crate::{error::invalid, wire::Cursor, Result};

#[derive(Debug, Clone, Copy, Default)]
pub struct FormatDescriptionEvent<'a> {
    pub binlog_version: u16,
    pub server_version: &'a str,
    pub create_timestamp: u32,
    pub header_length: u8,
    pub post_header_lengths: &'a [u8],
    pub checksum_algorithm: ChecksumAlgorithm,
}
impl<'a> Event<'a> for FormatDescriptionEvent<'a> {
    fn dump(&self, writer: &mut dyn Write) -> Result<()> {
        writeln!(writer, "Version: {}", self.binlog_version)?;
        writeln!(writer, "Server version: {}", self.server_version)?;
        writeln!(writer, "Checksum algorithm: {}", self.checksum_algorithm)?;
        writeln!(writer)?;
        Ok(())
    }

    fn decode(&mut self, data: &'a [u8]) -> Result<()> {
        let mut input = Cursor::new(data);
        let binlog_version = input.le(2)? as u16;
        let version = input.take(50)?;
        let end = version
            .iter()
            .position(|&b| b == 0)
            .unwrap_or(version.len());
        let server_version = std::str::from_utf8(&version[..end])?;
        let create_timestamp = input.le(4)? as u32;
        let header_length = input.byte()?;
        if binlog_version != 4 || header_length != 19 {
            return Err(crate::Error::Unsupported(
                "requires binlog v4 with 19-byte common header".into(),
            ));
        }
        *self = Self {
            binlog_version,
            server_version,
            create_timestamp,
            header_length,
            post_header_lengths: input.remaining(),
            checksum_algorithm: self.checksum_algorithm,
        };
        Ok(())
    }
}

#[derive(Debug, Clone, Copy)]
pub struct RotateEvent<'a> {
    pub position: u64,
    pub next_file: &'a [u8],
    pub(crate) post_header: usize,
}
impl Default for RotateEvent<'_> {
    fn default() -> Self {
        Self::with_post_header(8)
    }
}
impl RotateEvent<'_> {
    pub fn with_post_header(post_header: usize) -> Self {
        Self {
            position: 0,
            next_file: &[],
            post_header,
        }
    }
}
impl<'a> Event<'a> for RotateEvent<'a> {
    fn dump(&self, writer: &mut dyn Write) -> Result<()> {
        writeln!(writer, "Position: {}", self.position)?;
        writeln!(
            writer,
            "Next file: {}",
            String::from_utf8_lossy(self.next_file)
        )?;
        writeln!(writer)?;
        Ok(())
    }

    fn decode(&mut self, data: &'a [u8]) -> Result<()> {
        if self.post_header < 8 {
            return Err(invalid("short ROTATE post-header"));
        }
        let mut input = Cursor::new(data);
        let position = Cursor::new(input.take(self.post_header)?).le(8)?;
        let next_file = input.remaining();
        if position < 4 || next_file.is_empty() {
            return Err(invalid("invalid ROTATE file/position"));
        }
        self.position = position;
        self.next_file = next_file;
        Ok(())
    }
}

/// SQL and names remain raw bytes; character-set and DDL policy belong upstairs.
#[derive(Debug, Clone, Copy)]
pub struct QueryEvent<'a> {
    pub thread_id: u32,
    pub execution_time: u32,
    pub error_code: u16,
    pub status_vars: &'a [u8],
    pub schema: &'a [u8],
    pub query: &'a [u8],
    pub(crate) post_header: usize,
}
impl Default for QueryEvent<'_> {
    fn default() -> Self {
        Self::with_post_header(13)
    }
}
impl QueryEvent<'_> {
    pub fn with_post_header(post_header: usize) -> Self {
        Self {
            thread_id: 0,
            execution_time: 0,
            error_code: 0,
            status_vars: &[],
            schema: &[],
            query: &[],
            post_header,
        }
    }
}
impl<'a> Event<'a> for QueryEvent<'a> {
    fn dump(&self, writer: &mut dyn Write) -> Result<()> {
        writeln!(writer, "Thread ID: {}", self.thread_id)?;
        writeln!(writer, "Execution time: {}", self.execution_time)?;
        writeln!(writer, "Error code: {}", self.error_code)?;
        writeln!(writer, "Schema: {}", String::from_utf8_lossy(self.schema))?;
        writeln!(writer, "Query: {}", String::from_utf8_lossy(self.query))?;
        writeln!(writer, "Status variables: {:02x?}", self.status_vars)?;
        writeln!(writer)?;
        Ok(())
    }

    fn decode(&mut self, data: &'a [u8]) -> Result<()> {
        if self.post_header < 13 {
            return Err(invalid("short QUERY post-header"));
        }
        let mut input = Cursor::new(data);
        let mut header = Cursor::new(input.take(self.post_header)?);
        let thread_id = header.le(4)? as u32;
        let execution_time = header.le(4)? as u32;
        let schema_len = usize::from(header.byte()?);
        let error_code = header.le(2)? as u16;
        let status_len = header.le(2)? as usize;
        let status_vars = input.take(status_len)?;
        let schema = input.take(schema_len)?;
        if input.byte()? != 0 {
            return Err(invalid("QUERY schema terminator"));
        }
        *self = Self {
            thread_id,
            execution_time,
            error_code,
            status_vars,
            schema,
            query: input.remaining(),
            post_header: self.post_header,
        };
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct XidEvent {
    pub xid: u64,
}
impl<'a> Event<'a> for XidEvent {
    fn dump(&self, writer: &mut dyn Write) -> Result<()> {
        writeln!(writer, "XID: {}", self.xid)?;
        writeln!(writer)?;
        Ok(())
    }

    fn decode(&mut self, data: &'a [u8]) -> Result<()> {
        let mut input = Cursor::new(data);
        let xid = input.le(8)?;
        input.finish()?;
        self.xid = xid;
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub struct HeartbeatEvent<'a> {
    pub log_file: &'a [u8],
}
impl<'a> Event<'a> for HeartbeatEvent<'a> {
    fn dump(&self, writer: &mut dyn Write) -> Result<()> {
        writeln!(
            writer,
            "Log file: {}",
            String::from_utf8_lossy(self.log_file)
        )?;
        writeln!(writer)?;
        Ok(())
    }

    fn decode(&mut self, data: &'a [u8]) -> Result<()> {
        self.log_file = data;
        Ok(())
    }
}

/// Intervals stay borrowed. Each interval is [start, end), encoded as two u64s.
#[derive(Debug, Clone, Copy)]
pub struct SidIntervals<'a> {
    pub sid: &'a [u8],
    pub intervals: &'a [u8],
}
#[derive(Debug, Clone, Copy, Default)]
pub struct PreviousGtidsEvent<'a> {
    pub sid_count: u64,
    pub encoded_sids: &'a [u8],
}
impl PreviousGtidsEvent<'_> {
    pub fn sids(&self) -> impl Iterator<Item = Result<SidIntervals<'_>>> {
        let mut input = Cursor::new(self.encoded_sids);
        let mut remaining = self.sid_count;
        std::iter::from_fn(move || {
            if remaining == 0 {
                return None;
            }
            remaining -= 1;
            let value = (|| {
                let sid = input.take(16)?;
                let count = usize::try_from(input.le(8)?)
                    .map_err(|_| invalid("interval count overflow"))?;
                let bytes = count
                    .checked_mul(16)
                    .ok_or_else(|| invalid("interval size overflow"))?;
                Ok(SidIntervals {
                    sid,
                    intervals: input.take(bytes)?,
                })
            })();
            if value.is_err() {
                remaining = 0;
            }
            Some(value)
        })
    }
}
impl<'a> Event<'a> for PreviousGtidsEvent<'a> {
    fn dump(&self, writer: &mut dyn Write) -> Result<()> {
        writeln!(writer, "SID count: {}", self.sid_count)?;
        writeln!(writer, "Encoded SIDs: {:02x?}", self.encoded_sids)?;
        writeln!(writer)?;
        Ok(())
    }

    fn decode(&mut self, data: &'a [u8]) -> Result<()> {
        let mut input = Cursor::new(data);
        let sid_count = input.le(8)?;
        let encoded_sids = input.remaining();
        if sid_count > (encoded_sids.len() / 24) as u64 {
            return Err(invalid("PREVIOUS_GTIDS SID count exceeds body"));
        }
        for _ in 0..sid_count {
            input.take(16)?;
            let count =
                usize::try_from(input.le(8)?).map_err(|_| invalid("interval count overflow"))?;
            let bytes = count
                .checked_mul(16)
                .ok_or_else(|| invalid("interval size overflow"))?;
            let mut intervals = Cursor::new(input.take(bytes)?);
            let mut previous_end = 0;
            for _ in 0..count {
                let start = intervals.le(8)?;
                let end = intervals.le(8)?;
                if start == 0 || start >= end || start < previous_end || end > i64::MAX as u64 + 1 {
                    return Err(invalid("invalid GTID interval"));
                }
                previous_end = end;
            }
        }
        input.finish()?;
        *self = Self {
            sid_count,
            encoded_sids,
        };
        Ok(())
    }
}
