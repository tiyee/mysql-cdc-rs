//! Bounded, reusable offline binlog framing. Truncated tails are errors, never EOF.
use crate::protocol::mysql::event_type as event;
use crate::{Error, Result, error::invalid, protocol::mysql::wire::Cursor};
use std::io::Read;

pub const MAX_EVENT_BYTES: usize = 64 * 1024 * 1024;

#[derive(Debug, Clone, Copy)]
pub struct EventView<'a> {
    /// Raw MySQL event code; compare with constants in [`crate::protocol::mysql::event_type`].
    pub event_type: u8,
    pub flags: u16,
    pub log_pos: u32,
    pub data: &'a [u8],
}

pub struct BinlogFile<R> {
    reader: R,
    buffer: Vec<u8>,
    crc32: Option<bool>,
}
impl<R: Read> BinlogFile<R> {
    pub fn new(mut reader: R) -> Result<Self> {
        let mut magic = [0; 4];
        reader.read_exact(&mut magic)?;
        if magic != [0xfe, b'b', b'i', b'n'] {
            return Err(invalid("binlog magic"));
        }
        Ok(Self {
            reader,
            buffer: Vec::new(),
            crc32: None,
        })
    }
    pub fn next_event(&mut self) -> Result<Option<EventView<'_>>> {
        let mut header = [0; 19];
        loop {
            match self.reader.read(&mut header[..1]) {
                Ok(0) => {
                    return if self.crc32.is_some() {
                        Ok(None)
                    } else {
                        Err(invalid("binlog has no FORMAT_DESCRIPTION"))
                    };
                }
                Ok(_) => break,
                Err(e) if e.kind() == std::io::ErrorKind::Interrupted => continue,
                Err(e) => return Err(e.into()),
            }
        }
        self.reader.read_exact(&mut header[1..])?;
        let mut cursor = Cursor::new(&header);
        cursor.take(4)?;
        let event_type = cursor.byte()?;
        cursor.take(4)?;
        let len = cursor.le(4)? as usize;
        let log_pos = cursor.le(4)? as u32;
        let flags = cursor.le(2)? as u16;
        if !(19..=MAX_EVENT_BYTES).contains(&len) {
            return Err(invalid("event size outside 19..64MiB"));
        }
        self.buffer.resize(len - 19, 0);
        self.reader.read_exact(&mut self.buffer)?;
        let is_fde = event_type == event::FORMAT_DESCRIPTION_EVENT;
        if is_fde {
            // Supported servers (5.7+) always store the algorithm byte + 4-byte
            // trailer in FDE, including when binlog_checksum=NONE.
            if self.buffer.len() < 57 + 32 + 5 {
                return Err(invalid("short FORMAT_DESCRIPTION"));
            }
            let alg = self.buffer[self.buffer.len() - 5];
            self.crc32 = Some(match alg {
                0 => false,
                1 => true,
                _ => return Err(Error::Unsupported(format!("checksum algorithm {alg}"))),
            });
        }
        let crc32 = self
            .crc32
            .ok_or_else(|| invalid("offline binlog must begin with FORMAT_DESCRIPTION"))?;
        let trailer = if is_fde {
            5
        } else if crc32 {
            4
        } else {
            0
        };
        let body_len = self
            .buffer
            .len()
            .checked_sub(trailer)
            .ok_or_else(|| invalid("missing checksum"))?;
        if crc32 {
            let mut hash = crc32fast::Hasher::new();
            if is_fde {
                header[17] &= !1;
            }
            hash.update(&header);
            hash.update(&self.buffer[..self.buffer.len() - 4]);
            let mut checksum = Cursor::new(&self.buffer[self.buffer.len() - 4..]);
            if u64::from(hash.finalize()) != checksum.le(4)? {
                return Err(Error::Checksum);
            }
        }
        Ok(Some(EventView {
            event_type,
            flags,
            log_pos,
            data: &self.buffer[..body_len],
        }))
    }
}

pub fn validate_format(body: &[u8]) -> Result<()> {
    let format = crate::protocol::mysql::version::FormatDescription::parse(body)?;
    crate::protocol::mysql::version::VersionRegistry::default()
        .resolve(&format.server, None)?
        .validate_format(&format)
}
