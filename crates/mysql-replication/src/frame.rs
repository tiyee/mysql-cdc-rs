use crate::{error::invalid, event_type as event, events::EventHeader, Error, Result};
pub const MAX_EVENT_BYTES: usize = 64 * 1024 * 1024;

/// A validated frame. Body excludes the original checksum trailer; raw_data does not.
#[derive(Debug, Clone, Copy)]
pub struct BinlogFrame<'a> {
    pub raw_data: &'a [u8],
    pub header: EventHeader,
    pub body: &'a [u8],
}
#[derive(Debug, Default)]
pub struct FrameDecoder {
    crc32: Option<bool>,
}
impl FrameDecoder {
    pub fn new() -> Self {
        Self::default()
    }
    pub(crate) fn set_checksum(&mut self, crc32: bool) {
        self.crc32 = Some(crc32);
    }
    pub fn has_format(&self) -> bool {
        self.crc32.is_some()
    }
    pub fn decode<'a>(&mut self, raw: &'a [u8]) -> Result<BinlogFrame<'a>> {
        let header_bytes = raw.get(..EventHeader::LEN).ok_or(Error::Truncated {
            need: EventHeader::LEN,
            have: raw.len(),
        })?;
        let header = EventHeader::parse(header_bytes)?;
        let len = header.event_size as usize;
        if !(EventHeader::LEN..=MAX_EVENT_BYTES).contains(&len) || raw.len() != len {
            return Err(invalid("event size does not match bounded raw frame"));
        }
        let is_fde = header.event_type == event::FORMAT_DESCRIPTION_EVENT;
        let crc32 = if is_fde {
            if raw.len() < 19 + 57 + 32 + 5 {
                return Err(invalid("short FORMAT_DESCRIPTION"));
            }
            match raw[raw.len() - 5] {
                0 => false,
                1 => true,
                n => return Err(Error::Unsupported(format!("checksum algorithm {n}"))),
            }
        } else if let Some(value) = self.crc32 {
            value
        } else if header.event_type == event::ROTATE_EVENT {
            false
        } else {
            return Err(invalid("event before FORMAT_DESCRIPTION"));
        };
        let trailer = if is_fde {
            5
        } else if crc32 {
            4
        } else {
            0
        };
        let end = raw
            .len()
            .checked_sub(trailer)
            .filter(|&end| end >= 19)
            .ok_or_else(|| invalid("missing checksum"))?;
        if crc32 {
            let mut hash = crc32fast::Hasher::new();
            let mut checksum_header = [0; 19];
            checksum_header.copy_from_slice(header_bytes);
            if is_fde {
                checksum_header[17] &= !1;
            }
            hash.update(&checksum_header);
            hash.update(&raw[19..raw.len() - 4]);
            let mut expected = [0; 4];
            expected.copy_from_slice(&raw[raw.len() - 4..]);
            if hash.finalize() != u32::from_le_bytes(expected) {
                return Err(Error::Checksum);
            }
        }
        if is_fde {
            self.crc32 = Some(crc32);
        }
        Ok(BinlogFrame {
            raw_data: raw,
            header,
            body: &raw[19..end],
        })
    }
}

/// A body view for consumers that do not retain the complete event. Network
/// readers borrow the driver buffer directly, without assembling a second copy.
#[derive(Debug, Clone, Copy)]
pub struct EventView<'a> {
    pub header: EventHeader,
    pub body: &'a [u8],
}
