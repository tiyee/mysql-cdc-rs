use super::Event;
use crate::{error::invalid, wire::Cursor, Error, Result};
use std::io::Write;

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct GtidEvent {
    pub flags: u8,
    pub sid: [u8; 16],
    pub gno: i64,
    pub last_committed: Option<i64>,
    pub sequence_number: Option<i64>,
    pub immediate_commit_timestamp: Option<u64>,
    pub original_commit_timestamp: Option<u64>,
    pub transaction_length: Option<u64>,
    pub immediate_server_version: Option<u32>,
    pub original_server_version: Option<u32>,
    pub commit_group_ticket: Option<u64>,
}
fn nonnegative(value: u64) -> Result<i64> {
    i64::try_from(value).map_err(|_| invalid("GTID integer outside nonnegative i64 range"))
}
impl<'a> Event<'a> for GtidEvent {
    fn dump(&self, writer: &mut dyn Write) -> Result<()> {
        writeln!(writer, "Commit flag: {}", self.flags)?;
        writeln!(writer, "SID: {:02x?}", self.sid)?;
        writeln!(writer, "GNO: {}", self.gno)?;
        writeln!(writer, "Last committed: {:?}", self.last_committed)?;
        writeln!(writer, "Sequence number: {:?}", self.sequence_number)?;
        writeln!(
            writer,
            "Immediate commit timestamp: {:?}",
            self.immediate_commit_timestamp
        )?;
        writeln!(
            writer,
            "Original commit timestamp: {:?}",
            self.original_commit_timestamp
        )?;
        writeln!(writer, "Transaction length: {:?}", self.transaction_length)?;
        writeln!(
            writer,
            "Immediate server version: {:?}",
            self.immediate_server_version
        )?;
        writeln!(
            writer,
            "Original server version: {:?}",
            self.original_server_version
        )?;
        writeln!(
            writer,
            "Commit group ticket: {:?}",
            self.commit_group_ticket
        )?;
        writeln!(writer)?;
        Ok(())
    }

    fn decode(&mut self, data: &'a [u8]) -> Result<()> {
        let mut input = Cursor::new(data);
        let flags = input.byte()?;
        let mut sid = [0; 16];
        sid.copy_from_slice(input.take(16)?);
        let gno = nonnegative(input.le(8)?)?;
        let mut value = Self {
            flags,
            sid,
            gno,
            ..Default::default()
        };
        if !input.remaining().is_empty() {
            if input.byte()? != 2 {
                return Err(Error::Unsupported("GTID logical timestamp layout".into()));
            }
            value.last_committed = Some(nonnegative(input.le(8)?)?);
            value.sequence_number = Some(nonnegative(input.le(8)?)?);
        }
        if !input.remaining().is_empty() {
            let encoded = input.le(7)?;
            let immediate = encoded & !(1 << 55);
            let original = if encoded & (1 << 55) != 0 {
                input.le(7)?
            } else {
                immediate
            };
            value.immediate_commit_timestamp = Some(immediate);
            value.original_commit_timestamp = Some(original);
        }
        if !input.remaining().is_empty() {
            value.transaction_length = Some(input.lenenc()? as u64);
        }
        if !input.remaining().is_empty() {
            let encoded = input.le(4)? as u32;
            let immediate = encoded & !(1 << 31);
            let original = if encoded & (1 << 31) != 0 {
                input.le(4)? as u32
            } else {
                immediate
            };
            value.immediate_server_version = Some(immediate);
            value.original_server_version = Some(original);
        }
        if !input.remaining().is_empty() {
            value.commit_group_ticket = Some(input.le(8)?);
        }
        input.finish()?;
        *self = value;
        Ok(())
    }
}

/// Tagged GTIDs use the serialization-library envelope, not fixed GTID offsets.
#[derive(Debug, Clone, Default)]
pub struct TaggedGtidEvent<'a> {
    pub gtid: GtidEvent,
    pub tag: &'a str,
    /// Unknown ignorable suffix is retained rather than guessed or discarded.
    pub unknown_fields: &'a [u8],
}
impl<'a> Event<'a> for TaggedGtidEvent<'a> {
    fn dump(&self, writer: &mut dyn Write) -> Result<()> {
        writeln!(writer, "Tag: {}", self.tag)?;
        writeln!(writer, "Unknown fields: {:02x?}", self.unknown_fields)?;
        self.gtid.dump(writer)?;
        Ok(())
    }

    fn decode(&mut self, data: &'a [u8]) -> Result<()> {
        let mut input = Cursor::new(data);
        if input.byte()? != 2 {
            return Err(Error::Unsupported(
                "tagged GTID serialization version".into(),
            ));
        }
        let size =
            usize::try_from(input.varint()?).map_err(|_| invalid("tagged GTID size overflow"))?;
        let required = input.varint()?;
        if size != data.len() {
            return Err(invalid("tagged GTID envelope length mismatch"));
        }
        let mut value = Self::default();
        let mut seen = 0u16;
        while !input.remaining().is_empty() {
            let start = input.remaining();
            let id = input.varint()?;
            if id > 11 {
                if id <= required {
                    return Err(Error::Unsupported(format!(
                        "required tagged GTID field {id}"
                    )));
                }
                value.unknown_fields = start;
                break;
            }
            let bit = 1u16 << id;
            if seen & bit != 0 {
                return Err(invalid("duplicate tagged GTID field"));
            }
            seen |= bit;
            match id {
                0 => {
                    value.gtid.flags =
                        u8::try_from(input.varint()?).map_err(|_| invalid("GTID flags overflow"))?
                }
                1 => {
                    for byte in &mut value.gtid.sid {
                        *byte = u8::try_from(input.varint()?)
                            .map_err(|_| invalid("GTID SID byte overflow"))?;
                    }
                }
                2 => value.gtid.gno = positive(input.signed_varint()?)?,
                3 => {
                    let len = usize::try_from(input.varint()?)
                        .map_err(|_| invalid("GTID tag length overflow"))?;
                    value.tag = std::str::from_utf8(input.take(len)?)?;
                    mysql_common::packets::Tag::new(value.tag)
                        .map_err(|_| invalid("invalid GTID tag"))?;
                }
                4 => value.gtid.last_committed = Some(positive(input.signed_varint()?)?),
                5 => value.gtid.sequence_number = Some(positive(input.signed_varint()?)?),
                6 => value.gtid.immediate_commit_timestamp = Some(input.varint()?),
                7 => value.gtid.original_commit_timestamp = Some(input.varint()?),
                8 => value.gtid.transaction_length = Some(input.varint()?),
                9 => {
                    value.gtid.immediate_server_version = Some(
                        u32::try_from(input.varint()?)
                            .map_err(|_| invalid("GTID version overflow"))?,
                    )
                }
                10 => {
                    value.gtid.original_server_version = Some(
                        u32::try_from(input.varint()?)
                            .map_err(|_| invalid("GTID version overflow"))?,
                    )
                }
                11 => value.gtid.commit_group_ticket = Some(input.varint()?),
                _ => return Err(invalid("invalid tagged GTID field")),
            }
        }
        if seen & 0b1110 != 0b1110 || value.gtid.gno == 0 || value.tag.is_empty() {
            return Err(invalid("tagged GTID missing SID, GNO or tag"));
        }
        if value.gtid.original_commit_timestamp.is_none() {
            value.gtid.original_commit_timestamp = value.gtid.immediate_commit_timestamp;
        }
        if value.gtid.original_server_version.is_none() {
            value.gtid.original_server_version = value.gtid.immediate_server_version;
        }
        *self = value;
        Ok(())
    }
}
fn positive(value: i64) -> Result<i64> {
    if value < 0 {
        return Err(invalid("negative tagged GTID integer"));
    }
    Ok(value)
}
