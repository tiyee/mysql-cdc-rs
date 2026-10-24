//! Bounded binlog files share framing and event parsing with network replication.
use crate::{
    BinlogEvent, BinlogFrame, BinlogParser, FrameDecoder, MAX_EVENT_BYTES, Result,
    error::invalid,
    events::{BinlogMode, EventHeader},
    parser::ParserConfig,
};
use std::io::Read;

/// Validated raw file frames for custom protocol adapters.
pub struct FrameReader<R> {
    reader: R,
    buffer: Vec<u8>,
    framing: FrameDecoder,
}
impl<R: Read> FrameReader<R> {
    pub fn new(mut reader: R) -> Result<Self> {
        let mut magic = [0; 4];
        reader.read_exact(&mut magic)?;
        if magic != [0xfe, b'b', b'i', b'n'] {
            return Err(invalid("binlog magic"));
        }
        Ok(Self {
            reader,
            buffer: Vec::new(),
            framing: FrameDecoder::new(),
        })
    }
    pub fn next_frame(&mut self) -> Result<Option<BinlogFrame<'_>>> {
        read_frame(&mut self.reader, &mut self.buffer, &mut self.framing)
    }
}

/// A file reader whose field policy is fixed at construction.
pub struct BinlogFile<R, M: BinlogMode> {
    frames: FrameReader<R>,
    parser: BinlogParser<M>,
}
impl<R: Read, M: BinlogMode> BinlogFile<R, M> {
    pub fn new(reader: R, config: ParserConfig) -> Result<Self> {
        Ok(Self {
            frames: FrameReader::new(reader)?,
            parser: BinlogParser::new(config),
        })
    }
    pub fn next_event(&mut self) -> Result<Option<BinlogEvent<'_, M>>> {
        let Some(frame) = self.frames.next_frame()? else {
            return Ok(None);
        };
        self.parser.parse(frame).map(Some)
    }
}

fn read_frame<'a>(
    reader: &mut impl Read,
    buffer: &'a mut Vec<u8>,
    framing: &mut FrameDecoder,
) -> Result<Option<BinlogFrame<'a>>> {
    let mut header = [0; 19];
    loop {
        match reader.read(&mut header[..1]) {
            Ok(0) => {
                return if framing.has_format() {
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
    reader.read_exact(&mut header[1..])?;
    let length = EventHeader::parse(&header)?.event_size as usize;
    if !(19..=MAX_EVENT_BYTES).contains(&length) {
        return Err(invalid("event size outside 19..64MiB"));
    }
    buffer.resize(length, 0);
    buffer[..19].copy_from_slice(&header);
    reader.read_exact(&mut buffer[19..])?;
    framing.decode(buffer).map(Some)
}
