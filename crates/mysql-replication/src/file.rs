//! Bounded binlog files share framing and event parsing with network replication.
use crate::{
    BinlogEvent, BinlogFrame, BinlogParser, FrameDecoder, MAX_EVENT_BYTES, Result,
    error::invalid,
    events::{BinlogMode, EventHeader, Unspecified},
    parser::ParserConfig,
};
use std::io::Read;

pub struct BinlogFile<R, M: BinlogMode = Unspecified> {
    reader: R,
    buffer: Vec<u8>,
    framing: FrameDecoder,
    parser: BinlogParser<M>,
}
impl<R: Read> BinlogFile<R> {
    pub fn new(reader: R) -> Result<Self> {
        Self::with_config(reader, ParserConfig::default())
    }
    pub fn with_config(mut reader: R, config: ParserConfig) -> Result<Self> {
        let mut magic = [0; 4];
        reader.read_exact(&mut magic)?;
        if magic != [0xfe, b'b', b'i', b'n'] {
            return Err(invalid("binlog magic"));
        }
        Ok(Self {
            reader,
            buffer: Vec::new(),
            framing: FrameDecoder::new(),
            parser: BinlogParser::new(config),
        })
    }
}
impl<R: Read, M: BinlogMode> BinlogFile<R, M> {
    pub fn for_mode(reader: R, config: ParserConfig) -> Result<Self> {
        BinlogFile::with_config(reader, config).map(|file| file.with_mode())
    }
    pub fn with_mode<N: BinlogMode>(self) -> BinlogFile<R, N> {
        BinlogFile {
            reader: self.reader,
            buffer: self.buffer,
            framing: self.framing,
            parser: self.parser.with_mode(),
        }
    }
    /// Reads only a frame, so custom adapters can select their own body layout.
    pub fn next_frame(&mut self) -> Result<Option<BinlogFrame<'_>>> {
        read_frame(&mut self.reader, &mut self.buffer, &mut self.framing)
    }
    pub fn next_event(&mut self) -> Result<Option<BinlogEvent<'_, M>>> {
        let Some(frame) = read_frame(&mut self.reader, &mut self.buffer, &mut self.framing)? else {
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
