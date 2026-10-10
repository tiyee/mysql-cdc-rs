use super::{Event, EventData, EventHeader};
use crate::{BinlogFrame, Result};
use std::{ops::Range, sync::Arc};

/// One wire event, borrowing the reader's complete, unmodified event buffer.
#[derive(Debug, Clone)]
pub struct BinlogEvent<'a> {
    pub raw_data: &'a [u8],
    pub header: EventHeader,
    pub event: EventData<'a>,
    pub(crate) body: &'a [u8],
}
impl BinlogEvent<'_> {
    pub fn frame(&self) -> BinlogFrame<'_> {
        BinlogFrame {
            raw_data: self.raw_data,
            header: self.header,
            body: self.body,
        }
    }
    /// Retains one raw buffer and its layout; typed fields are borrowed on view().
    pub fn into_owned(self) -> Result<OwnedBinlogEvent> {
        let start = EventHeader::LEN;
        let end = start + self.body.len();
        if self.raw_data.get(start..end) != Some(self.body)
            || EventHeader::parse(
                self.raw_data
                    .get(..start)
                    .ok_or_else(|| crate::error::invalid("missing common header"))?,
            )? != self.header
        {
            return Err(crate::error::invalid("event fields do not match raw data"));
        }
        Ok(OwnedBinlogEvent {
            raw_data: Arc::from(self.raw_data),
            header: self.header,
            body: start..end,
            template: self.event.layout(),
        })
    }
}

/// No self references: a view borrows this owned raw buffer when requested.
#[derive(Debug, Clone)]
pub struct OwnedBinlogEvent {
    raw_data: Arc<[u8]>,
    header: EventHeader,
    body: Range<usize>,
    template: EventData<'static>,
}
impl OwnedBinlogEvent {
    pub fn raw_data(&self) -> &[u8] {
        &self.raw_data
    }
    pub fn view(&self) -> Result<BinlogEvent<'_>> {
        let body = &self.raw_data[self.body.clone()];
        let mut event = self.template.clone();
        event.decode(body)?;
        Ok(BinlogEvent {
            raw_data: &self.raw_data,
            header: self.header,
            event,
            body,
        })
    }
}
