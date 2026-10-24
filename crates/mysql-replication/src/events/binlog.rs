use super::{BinlogMode, Event, EventData, EventHeader};
use crate::{BinlogFrame, Result};
use std::{ops::Range, sync::Arc};

/// One wire event, borrowing the reader's complete, unmodified event buffer.
/// The public fields are identical for every mode; only the ROWS payload and
/// decoded image types carry the selected policy. This is an envelope, whereas
/// [`Event`] is the shared decoding/dump trait and [`EventData`] is its body enum.
///
/// ```compile_fail
/// use mysql_replication::{BinlogEvent, Full, Minimal};
/// fn change_policy(event: BinlogEvent<'_, Full>) -> BinlogEvent<'_, Minimal> { event }
/// ```
#[derive(Debug, Clone)]
pub struct BinlogEvent<'a, M: BinlogMode> {
    /// Original header, body and trailer/checksum, excluding file/network framing.
    pub raw_data: &'a [u8],
    /// The same 19-byte common header in every row-image mode.
    pub header: EventHeader,
    /// Protocol body; the ROWS variant retains this envelope's mode parameter.
    pub event: EventData<'a, M>,
    pub(crate) body: &'a [u8],
}
impl<M: BinlogMode> BinlogEvent<'_, M> {
    pub fn frame(&self) -> BinlogFrame<'_> {
        BinlogFrame {
            raw_data: self.raw_data,
            header: self.header,
            body: self.body,
        }
    }
    /// Retains one raw buffer and its layout; typed fields are borrowed on view().
    pub fn into_owned(self) -> Result<OwnedBinlogEvent<M>> {
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
pub struct OwnedBinlogEvent<M: BinlogMode> {
    raw_data: Arc<[u8]>,
    header: EventHeader,
    body: Range<usize>,
    template: EventData<'static, M>,
}
impl<M: BinlogMode> OwnedBinlogEvent<M> {
    pub fn raw_data(&self) -> &[u8] {
        &self.raw_data
    }
    pub fn view(&self) -> Result<BinlogEvent<'_, M>> {
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
