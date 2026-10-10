//! Concrete protocol events share decoding and diagnostic output, not CDC semantics.
mod binlog;
mod control;
mod gtid;
mod header;
mod rows;
mod table_map;

use crate::Result;
pub use binlog::{BinlogEvent, OwnedBinlogEvent};
pub use control::{
    ChecksumAlgorithm, FormatDescriptionEvent, HeartbeatEvent, PreviousGtidsEvent, QueryEvent,
    RotateEvent, SidIntervals, XidEvent,
};
pub use gtid::{GtidEvent, TaggedGtidEvent};
pub use header::EventHeader;
pub use rows::{
    ColumnDefinition, ColumnType, DecimalValue, IntegerValue, Row, RowField, RowFields, RowImage,
    RowImages, RowValue, Rows, RowsDecoder, RowsEvent, RowsKind,
};
use std::{fmt::Debug, io::Write};
pub use table_map::{TableColumns, TableMapEvent};

/// A decoder is bound to its file layout before receiving an event body.
/// Borrowed fields remain valid for the input lifetime. Header decoding receives
/// exactly 19 bytes; body decoding excludes the common header and checksum.
pub trait Event<'a>: Debug {
    fn decode(&mut self, data: &'a [u8]) -> Result<()>;
    /// Writes diagnostics only. This does not serialize a binary event or CDC row.
    fn dump(&self, writer: &mut dyn Write) -> Result<()>;
}

#[derive(Debug, Clone, Copy, Default)]
pub struct RawEvent<'a> {
    pub data: &'a [u8],
}
impl<'a> Event<'a> for RawEvent<'a> {
    fn dump(&self, writer: &mut dyn Write) -> Result<()> {
        writeln!(writer, "Data: {:02x?}\n", self.data)?;
        Ok(())
    }
    fn decode(&mut self, data: &'a [u8]) -> Result<()> {
        self.data = data;
        Ok(())
    }
}

/// An explicit variant retains typed access without downcasting.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum EventData<'a> {
    FormatDescription(FormatDescriptionEvent<'a>),
    Rotate(RotateEvent<'a>),
    Query(QueryEvent<'a>),
    TableMap(TableMapEvent<'a>),
    Rows(RowsEvent<'a>),
    Xid(XidEvent),
    Gtid(GtidEvent),
    AnonymousGtid(GtidEvent),
    TaggedGtid(TaggedGtidEvent<'a>),
    PreviousGtids(PreviousGtidsEvent<'a>),
    Heartbeat(HeartbeatEvent<'a>),
    /// Unknown or deliberately unimplemented body; never silently discarded.
    Raw(RawEvent<'a>),
}
impl<'a> Event<'a> for EventData<'a> {
    fn dump(&self, writer: &mut dyn Write) -> Result<()> {
        match self {
            Self::FormatDescription(event) => event.dump(writer),
            Self::Rotate(event) => event.dump(writer),
            Self::Query(event) => event.dump(writer),
            Self::TableMap(event) => event.dump(writer),
            Self::Rows(event) => event.dump(writer),
            Self::Xid(event) => event.dump(writer),
            Self::TaggedGtid(event) => event.dump(writer),
            Self::PreviousGtids(event) => event.dump(writer),
            Self::Heartbeat(event) => event.dump(writer),
            Self::Raw(event) => event.dump(writer),
            Self::Gtid(event) | Self::AnonymousGtid(event) => event.dump(writer),
        }
    }

    fn decode(&mut self, data: &'a [u8]) -> Result<()> {
        match self {
            Self::FormatDescription(v) => v.decode(data),
            Self::Rotate(v) => v.decode(data),
            Self::Query(v) => v.decode(data),
            Self::TableMap(v) => v.decode(data),
            Self::Rows(v) => v.decode(data),
            Self::Xid(v) => v.decode(data),
            Self::Gtid(v) | Self::AnonymousGtid(v) => v.decode(data),
            Self::TaggedGtid(v) => v.decode(data),
            Self::PreviousGtids(v) => v.decode(data),
            Self::Heartbeat(v) => v.decode(data),
            Self::Raw(v) => v.decode(data),
        }
    }
}

impl EventData<'_> {
    pub(super) fn layout(&self) -> EventData<'static> {
        match self {
            Self::FormatDescription(v) => EventData::FormatDescription(FormatDescriptionEvent {
                checksum_algorithm: v.checksum_algorithm,
                ..Default::default()
            }),
            Self::Rotate(v) => EventData::Rotate(RotateEvent::with_post_header(v.post_header)),
            Self::Query(v) => EventData::Query(QueryEvent::with_post_header(v.post_header)),
            Self::TableMap(v) => {
                EventData::TableMap(TableMapEvent::with_post_header(v.post_header))
            }
            Self::Rows(v) => EventData::Rows(RowsEvent::with_layout(v.kind, v.post_header, v.v2)),
            Self::Xid(_) => EventData::Xid(Default::default()),
            Self::Gtid(_) => EventData::Gtid(Default::default()),
            Self::AnonymousGtid(_) => EventData::AnonymousGtid(Default::default()),
            Self::TaggedGtid(_) => EventData::TaggedGtid(Default::default()),
            Self::PreviousGtids(_) => EventData::PreviousGtids(Default::default()),
            Self::Heartbeat(_) => EventData::Heartbeat(Default::default()),
            Self::Raw(_) => EventData::Raw(Default::default()),
        }
    }
}
