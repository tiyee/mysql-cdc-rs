use super::{BinlogMode, Event, Minimal};
use crate::{Result, error::invalid, wire::Cursor};
use std::{io::Write, marker::PhantomData};

mod decoder;
mod metadata;
mod typed;
mod value;

pub use decoder::{ColumnDefinition, Fields, OwnedRowImage, RowImage, RowsDecoder};
pub use metadata::{ColumnInfo, TableDefinition};
pub use mysql_common::constants::ColumnType;
pub use typed::{CompleteImage, Field, Row, RowOperation, Rows, SparseImage};
pub use value::{DecimalValue, IntegerValue, RowValue};

/// Protocol operation, independent from a CDC change or table whitelist.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RowsKind {
    Write,
    Update,
    Delete,
}

/// A borrowed ROWS body. Typed parsers bind its historical TABLE_MAP plan.
/// `Minimal::Image` is sparse and `Full::Image` is complete; `rows()` selects
/// these types statically without an external decoder registry. Raw
/// frames and views remain available for custom adapters.
#[derive(Debug, Clone)]
pub struct RowsEvent<'a, M: BinlogMode> {
    /// TABLE_MAP identifier, independent from the common header's server ID.
    pub table_id: u64,
    /// ROWS-body flags, independent from the common header's flags.
    pub flags: u16,
    pub kind: RowsKind,
    /// Extra data, bitmaps and values are delayed until after table selection.
    pub encoded_images: &'a [u8],
    pub(crate) post_header: usize,
    pub(crate) v2: bool,
    pub(crate) decoder: Option<std::sync::Arc<RowsDecoder<M>>>,
    mode: PhantomData<M>,
}
impl<'a, M: BinlogMode> RowsEvent<'a, M> {
    pub(crate) fn is_statement_end(&self) -> bool {
        self.table_id == 0x00ff_ffff && self.flags & 1 != 0
    }
    pub fn with_layout(kind: RowsKind, post_header: usize, v2: bool) -> Self {
        Self {
            table_id: 0,
            flags: 0,
            kind,
            encoded_images: &[],
            post_header,
            v2,
            decoder: None,
            mode: PhantomData,
        }
    }
    pub fn images(&self) -> Result<RowImages<'a>> {
        let mut input = Cursor::new(self.encoded_images);
        let extra_data = if self.v2 {
            let length = input.le(2)? as usize;
            input.take(
                length
                    .checked_sub(2)
                    .ok_or_else(|| invalid("extra-data length < 2"))?,
            )?
        } else {
            &[]
        };
        let column_count = input.lenenc()?;
        if column_count == 0 || column_count > 4096 {
            return Err(invalid("ROWS column count outside 1..4096"));
        }
        let first = input.take(column_count.div_ceil(8))?;
        let second = if self.kind == RowsKind::Update {
            input.take(column_count.div_ceil(8))?
        } else {
            first
        };
        let images = RowImages {
            extra_data,
            column_count,
            before_columns: first,
            after_columns: second,
            rows: input.remaining(),
        };
        M::validate_images(&images)?;
        Ok(images)
    }
}

/// A statement-end sentinel with no TABLE_MAP or row images.
#[derive(Debug, Clone)]
pub struct RowsStatementEndEvent<'a> {
    event: RowsEvent<'a, Minimal>,
}
impl<'a> RowsStatementEndEvent<'a> {
    pub(crate) fn new(event: RowsEvent<'a, Minimal>) -> Self {
        Self { event }
    }
    pub fn table_id(&self) -> u64 {
        self.event.table_id
    }
    pub fn flags(&self) -> u16 {
        self.event.flags
    }
    pub fn kind(&self) -> RowsKind {
        self.event.kind
    }
    pub(crate) fn layout(&self) -> RowsStatementEndEvent<'static> {
        RowsStatementEndEvent::new(RowsEvent::with_layout(
            self.event.kind,
            self.event.post_header,
            self.event.v2,
        ))
    }
}
impl<'a> Event<'a> for RowsStatementEndEvent<'a> {
    fn decode(&mut self, data: &'a [u8]) -> Result<()> {
        self.event.decode(data)?;
        if !self.event.is_statement_end() {
            return Err(invalid("invalid ROWS statement-end sentinel"));
        }
        Ok(())
    }
    fn dump(&self, writer: &mut dyn Write) -> Result<()> {
        self.event.dump(writer)
    }
}
impl<'a, M: BinlogMode> RowsEvent<'a, M> {
    /// The TABLE_MAP definition bound by this mode's parser.
    pub fn table(&self) -> Result<&TableDefinition> {
        Ok(self.bound_decoder()?.table())
    }
    pub fn columns(&self) -> Result<&[ColumnInfo]> {
        Ok(&self.table()?.columns)
    }
    fn bound_decoder(&self) -> Result<&RowsDecoder<M>> {
        self.decoder
            .as_deref()
            .ok_or_else(|| invalid("ROWS without bound TABLE_MAP"))
    }
    /// Returns sparse or complete images without an external decoder registry.
    pub fn rows(&self) -> Result<Rows<'a, M>> {
        self.bound_decoder()?.rows(self)
    }
}
impl<'a, M: BinlogMode> Event<'a> for RowsEvent<'a, M> {
    fn dump(&self, writer: &mut dyn Write) -> Result<()> {
        writeln!(writer, "Table ID: {}", self.table_id)?;
        writeln!(writer, "Flags: {}", self.flags)?;
        writeln!(writer, "Kind: {:?}", self.kind)?;
        writeln!(writer, "Encoded images: {:02x?}", self.encoded_images)?;
        writeln!(writer)?;
        Ok(())
    }

    fn decode(&mut self, data: &'a [u8]) -> Result<()> {
        let width = self
            .post_header
            .checked_sub(if self.v2 { 4 } else { 2 })
            .ok_or_else(|| invalid("short ROWS post-header"))?;
        if !matches!(width, 4 | 6) {
            return Err(invalid("unsupported ROWS table ID width"));
        }
        let mut input = Cursor::new(data);
        let table_id = input.le(width)?;
        let flags = input.le(2)? as u16;
        self.table_id = table_id;
        self.flags = flags;
        self.encoded_images = input.remaining();
        M::validate_event(self)
    }
}
#[derive(Debug, Clone, Copy)]
pub struct RowImages<'a> {
    pub extra_data: &'a [u8],
    pub column_count: usize,
    /// First wire bitmap; for WRITE this describes the after image.
    pub before_columns: &'a [u8],
    /// Second UPDATE bitmap, otherwise the same first wire bitmap.
    pub after_columns: &'a [u8],
    pub rows: &'a [u8],
}
