use super::Event;
use crate::{Result, error::invalid, wire::Cursor};
use std::io::Write;

mod decoder;
mod typed;
mod value;

pub use decoder::{ColumnDefinition, Row, RowField, RowFields, RowImage, Rows, RowsDecoder};
pub use mysql_common::constants::ColumnType;
pub use typed::{
    CompleteImage, Full, ImageMode, Minimal, SparseImage, TypedRow, TypedRows, TypedRowsDecoder,
    TypedRowsEvent,
};
pub use value::{DecimalValue, IntegerValue, RowValue};

/// Protocol operation, independent from a CDC change or table whitelist.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RowsKind {
    Write,
    Update,
    Delete,
}

#[derive(Debug, Clone, Copy)]
pub struct RowsEvent<'a> {
    pub table_id: u64,
    pub flags: u16,
    pub kind: RowsKind,
    /// Extra data, bitmaps and values are delayed until after table selection.
    pub encoded_images: &'a [u8],
    pub(crate) post_header: usize,
    pub(crate) v2: bool,
}
impl<'a> RowsEvent<'a> {
    pub fn with_layout(kind: RowsKind, post_header: usize, v2: bool) -> Self {
        Self {
            table_id: 0,
            flags: 0,
            kind,
            encoded_images: &[],
            post_header,
            v2,
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
        Ok(RowImages {
            extra_data,
            column_count,
            before_columns: first,
            after_columns: second,
            rows: input.remaining(),
        })
    }

    /// Decodes row boundaries using a previously compiled TABLE_MAP layout.
    /// Values borrow the event; the returned iterator does not borrow the decoder.
    pub fn rows(&self, decoder: &RowsDecoder) -> Result<Rows<'a>> {
        decoder.rows(self)
    }

    /// Validates image shape and binds a typed view to this TABLE_MAP layout.
    /// Row values are validated lazily before each typed row is returned.
    pub fn with_mode<M: ImageMode>(&self, decoder: &RowsDecoder) -> Result<TypedRowsEvent<'a, M>> {
        TypedRowsEvent::new(*self, decoder)
    }
}
impl<'a> Event<'a> for RowsEvent<'a> {
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
        Ok(())
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
