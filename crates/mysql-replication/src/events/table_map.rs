use super::Event;
use crate::{Result, error::invalid, wire::Cursor};
use std::io::Write;

#[derive(Debug, Clone, Copy)]
pub struct TableMapEvent<'a> {
    pub table_id: u64,
    pub flags: u16,
    pub schema: &'a [u8],
    pub table: &'a [u8],
    /// Column types, per-type metadata, nullability and optional metadata.
    /// Kept encoded so excluded tables need no column-plan allocation.
    pub column_data: &'a [u8],
    pub(crate) post_header: usize,
}
impl Default for TableMapEvent<'_> {
    fn default() -> Self {
        Self::with_post_header(8)
    }
}
impl TableMapEvent<'_> {
    pub fn with_post_header(post_header: usize) -> Self {
        Self {
            table_id: 0,
            flags: 0,
            schema: &[],
            table: &[],
            column_data: &[],
            post_header,
        }
    }
    /// Validates column envelopes only when a consumer needs this table.
    pub fn columns(&self) -> Result<TableColumns<'_>> {
        let mut input = Cursor::new(self.column_data);
        let count = input.lenenc()?;
        if count == 0 || count > 4096 {
            return Err(invalid("TABLE_MAP column count outside 1..4096"));
        }
        let types = input.take(count)?;
        let metadata_len = input.lenenc()?;
        let metadata = input.take(metadata_len)?;
        let nullable = input.take(count.div_ceil(8))?;
        Ok(TableColumns {
            count,
            types,
            metadata,
            nullable,
            optional_metadata: input.remaining(),
        })
    }
}
#[derive(Debug, Clone, Copy)]
pub struct TableColumns<'a> {
    pub count: usize,
    pub types: &'a [u8],
    pub metadata: &'a [u8],
    pub nullable: &'a [u8],
    pub optional_metadata: &'a [u8],
}
impl<'a> Event<'a> for TableMapEvent<'a> {
    fn dump(&self, writer: &mut dyn Write) -> Result<()> {
        writeln!(writer, "Table ID: {}", self.table_id)?;
        writeln!(writer, "Flags: {}", self.flags)?;
        writeln!(writer, "Schema: {}", String::from_utf8_lossy(self.schema))?;
        writeln!(writer, "Table: {}", String::from_utf8_lossy(self.table))?;
        writeln!(writer, "Column data: {:02x?}", self.column_data)?;
        writeln!(writer)?;
        Ok(())
    }

    fn decode(&mut self, data: &'a [u8]) -> Result<()> {
        let width = match self.post_header {
            6 => 4,
            8 => 6,
            _ => return Err(invalid("unsupported TABLE_MAP post-header")),
        };
        let mut input = Cursor::new(data);
        let table_id = input.le(width)?;
        let flags = input.le(2)? as u16;
        let schema = input.name()?;
        let table = input.name()?;
        *self = Self {
            table_id,
            flags,
            schema,
            table,
            column_data: input.remaining(),
            post_header: self.post_header,
        };
        Ok(())
    }
}
