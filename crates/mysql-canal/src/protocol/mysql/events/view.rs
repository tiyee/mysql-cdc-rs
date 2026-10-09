use crate::{
    protocol::mysql::{file::EventView, version::EventAction},
    Operation, Result,
};

/// A normalized TABLE_MAP envelope. Column metadata remains borrowed and is
/// compiled only after the schema/table whitelist matches.
#[derive(Debug, Clone, Copy)]
pub struct TableMapEvent<'a> {
    pub table_id: u64,
    pub flags: u16,
    pub schema: &'a str,
    pub table: &'a str,
    pub column_data: &'a [u8],
    pub(crate) standard_body: Option<&'a [u8]>,
}
impl<'a> TableMapEvent<'a> {
    pub fn new(
        table_id: u64,
        flags: u16,
        schema: &'a str,
        table: &'a str,
        column_data: &'a [u8],
    ) -> Self {
        Self {
            table_id,
            flags,
            schema,
            table,
            column_data,
            standard_body: None,
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct RowImages<'a> {
    pub column_count: usize,
    pub before_columns: &'a [u8],
    pub after_columns: &'a [u8],
    pub rows: &'a [u8],
}
pub type RowImagesParser = for<'a> fn(Operation, &'a [u8]) -> Result<RowImages<'a>>;

/// Image parsing is delayed until after table-id lookup. Excluded tables never
/// parse extra-data, bitmaps or row values, even through an adapter.
#[derive(Debug, Clone, Copy)]
pub struct RowsEvent<'a> {
    pub table_id: u64,
    pub flags: u16,
    pub operation: Operation,
    encoded_images: &'a [u8],
    parse_images: RowImagesParser,
    pub(crate) standard_body: Option<(u8, &'a [u8])>,
}
impl<'a> RowsEvent<'a> {
    pub fn new(
        table_id: u64,
        flags: u16,
        operation: Operation,
        encoded_images: &'a [u8],
        parse_images: RowImagesParser,
    ) -> Self {
        Self {
            table_id,
            flags,
            operation,
            encoded_images,
            parse_images,
            standard_body: None,
        }
    }
    pub fn images(self) -> Result<RowImages<'a>> {
        (self.parse_images)(self.operation, self.encoded_images)
    }
}

#[derive(Debug)]
pub enum ParsedEvent<'a> {
    TableMap(TableMapEvent<'a>),
    Rows(RowsEvent<'a>),
    Control(EventAction<'a>),
    /// Lets existing adapters classify additional control event codes. New row
    /// layouts must return a typed TableMap/Rows variant from their parser.
    Unclassified(EventView<'a>),
}
pub trait EventParser: Send {
    fn parse<'a>(&self, event: EventView<'a>) -> Result<ParsedEvent<'a>>;
}
