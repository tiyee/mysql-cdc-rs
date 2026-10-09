//! Version-specific bytes become borrowed, version-independent event views here.
mod parser;
mod view;

pub use parser::{MysqlEventParser, parse_row_images, parse_v2_row_images};
pub(crate) use parser::{parse_query, parse_rotate};
pub use view::{EventParser, ParsedEvent, RowImages, RowImagesParser, RowsEvent, TableMapEvent};

pub const STMT_END_F: u16 = 1;
pub const GENERATED_INVISIBLE_PK_F: u16 = 4;
