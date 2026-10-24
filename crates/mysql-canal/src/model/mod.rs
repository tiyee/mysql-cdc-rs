//! Typed change records, primary keys and ordered table metadata.
pub mod change;
pub mod mode;
pub mod schema;

pub use change::{Change, Operation, OwnedChange, PrimaryKey};
pub use mode::{CdcMode, Configured, Full, Minimal};
pub use schema::{ColumnMeta, TableMeta};
