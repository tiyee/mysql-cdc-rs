#![forbid(unsafe_code)]

pub mod config;
pub mod engine;
pub mod error;
pub mod model;
pub mod protocol;
pub mod sink;
pub mod source;

pub use error::{Error, Result};
pub use model::{Change, ColumnMeta, Operation, OwnedChange, PrimaryKey, TableMeta};
pub use protocol::mysql::Decoder;

// Preserve existing public import paths while implementations are grouped by domain.
pub use engine::{processor, runtime, transaction};
pub use model::{change, schema};
pub use protocol::mysql::{decoder, event_type, file, version};
pub use source as backend;
