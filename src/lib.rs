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
