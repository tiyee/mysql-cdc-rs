#![forbid(unsafe_code)]
//! MySQL CDC conversion, committed transactions and checkpoint management.
//!
//! ```no_run
//! use mysql_canal::{CommittedTransaction, Cdc, Result, config::AppConfig};
//! use std::path::Path;
//!
//! fn main() -> Result<()> {
//!     let config = AppConfig::read(Path::new("cdc.toml"))?;
//!     let mut replication = Cdc::new(config)?;
//!     replication.add_handle(|event: CommittedTransaction<'_>| {
//!         for change in event.changes {
//!             println!("{} {:?} {:?}", event.source_id, change.operation, change.pk);
//!         }
//!         Ok(())
//!     });
//!     replication.run()
//! }
//! ```

pub mod cdc;
pub mod config;
pub mod engine;
pub mod error;
pub mod model;
pub mod protocol;
pub mod sink;
pub mod source;

pub use cdc::{Cdc, CommittedTransaction, TransactionHandler};
pub use error::{Error, Result};
pub use model::{Change, ColumnMeta, Operation, OwnedChange, PrimaryKey, TableMeta};
pub use protocol::mysql::Decoder;
