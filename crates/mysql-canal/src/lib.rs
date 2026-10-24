#![forbid(unsafe_code)]
//! MySQL CDC conversion, committed transactions and checkpoint management.
//!
//! ```no_run
//! use mysql_canal::{CommittedTransaction, Canal, Result, config::AppConfig};
//! use std::path::Path;
//!
//! fn main() -> Result<()> {
//!     let config = AppConfig::read(Path::new("canal.toml"))?;
//!     let mut canal = Canal::new(config)?;
//!     canal.add_handle(|event: CommittedTransaction<'_>| {
//!         for change in event.changes {
//!             println!("{} {:?} {:?}", event.source_id, change.operation, change.pk);
//!         }
//!         Ok(())
//!     });
//!     canal.run()
//! }
//! ```

pub mod config;
pub mod engine;
pub mod error;
pub mod model;
pub mod protocol;
pub mod sink;
pub mod source;

pub use engine::Canal;
pub use error::{Error, Result};
pub use model::{
    CdcImageMode, Change, ColumnMeta, CommittedTransaction, Full, Minimal, Operation, OwnedChange,
    PrimaryKey, TableMeta,
};
pub use protocol::mysql::Decoder;
pub use sink::TransactionHandler;
