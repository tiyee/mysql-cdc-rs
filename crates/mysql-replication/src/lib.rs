#![forbid(unsafe_code)]
//! MySQL replication with configuration-driven, committed event handlers.
//!
//! ```no_run
//! use mysql_replication::{Event, Replication, Result, config::AppConfig};
//! use std::path::Path;
//!
//! fn main() -> Result<()> {
//!     let config = AppConfig::read(Path::new("cdc.toml"))?;
//!     let mut replication = Replication::new(config)?;
//!     replication.add_handle(|event: Event<'_>| {
//!         for change in event.changes {
//!             println!("{} {:?} {:?}", event.source_id, change.operation, change.pk);
//!         }
//!         Ok(())
//!     });
//!     replication.run()
//! }
//! ```

pub mod config;
pub mod engine;
pub mod error;
pub mod model;
pub mod protocol;
pub mod replication;
pub mod sink;
pub mod source;

pub use error::{Error, Result};
pub use model::{Change, ColumnMeta, Operation, OwnedChange, PrimaryKey, TableMeta};
pub use protocol::mysql::Decoder;
pub use replication::{Event, EventHandler, Replication};
