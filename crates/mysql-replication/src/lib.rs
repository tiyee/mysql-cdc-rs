#![forbid(unsafe_code)]
#![doc = include_str!("../README.md")]
//! MySQL replication transport and borrowed binary-log events.
//!
//! Events describe the wire protocol. Filtering, row-to-change conversion,
//! transaction delivery and checkpoint persistence belong to the consumer.
mod error;
pub mod event_type;
pub mod events;
pub mod file;
mod frame;
pub mod parser;
pub mod stream;
mod wire;

pub use error::{Error, Result};
pub use events::{BinlogEvent, Event, EventData, EventHeader, OwnedBinlogEvent};
pub use frame::{BinlogFrame, EventView, FrameDecoder, MAX_EVENT_BYTES};
pub use parser::BinlogParser;
