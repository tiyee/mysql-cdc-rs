#![forbid(unsafe_code)]
#![doc = include_str!("../README.md")]
//! MySQL replication transport and borrowed binary-log events.
//!
//! Events describe the wire protocol. Filtering, row-to-change conversion,
//! transaction delivery and checkpoint persistence belong to the consumer.
pub mod config;
mod error;
pub mod event_type;
pub mod events;
pub mod file;
mod frame;
pub mod parser;
pub mod primary_key;
pub mod stream;
pub mod version;
mod wire;

pub use error::{Error, Result};
pub use events::{
    BinlogEvent, BinlogMode, Event, EventData, EventHeader, Full, Minimal, OwnedBinlogEvent,
};
pub use frame::{BinlogFrame, EventView, FrameDecoder, MAX_EVENT_BYTES};
pub use parser::BinlogParser;
