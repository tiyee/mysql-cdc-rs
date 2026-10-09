//! MySQL binlog framing, event constants, row decoding and version adaptation.
pub mod decoder;
pub use mysql_replication::event_type;
pub mod events;
pub mod file;
pub mod version;
pub(crate) mod wire;

pub use decoder::Decoder;
