#![forbid(unsafe_code)]
#![doc = include_str!("../README.md")]

/// CDC conversion, committed transactions and checkpoint management.
pub use mysql_canal as canal;
/// Replication transport and borrowed raw binlog events.
pub use mysql_replication as replication;
