//! MySQL binlog event codes. Keep the wire type as `u8` so unknown events
//! retain their original code and can follow the ignorable-event policy.
use mysql_common::binlog::consts::EventType;

pub const QUERY_EVENT: u8 = EventType::QUERY_EVENT as u8;
pub const STOP_EVENT: u8 = EventType::STOP_EVENT as u8;
pub const ROTATE_EVENT: u8 = EventType::ROTATE_EVENT as u8;
pub const FORMAT_DESCRIPTION_EVENT: u8 = EventType::FORMAT_DESCRIPTION_EVENT as u8;
pub const XID_EVENT: u8 = EventType::XID_EVENT as u8;
pub const TABLE_MAP_EVENT: u8 = EventType::TABLE_MAP_EVENT as u8;
pub const WRITE_ROWS_EVENT_V1: u8 = EventType::WRITE_ROWS_EVENT_V1 as u8;
pub const UPDATE_ROWS_EVENT_V1: u8 = EventType::UPDATE_ROWS_EVENT_V1 as u8;
pub const DELETE_ROWS_EVENT_V1: u8 = EventType::DELETE_ROWS_EVENT_V1 as u8;
pub const HEARTBEAT_EVENT: u8 = EventType::HEARTBEAT_EVENT as u8;
pub const IGNORABLE_EVENT: u8 = EventType::IGNORABLE_EVENT as u8;
pub const ROWS_QUERY_EVENT: u8 = EventType::ROWS_QUERY_EVENT as u8;
pub const WRITE_ROWS_EVENT: u8 = EventType::WRITE_ROWS_EVENT as u8;
pub const UPDATE_ROWS_EVENT: u8 = EventType::UPDATE_ROWS_EVENT as u8;
pub const DELETE_ROWS_EVENT: u8 = EventType::DELETE_ROWS_EVENT as u8;
pub const GTID_EVENT: u8 = EventType::GTID_EVENT as u8;
pub const ANONYMOUS_GTID_EVENT: u8 = EventType::ANONYMOUS_GTID_EVENT as u8;
pub const PREVIOUS_GTIDS_EVENT: u8 = EventType::PREVIOUS_GTIDS_EVENT as u8;
pub const XA_PREPARE_LOG_EVENT: u8 = EventType::XA_PREPARE_LOG_EVENT as u8;
pub const PARTIAL_UPDATE_ROWS_EVENT: u8 = EventType::PARTIAL_UPDATE_ROWS_EVENT as u8;
pub const TRANSACTION_PAYLOAD_EVENT: u8 = EventType::TRANSACTION_PAYLOAD_EVENT as u8;
// MySQL heartbeat v2 is not represented in mysql_common's EventType.
pub const HEARTBEAT_LOG_EVENT_V2: u8 = 41;
pub const GTID_TAGGED_LOG_EVENT: u8 = EventType::GTID_TAGGED_LOG_EVENT as u8;
