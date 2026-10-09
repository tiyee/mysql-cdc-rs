#![allow(dead_code)]
pub use mysql_canal::protocol::mysql::event_type::*;
use mysql_canal::{engine::checkpoint::State, ColumnMeta, TableMeta};

pub fn column(name: &str, ty: &str, pk: bool) -> ColumnMeta {
    ColumnMeta {
        name: name.into(),
        data_type: ty.into(),
        column_type: ty.into(),
        nullable: !pk,
        primary_key: pk,
        charset: None,
        octet_length: None,
        numeric_precision: None,
        numeric_scale: None,
        datetime_precision: None,
        extra: String::new(),
    }
}
pub fn basic() -> TableMeta {
    let pk = column("id", "int", true);
    let mut name = column("name", "varchar", false);
    name.charset = Some("utf8mb4".into());
    name.octet_length = Some(100);
    TableMeta {
        schema: "app".into(),
        table: "users".into(),
        columns: vec![pk, name, column("age", "int", false)],
    }
}
pub fn lenenc(out: &mut Vec<u8>, n: usize) {
    match n {
        0..=250 => out.push(n as u8),
        251..=65535 => {
            out.push(0xfc);
            out.extend((n as u16).to_le_bytes());
        }
        _ => {
            out.push(0xfd);
            out.extend(&(n as u32).to_le_bytes()[..3]);
        }
    }
}
pub fn table_map(id: u64, meta: &TableMeta, types: &[u8], metadata: &[u8]) -> Vec<u8> {
    let mut out = id.to_le_bytes()[..6].to_vec();
    out.extend([0, 0]);
    out.push(meta.schema.len() as u8);
    out.extend(meta.schema.as_bytes());
    out.push(0);
    out.push(meta.table.len() as u8);
    out.extend(meta.table.as_bytes());
    out.push(0);
    lenenc(&mut out, types.len());
    out.extend(types);
    lenenc(&mut out, metadata.len());
    out.extend(metadata);
    let mut nullable = vec![0; meta.columns.len().div_ceil(8)];
    for (i, col) in meta.columns.iter().enumerate() {
        if col.nullable {
            nullable[i / 8] |= 1 << (i % 8);
        }
    }
    out.extend(nullable);
    out
}
pub fn rows(id: u64, ty: u8, columns: usize, first: &[u8], second: &[u8], data: &[u8]) -> Vec<u8> {
    let mut out = id.to_le_bytes()[..6].to_vec();
    out.extend([0, 0]);
    if matches!(ty, WRITE_ROWS_EVENT | UPDATE_ROWS_EVENT | DELETE_ROWS_EVENT) {
        out.extend([2, 0]);
    }
    lenenc(&mut out, columns);
    out.extend(first);
    if matches!(ty, UPDATE_ROWS_EVENT_V1 | UPDATE_ROWS_EVENT) {
        out.extend(second);
    }
    out.extend(data);
    out
}
pub fn query(sql: &str) -> Vec<u8> {
    let mut out = vec![0; 13];
    out.push(0);
    out.extend(sql.as_bytes());
    out
}
pub fn format() -> Vec<u8> {
    let mut out = vec![4, 0];
    let mut version = [0; 50];
    version[..6].copy_from_slice(b"8.4.6\0");
    out.extend(version);
    out.extend([0; 4]);
    out.push(19);
    let mut lengths = vec![0; usize::from(GTID_TAGGED_LOG_EVENT)];
    for (event, len) in [
        (QUERY_EVENT, 13),
        (ROTATE_EVENT, 8),
        (TABLE_MAP_EVENT, 8),
        (WRITE_ROWS_EVENT, 10),
        (UPDATE_ROWS_EVENT, 10),
        (DELETE_ROWS_EVENT, 10),
    ] {
        lengths[usize::from(event) - 1] = len;
    }
    out.extend(lengths);
    out
}
pub fn state() -> State {
    State {
        version: 1,
        source_id: "test".into(),
        source_plugin: "mysql".into(),
        source_uuid: "test-source".into(),
        file: "binlog.000001".into(),
        position: 4,
        tables: vec![basic()],
    }
}
pub fn framed(ty: u8, pos: u32, body: &[u8], crc: bool) -> Vec<u8> {
    let mut out = vec![0; 19];
    out[4] = ty;
    out[5..9].copy_from_slice(&1u32.to_le_bytes());
    let size = 19
        + body.len()
        + if crc { 4 } else { 0 }
        + if ty == FORMAT_DESCRIPTION_EVENT {
            if crc {
                1
            } else {
                5
            }
        } else {
            0
        };
    out[9..13].copy_from_slice(&(size as u32).to_le_bytes());
    out[13..17].copy_from_slice(&pos.to_le_bytes());
    out.extend(body);
    if ty == FORMAT_DESCRIPTION_EVENT {
        out.push(u8::from(crc));
        if !crc {
            out.extend([0; 4]);
        }
    }
    if crc {
        let checksum = crc32fast::hash(&out);
        out.extend(checksum.to_le_bytes());
    }
    out
}
