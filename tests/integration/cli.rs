#[path = "../support/mod.rs"]
mod common;
use common::*;
use std::{
    fs,
    process::Command,
    time::{SystemTime, UNIX_EPOCH},
};

#[test]
fn offline_cli_replays_from_snapshot_boundary_and_rejects_truncation() {
    let directory = std::env::temp_dir().join(format!(
        "pikachu-cli-{}-{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    fs::create_dir(&directory).unwrap();
    let mut bytes = vec![0xfe, b'b', b'i', b'n'];
    fn append(bytes: &mut Vec<u8>, ty: u8, body: &[u8]) {
        let size = framed(ty, 0, body, true).len();
        let pos = (bytes.len() + size) as u32;
        bytes.extend(framed(ty, pos, body, true));
    }
    append(&mut bytes, FORMAT_DESCRIPTION_EVENT, &format());
    append(
        &mut bytes,
        QUERY_EVENT,
        &query("CREATE TABLE historical_schema_not_replayed (id INT)"),
    );
    let mut snapshot = state();
    snapshot.source_id = Some("east".into());
    snapshot.position = bytes.len() as u64;
    let state_path = directory.join("state.json");
    snapshot.save(&state_path).unwrap();
    append(&mut bytes, QUERY_EVENT, &query("BEGIN"));
    append(
        &mut bytes,
        TABLE_MAP_EVENT,
        &table_map(1, &basic(), &[3, 15, 3], &[100, 0]),
    );
    append(
        &mut bytes,
        DELETE_ROWS_EVENT,
        &rows(1, DELETE_ROWS_EVENT, 3, &[1], &[], &[0, 1, 0, 0, 0]),
    );
    append(&mut bytes, XID_EVENT, &[0; 8]);
    let binlog_path = directory.join(&snapshot.file);
    fs::write(&binlog_path, &bytes).unwrap();
    let execute = || {
        Command::new(env!("CARGO_BIN_EXE_pikachu-cdc"))
            .arg("replay")
            .arg(&state_path)
            .arg(&binlog_path)
            .output()
            .unwrap()
    };
    let result = execute();
    assert!(
        result.status.success(),
        "{}",
        String::from_utf8_lossy(&result.stderr)
    );
    let change: serde_json::Value = serde_json::from_slice(&result.stdout).unwrap();
    assert_eq!(change["pk"]["value"], 1);
    assert_eq!(change["source"], "east");
    assert_eq!(
        pikachu_cdc::engine::checkpoint::State::read(&state_path)
            .unwrap()
            .position,
        snapshot.position
    );
    fs::write(&binlog_path, &bytes[..bytes.len() - 1]).unwrap();
    let result = execute();
    assert!(!result.status.success());
    assert!(result.stdout.is_empty());
    fs::write(&binlog_path, &bytes).unwrap();
    snapshot.position += 1;
    snapshot.save(&state_path).unwrap();
    let result = execute();
    assert!(!result.status.success());
    assert!(result.stdout.is_empty());
    fs::remove_dir_all(directory).unwrap();
}
