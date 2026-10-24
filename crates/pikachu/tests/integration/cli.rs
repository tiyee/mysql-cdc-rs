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
    snapshot.source_id = "east".into();
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
    assert_eq!(
        change,
        serde_json::json!({
            "source": "east", "schema": "app", "table": "users",
            "operation": "delete", "pk": {"type": "signed", "value": 1}, "changed": []
        })
    );
    assert_eq!(
        mysql_canal::engine::checkpoint::State::read(&state_path)
            .unwrap()
            .position,
        snapshot.position
    );
    for field in ["source_id", "source_plugin"] {
        let mut invalid = serde_json::to_value(&snapshot).unwrap();
        invalid.as_object_mut().unwrap().remove(field);
        fs::write(&state_path, serde_json::to_vec(&invalid).unwrap()).unwrap();
        let result = execute();
        assert!(!result.status.success());
        assert!(result.stdout.is_empty());
        assert!(String::from_utf8_lossy(&result.stderr).contains(field));
    }
    snapshot.save(&state_path).unwrap();
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

#[test]
fn offline_cli_full_mode_preserves_all_changed_fields_and_state() {
    let directory = std::env::temp_dir().join(format!(
        "pikachu-full-cli-{}-{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    fs::create_dir(&directory).unwrap();
    let snapshot = state();
    let state_path = directory.join("state.json");
    snapshot.save(&state_path).unwrap();
    let before = fs::read(&state_path).unwrap();
    let mut bytes = vec![0xfe, b'b', b'i', b'n'];
    let mut append = |ty, body: &[u8]| {
        let size = framed(ty, 0, body, true).len();
        bytes.extend(framed(ty, (bytes.len() + size) as u32, body, true));
    };
    append(FORMAT_DESCRIPTION_EVENT, &format());
    append(QUERY_EVENT, &query("BEGIN"));
    append(
        TABLE_MAP_EVENT,
        &table_map(7, &basic(), &[3, 15, 3], &[100, 0]),
    );
    let mut data = vec![0];
    data.extend(42i32.to_le_bytes());
    data.extend([1, b'x']);
    data.extend(20i32.to_le_bytes());
    data.push(2);
    data.extend(43i32.to_le_bytes());
    data.extend(20i32.to_le_bytes());
    append(
        UPDATE_ROWS_EVENT,
        &rows(7, UPDATE_ROWS_EVENT, 3, &[7], &[7], &data),
    );
    append(XID_EVENT, &[0; 8]);
    let binlog_path = directory.join(&snapshot.file);
    fs::write(&binlog_path, bytes).unwrap();
    let execute = |options: &[&str]| {
        Command::new(env!("CARGO_BIN_EXE_pikachu-cdc"))
            .arg("replay")
            .arg(&state_path)
            .arg(&binlog_path)
            .args(options)
            .output()
            .unwrap()
    };
    let result = execute(&["--row-image", "full"]);
    assert!(
        result.status.success(),
        "{}",
        String::from_utf8_lossy(&result.stderr)
    );
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&result.stdout).unwrap(),
        serde_json::json!({"source":"test", "schema":"app", "table":"users", "operation":"update", "pk":{"type":"signed","value":43}, "previous_pk":{"type":"signed","value":42}, "changed":["id","name","age"]})
    );
    assert_eq!(fs::read(&state_path).unwrap(), before);
    for options in [
        vec![],
        vec!["--row-image", "minimal"],
        vec!["--row-image", "auto"],
        vec!["--row-image"],
        vec!["--row-image", "full", "--row-image", "full"],
    ] {
        let result = execute(&options);
        assert!(!result.status.success(), "{options:?}");
        assert!(result.stdout.is_empty());
        assert_eq!(fs::read(&state_path).unwrap(), before);
        let error = String::from_utf8_lossy(&result.stderr);
        assert!(
            error.contains("MINIMAL")
                || error.contains("minimal or full")
                || error.contains("usage:"),
            "{error}"
        );
    }
    fs::remove_dir_all(directory).unwrap();
}

#[test]
fn online_cli_rejects_invalid_source_overrides_before_connecting() {
    let directory =
        std::env::temp_dir().join(format!("pikachu-cli-options-{}", std::process::id()));
    fs::create_dir(&directory).unwrap();
    let path = directory.join("cdc.toml");
    fs::write(&path, "[[mysql-canal.sources]]\nid='east'\nstate_file='state.json'\n[mysql-canal.sources.source]\nplugin='mysql'\n[mysql-canal.sources.mysql-replication]\nurl_env='UNSET_CDC_ROW_IMAGE_TEST_URL'\nserver_id=100\n[[mysql-canal.sources.tables]]\nschema='app'\ntable='users'\n").unwrap();
    for (command, options, expected) in [
        (
            "check",
            vec!["--row-image", "east=auto"],
            "row_image must be minimal or full",
        ),
        (
            "check",
            vec!["--row-image", "unknown=full"],
            "unknown source unknown",
        ),
        (
            "init",
            vec!["--row-image", "east=full", "--row-image", "east=minimal"],
            "duplicate row-image override",
        ),
        ("check", vec!["--row-image", "full"], "requires SOURCE_ID"),
        ("run", vec!["--row-image"], "requires SOURCE_ID"),
        ("check", vec!["--once"], "unexpected option"),
        ("run", vec!["--once", "--once"], "unexpected option"),
    ] {
        let result = Command::new(env!("CARGO_BIN_EXE_pikachu-cdc"))
            .arg(command)
            .arg(&path)
            .args(options)
            .output()
            .unwrap();
        assert!(!result.status.success());
        assert!(result.stdout.is_empty());
        assert!(
            String::from_utf8_lossy(&result.stderr).contains(expected),
            "{}",
            String::from_utf8_lossy(&result.stderr)
        );
        assert!(!directory.join("state.json").exists());
    }
    fs::remove_dir_all(directory).unwrap();
}

#[test]
fn online_cli_requires_explicit_replication_config_before_connecting() {
    let directory =
        std::env::temp_dir().join(format!("pikachu-cli-replication-{}", std::process::id()));
    fs::create_dir(&directory).unwrap();
    let path = directory.join("cdc.toml");
    fs::write(
        &path,
        "[[mysql-canal.sources]]\nid='east'\nstate_file='state.json'\n[mysql-canal.sources.source]\nplugin='mysql'\n[[mysql-canal.sources.tables]]\nschema='app'\ntable='users'\n",
    )
    .unwrap();
    for command in ["check", "init", "run"] {
        let result = Command::new(env!("CARGO_BIN_EXE_pikachu-cdc"))
            .arg(command)
            .arg(&path)
            .output()
            .unwrap();
        assert!(!result.status.success());
        assert!(result.stdout.is_empty());
        assert!(
            String::from_utf8_lossy(&result.stderr).contains("missing field `mysql-replication`")
        );
        assert!(!directory.join("state.json").exists());
    }
    fs::remove_dir_all(directory).unwrap();
}

#[test]
fn online_cli_requires_canal_configuration_in_the_product_file() {
    let directory = std::env::temp_dir().join(format!("pikachu-cli-canal-{}", std::process::id()));
    fs::create_dir(&directory).unwrap();
    let path = directory.join("pikachu.toml");
    fs::write(
        &path,
        "[[sources]]\nid='east'\nstate_file='state.json'\n[sources.source]\nplugin='mysql'\n[sources.mysql-replication]\nurl_env='UNSET_COMPOSED_CONFIG_TEST_URL'\nserver_id=100\n[[sources.tables]]\nschema='app'\ntable='users'\n",
    )
    .unwrap();
    for command in ["check", "init", "run"] {
        let result = Command::new(env!("CARGO_BIN_EXE_pikachu-cdc"))
            .arg(command)
            .arg(&path)
            .output()
            .unwrap();
        assert!(!result.status.success());
        assert!(result.stdout.is_empty());
        assert!(String::from_utf8_lossy(&result.stderr).contains("unknown field `sources`"));
        assert!(!directory.join("state.json").exists());
    }
    fs::remove_dir_all(directory).unwrap();
}
