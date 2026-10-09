//! Explicit opt-in: creates and removes a uniquely named test database.
use mysql::prelude::Queryable;
use pikachu_cdc::{
    config::{Config, SourceConfig, WhitelistTable},
    engine::processor::Processor,
    protocol::mysql::version::VersionRegistry,
    source::SourceRegistry,
    source::mysql as mysql_source,
};
use std::{
    path::PathBuf,
    time::{SystemTime, UNIX_EPOCH},
};

#[test]
#[ignore = "requires dedicated MySQL with CDC_TEST_MYSQL_URL; see tests/infra/compose.yaml"]
fn mysql_row_minimal_end_to_end() {
    let name = format!(
        "pikachu_cdc_test_{}_{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_micros()
    );
    let config = Config {
        source: SourceConfig {
            url_env: "CDC_TEST_MYSQL_URL".into(),
            server_id: 901002,
            tls_ca: None,
            plugin: "mysql".into(),
            adapter: None,
        },
        state_file: PathBuf::from("unused-test-state.json"),
        max_transaction_bytes: 1024 * 1024,
        tables: ["users", "strings", "money"]
            .into_iter()
            .map(|table| WhitelistTable {
                schema: name.clone(),
                table: table.into(),
            })
            .collect(),
    };
    let mut admin = mysql_source::connect(&config).unwrap();
    let uuid = mysql_source::validate_server(&mut admin, config.source.server_id).unwrap();
    admin
        .query_drop(format!("CREATE DATABASE `{name}`"))
        .unwrap();
    admin.query_drop(format!("USE `{name}`")).unwrap();
    admin.query_drop("CREATE TABLE users (id BIGINT UNSIGNED PRIMARY KEY, name VARCHAR(100) CHARACTER SET utf8mb4, amount DECIMAL(12,4), flags BIT(9), kind ENUM('a','b'), tags SET('a','b'), body JSON, created_at DATETIME(6), raw_data BLOB) ENGINE=InnoDB").unwrap();
    admin.query_drop("CREATE TABLE strings (id VARCHAR(40) CHARACTER SET utf8mb4 PRIMARY KEY, note VARCHAR(80)) ENGINE=InnoDB").unwrap();
    admin.query_drop("CREATE TABLE money (id DECIMAL(10,2) PRIMARY KEY, note CHAR(100) CHARACTER SET utf8mb4) ENGINE=InnoDB").unwrap();
    admin
        .query_drop("CREATE TABLE excluded (id INT PRIMARY KEY) ENGINE=InnoDB")
        .unwrap();
    let versions = VersionRegistry::default();
    let session = SourceRegistry::default()
        .resolve("mysql")
        .unwrap()
        .connect(&config, &versions)
        .unwrap();
    let mut session = session;
    assert_eq!(session.info().uuid, uuid);
    let info = session.info();
    eprintln!("Testing MySQL {} with {}", info.version.raw, info.adapter);
    if let Ok(expected) = std::env::var("CDC_TEST_MYSQL_SERIES") {
        assert_eq!(
            format!("{}.{}", info.version.major, info.version.minor),
            expected,
            "the CI image must match the requested MySQL series"
        );
    }
    let state = session.initialize(&config).unwrap();
    // Set session values as GLOBAL changes do not affect existing sessions.
    admin
        .query_drop("SET SESSION binlog_format='ROW', binlog_row_image='MINIMAL'")
        .unwrap();
    admin.query_drop("INSERT INTO users VALUES (18446744073709551615,'Alice',12345678.9000,b'100000001','b','a,b',JSON_OBJECT('v',1),NOW(6),X'010203')").unwrap();
    admin
        .query_drop(
            "UPDATE users SET name=NULL, body=JSON_OBJECT('v',2) WHERE id=18446744073709551615",
        )
        .unwrap();
    admin
        .query_drop("UPDATE users SET id=7 WHERE id=18446744073709551615")
        .unwrap();
    admin
        .query_drop("INSERT INTO strings VALUES ('你好','a'),('world','b')")
        .unwrap();
    admin
        .query_drop("UPDATE strings SET id='新主键' WHERE id='你好'")
        .unwrap();
    admin
        .query_drop("INSERT INTO money VALUES (-12345678.90,'long char')")
        .unwrap();
    admin
        .query_drop("DELETE FROM money WHERE id=-12345678.90")
        .unwrap();
    admin.query_drop("INSERT INTO excluded VALUES (1)").unwrap();
    admin.query_drop("BEGIN").unwrap();
    admin
        .query_drop("INSERT INTO strings VALUES ('rolled_back','x')")
        .unwrap();
    admin.query_drop("ROLLBACK").unwrap();
    let mut output = Vec::new();
    let mut processor = Processor::new(state.clone(), None, config.max_transaction_bytes).unwrap();
    let mut stream = session.start(&config, &state, true).unwrap();
    while let Some(event) = stream.next_event().unwrap() {
        processor.process(event, &mut output).unwrap();
    }
    processor.finish().unwrap();
    let changes: Vec<serde_json::Value> = String::from_utf8(output)
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    assert_eq!(changes.len(), 8, "{changes:#?}");
    assert_eq!(changes[0]["pk"]["value"], u64::MAX);
    assert_eq!(changes[1]["changed"], serde_json::json!(["name", "body"]));
    assert_eq!(changes[2]["pk"]["value"], 7);
    assert_eq!(changes[2]["previous_pk"]["value"], u64::MAX);
    assert_eq!(changes[5]["pk"]["value"], "新主键");
    assert_eq!(changes[5]["previous_pk"]["value"], "你好");
    assert_eq!(changes[7]["pk"]["value"], "-12345678.90");
    assert!(processor.state().position > state.position);
    admin.query_drop(format!("DROP DATABASE `{name}`")).unwrap();
}
