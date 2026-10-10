//! Real-server checks for replication, independent from CDC schema and checkpoints.
use mysql::{Conn, prelude::Queryable};
use mysql_replication::{
    Error, EventData, Result,
    events::{RowImage, RowValue, RowsDecoder, RowsKind},
    primary_key::validate_primary_keys,
    stream::{
        ConnectionConfig, ReplicationConfig, ReplicationStream, RowImage as RequiredImage, connect,
    },
    version::parse_server_version,
};
use std::{
    collections::BTreeMap,
    time::{SystemTime, UNIX_EPOCH},
};

const STMT_END_F: u16 = 1;

struct Fixture {
    conn: Conn,
    schema: String,
    version: (u32, u32, u32),
}
impl Fixture {
    fn new() -> Self {
        let mut conn = connect(&ConnectionConfig::new("CDC_TEST_MYSQL_URL")).unwrap();
        let raw: String = conn.query_first("SELECT VERSION()").unwrap().unwrap();
        let version = parse_server_version(&raw).unwrap();
        if let Ok(expected) = std::env::var("CDC_TEST_MYSQL_SERIES") {
            assert_eq!(
                format!("{}.{}", version.0, version.1),
                expected,
                "wrong MySQL image series"
            );
        }
        eprintln!("Testing raw replication on MySQL {raw}");
        let schema = format!(
            "replication_{}_{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        );
        let mut fixture = Self {
            conn,
            schema,
            version,
        };
        fixture.sql(&format!(
            "CREATE DATABASE `{}` CHARACTER SET utf8mb4",
            fixture.schema
        ));
        fixture.sql(&format!("USE `{}`", fixture.schema));
        fixture.sql("SET NAMES utf8mb4");
        fixture.sql("SET SESSION binlog_format='ROW', binlog_row_image='MINIMAL'");
        fixture
    }
    fn sql(&mut self, sql: &str) {
        self.conn
            .query_drop(sql)
            .unwrap_or_else(|error| panic!("SQL {sql:?} failed: {error}"));
    }
    fn position(&mut self) -> ReplicationConfig {
        let status = if (self.version.0, self.version.1) >= (8, 4) {
            "SHOW BINARY LOG STATUS"
        } else {
            "SHOW MASTER STATUS"
        };
        let row: mysql::Row = self.conn.query_first(status).unwrap().unwrap();
        let mut config = ReplicationConfig::new(
            902001,
            row.get::<String, _>(0).unwrap(),
            row.get::<u64, _>(1).unwrap(),
        );
        config.non_blocking = true;
        config.required_row_image = Some(RequiredImage::Minimal);
        config
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        if let Err(error) = self.conn.query_drop("ROLLBACK") {
            eprintln!("Raw fixture rollback failed: {error}");
        }
        if let Err(error) = self
            .conn
            .query_drop(format!("DROP DATABASE IF EXISTS `{}`", self.schema))
        {
            eprintln!("Raw fixture cleanup failed: {error}");
        }
    }
}

fn unsupported<T>(result: Result<T>, expected: &str) {
    match result {
        Err(Error::Unsupported(message)) => assert_eq!(message, expected),
        Err(error) => panic!("expected Unsupported({expected:?}), got {error:?}"),
        Ok(_) => panic!("expected Unsupported({expected:?}), got success"),
    }
}

#[test]
#[ignore = "requires dedicated MySQL; run with --ignored --test-threads=1"]
fn mysql_primary_key_preflight_validates_only_selected_tables_before_binlog_request() {
    let mut fixture = Fixture::new();
    for (table, definition) in [
        ("signed_key", "id INT PRIMARY KEY"),
        ("unsigned_key", "id BIGINT UNSIGNED PRIMARY KEY"),
        ("decimal_key", "id DECIMAL(65,30) PRIMARY KEY"),
        ("char_key", "id CHAR(20) CHARACTER SET utf8mb4 PRIMARY KEY"),
        ("utf8_key", "id VARCHAR(20) CHARACTER SET utf8 PRIMARY KEY"),
        (
            "ascii_key",
            "id VARCHAR(20) CHARACTER SET ascii PRIMARY KEY",
        ),
        (
            "latin1_key",
            "id VARCHAR(20) CHARACTER SET latin1 PRIMARY KEY",
        ),
        ("no_key", "id INT"),
        ("unique_key", "id INT NOT NULL UNIQUE"),
        ("composite_key", "id INT, note INT, PRIMARY KEY(id,note)"),
        ("binary_key", "id BINARY(4) PRIMARY KEY"),
        ("float_key", "id FLOAT PRIMARY KEY"),
        ("double_key", "id DOUBLE PRIMARY KEY"),
        ("date_key", "id DATE PRIMARY KEY"),
        ("gbk_key", "id VARCHAR(20) CHARACTER SET gbk PRIMARY KEY"),
    ] {
        fixture.sql(&format!(
            "CREATE TABLE {table} ({definition}) ENGINE=InnoDB"
        ));
    }
    let schema = fixture.schema.clone();
    let selected = [
        "signed_key",
        "unsigned_key",
        "decimal_key",
        "char_key",
        "utf8_key",
        "ascii_key",
        "latin1_key",
    ];
    validate_primary_keys(
        &mut fixture.conn,
        selected.map(|table| (schema.as_str(), table)),
    )
    .unwrap();
    let lower_case: u32 = fixture
        .conn
        .query_first("SELECT @@lower_case_table_names")
        .unwrap()
        .unwrap();
    if lower_case == 0 {
        fixture.sql("CREATE TABLE SIGNED_KEY (id BINARY(4) PRIMARY KEY) ENGINE=InnoDB");
        validate_primary_keys(&mut fixture.conn, [(schema.as_str(), "signed_key")]).unwrap();
        unsupported(
            validate_primary_keys(&mut fixture.conn, [(schema.as_str(), "SIGNED_KEY")]),
            &format!(
                "{schema}.SIGNED_KEY.id: primary-key type binary; use integer, DECIMAL, CHAR or VARCHAR"
            ),
        );
    }
    for (database, table) in [("", "signed_key"), (schema.as_str(), "")] {
        assert!(
            matches!(validate_primary_keys(&mut fixture.conn, [(database, table)]), Err(Error::Invalid(message)) if message == "primary-key check requires a database and table name")
        );
    }
    assert!(
        matches!(validate_primary_keys(&mut fixture.conn, []), Err(Error::Invalid(message)) if message == "primary-key check requires at least one table")
    );
    for (table, reason) in [
        ("no_key", "requires exactly one primary-key column; found 0"),
        (
            "unique_key",
            "requires exactly one primary-key column; found 0",
        ),
        (
            "missing_table",
            "requires exactly one primary-key column; found 0",
        ),
        (
            "composite_key",
            "requires exactly one primary-key column; found 2",
        ),
        (
            "binary_key",
            "id: primary-key type binary; use integer, DECIMAL, CHAR or VARCHAR",
        ),
        (
            "float_key",
            "id: primary-key type float; use integer, DECIMAL, CHAR or VARCHAR",
        ),
        (
            "double_key",
            "id: primary-key type double; use integer, DECIMAL, CHAR or VARCHAR",
        ),
        (
            "date_key",
            "id: primary-key type date; use integer, DECIMAL, CHAR or VARCHAR",
        ),
        ("gbk_key", "id: primary-key charset Some(\"gbk\")"),
    ] {
        let separator = if reason.starts_with("id:") { "." } else { " " };
        let expected = format!("{schema}.{table}{separator}{reason}");
        unsupported(
            validate_primary_keys(&mut fixture.conn, [(schema.as_str(), table)]),
            &expected,
        );
        // An invalid filename would fail differently if a binlog request were sent.
        let config = ReplicationConfig::new(902001, "missing.000001", 4);
        unsupported(
            ReplicationStream::connect_with_tables(
                &ConnectionConfig::new("CDC_TEST_MYSQL_URL"),
                config.clone(),
                [(schema.as_str(), table)],
            ),
            &expected,
        );
        let conn = connect(&ConnectionConfig::new("CDC_TEST_MYSQL_URL")).unwrap();
        unsupported(
            ReplicationStream::from_connection_with_tables(
                conn,
                config,
                [(schema.as_str(), table)],
            ),
            &expected,
        );
    }
}

#[derive(Debug, PartialEq)]
enum Value {
    Integer(u64),
    Bytes(Vec<u8>),
    Decimal(String),
    Null,
}
type Image = Option<Vec<(u16, Value)>>;

fn image(image: Option<RowImage<'_>>, raw: &[u8]) -> Image {
    image.map(|image| {
        image
            .fields()
            .map(|field| {
                let field = field.unwrap();
                let value = match field.value {
                    RowValue::Integer(value) => Value::Integer(value.unsigned()),
                    RowValue::Decimal(value) => Value::Decimal(value.to_string()),
                    RowValue::Bytes(value) => {
                        let address = value.as_ptr() as usize;
                        assert!(
                            address >= raw.as_ptr() as usize
                                && address + value.len() <= raw.as_ptr() as usize + raw.len(),
                            "raw string must borrow the event buffer"
                        );
                        Value::Bytes(value.to_vec())
                    }
                    RowValue::Null => Value::Null,
                    value => panic!("unexpected raw value: {value:?}"),
                };
                (field.column_index, value)
            })
            .collect()
    })
}

#[test]
#[ignore = "requires dedicated MySQL; run with --ignored --test-threads=1"]
fn mysql_raw_rows_preserve_sparse_images_exact_values_and_unselected_tables() {
    let mut fixture = Fixture::new();
    fixture.sql("CREATE TABLE items (id BIGINT UNSIGNED PRIMARY KEY, note VARCHAR(80), amount DECIMAL(12,2)) ENGINE=InnoDB");
    fixture.sql("CREATE TABLE without_key (id INT NOT NULL, note VARCHAR(80)) ENGINE=InnoDB");
    let start = fixture.position();
    fixture.sql("INSERT INTO items VALUES (18446744073709551615,'Alice',123.40)");
    fixture.sql("UPDATE items SET note=NULL, amount=-123.40 WHERE id=18446744073709551615");
    fixture.sql("UPDATE items SET id=42 WHERE id=18446744073709551615");
    fixture.sql("DELETE FROM items WHERE id=42");
    fixture.sql("INSERT INTO without_key VALUES (7,'outside')");
    let mut stream = ReplicationStream::connect_with_tables(
        &ConnectionConfig::new("CDC_TEST_MYSQL_URL"),
        start,
        [(fixture.schema.as_str(), "items")],
    )
    .unwrap();
    let mut tables = BTreeMap::new();
    let mut actual = Vec::new();
    while let Some(event) = stream.next_event().unwrap() {
        match event.event {
            EventData::FormatDescription(_) | EventData::Rotate(_) => tables.clear(),
            EventData::TableMap(table) if table.schema == fixture.schema.as_bytes() => {
                tables.insert(
                    table.table_id,
                    (
                        String::from_utf8(table.table.to_vec()).unwrap(),
                        RowsDecoder::new(&table).unwrap(),
                    ),
                );
            }
            EventData::Rows(rows) => {
                if rows.table_id != 0x00ff_ffff || rows.flags & STMT_END_F == 0 {
                    let (table, decoder) = tables
                        .get(&rows.table_id)
                        .expect("ROWS must have a current TABLE_MAP");
                    for row in rows.rows(decoder).unwrap() {
                        let row = row.unwrap();
                        actual.push((
                            table.clone(),
                            rows.kind,
                            image(row.before, event.raw_data),
                            image(row.after, event.raw_data),
                        ));
                    }
                }
                if rows.flags & STMT_END_F != 0 {
                    tables.clear();
                }
            }
            _ => {}
        }
    }
    use Value::{Bytes, Decimal, Integer, Null};
    assert_eq!(
        actual,
        vec![
            (
                "items".into(),
                RowsKind::Write,
                None,
                Some(vec![
                    (0, Integer(u64::MAX)),
                    (1, Bytes(b"Alice".to_vec())),
                    (2, Decimal("123.40".into()))
                ])
            ),
            (
                "items".into(),
                RowsKind::Update,
                Some(vec![(0, Integer(u64::MAX))]),
                Some(vec![(1, Null), (2, Decimal("-123.40".into()))])
            ),
            (
                "items".into(),
                RowsKind::Update,
                Some(vec![(0, Integer(u64::MAX))]),
                Some(vec![(0, Integer(42))])
            ),
            (
                "items".into(),
                RowsKind::Delete,
                Some(vec![(0, Integer(42))]),
                None
            ),
            (
                "without_key".into(),
                RowsKind::Write,
                None,
                Some(vec![(0, Integer(7)), (1, Bytes(b"outside".to_vec()))])
            ),
        ]
    );
}
