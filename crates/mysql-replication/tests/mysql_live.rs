//! Real-server checks for replication, independent from CDC schema and checkpoints.
use mysql::{Conn, prelude::Queryable};
use mysql_replication::{
    Error, Event, EventData, Result,
    event_type::*,
    events::{
        BinlogMode, FormatDescriptionEvent, Full, Minimal, RowImage, RowValue, RowsDecoder,
        RowsEvent, RowsKind, TableMapEvent,
    },
    primary_key::validate_primary_keys,
    stream::{
        ConnectionConfig, ReplicationConfig, ReplicationConnection, ReplicationStream,
        RowImage as RequiredImage, connect,
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
        fixture.sql(&format!(
            "SET SESSION binlog_format='ROW', binlog_row_image='{}'",
            if full_mode() { "FULL" } else { "MINIMAL" }
        ));
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
        config.required_row_image = Some(if full_mode() {
            RequiredImage::Full
        } else {
            RequiredImage::Minimal
        });
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
        // Preflight fails before the invalid file could be requested.
        let mut conn = connect(&ConnectionConfig::new("CDC_TEST_MYSQL_URL")).unwrap();
        let subscription = (|| -> Result<ReplicationConnection> {
            validate_primary_keys(&mut conn, [(schema.as_str(), table)])?;
            ReplicationConnection::from_connection(
                conn,
                ReplicationConfig::new(902001, "missing.000001", 4),
            )
        })();
        unsupported(subscription, &expected);
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
                (field.column.index, value)
            })
            .collect()
    })
}

fn typed_images<M: BinlogMode>(
    body: &[u8],
    kind: RowsKind,
    v2: bool,
    table_body: &[u8],
    raw: &[u8],
) -> Vec<(Image, Image)> {
    let mut table = TableMapEvent::default();
    table.decode(table_body).unwrap();
    let decoder = RowsDecoder::<M>::new(&table).unwrap();
    let mut event = RowsEvent::<M>::with_layout(kind, if v2 { 10 } else { 8 }, v2);
    event.decode(body).unwrap();
    let mut indices = [None, None];
    decoder
        .rows(&event)
        .unwrap()
        .map(|row| {
            let (before, after) = row.unwrap().operation.into_images();
            for (image, previous) in [before.as_ref(), after.as_ref()]
                .into_iter()
                .zip(&mut indices)
            {
                if let Some(image) = image {
                    let current = image.as_ref().column_indices().as_ptr();
                    if let Some(previous) = previous.replace(current) {
                        assert_eq!(current, previous, "rows share their column plan");
                    }
                }
            }
            (
                image(before.map(|image| image.as_ref().clone()), raw),
                image(after.map(|image| image.as_ref().clone()), raw),
            )
        })
        .collect()
}

#[test]
#[ignore = "requires dedicated MySQL; run with --ignored --test-threads=1"]
fn mysql_raw_rows_preserve_sparse_images_exact_values_and_unselected_tables() {
    let mut fixture = Fixture::new();
    fixture.sql("CREATE TABLE items (id BIGINT UNSIGNED PRIMARY KEY, note VARCHAR(80), amount DECIMAL(12,2)) ENGINE=InnoDB");
    fixture.sql("CREATE TABLE without_key (id INT NOT NULL, note VARCHAR(80)) ENGINE=InnoDB");
    let start = fixture.position();
    let typed_start = start.clone();
    fixture.sql("INSERT INTO items VALUES (18446744073709551615,'Alice',123.40)");
    fixture.sql("UPDATE items SET note=NULL, amount=-123.40 WHERE id=18446744073709551615");
    fixture.sql("UPDATE items SET id=42 WHERE id=18446744073709551615");
    fixture.sql("DELETE FROM items WHERE id=42");
    fixture.sql("INSERT INTO without_key VALUES (7,'outside')");
    let mut conn = connect(&ConnectionConfig::new("CDC_TEST_MYSQL_URL")).unwrap();
    validate_primary_keys(&mut conn, [(fixture.schema.as_str(), "items")]).unwrap();
    let mut stream = ReplicationConnection::from_connection(conn, start).unwrap();
    let mut parser = mysql_replication::BinlogParser::<Minimal>::default();
    let mut tables = BTreeMap::new();
    let mut actual = Vec::new();
    while let Some(frame) = stream.next_frame().unwrap() {
        let body = frame.body;
        let event = parser.parse(frame).unwrap();
        match event.event {
            EventData::FormatDescription(_)
            | EventData::Rotate(_)
            | EventData::RowsStatementEnd(_) => tables.clear(),
            EventData::TableMap(table) if table.schema == fixture.schema.as_bytes() => {
                tables.insert(
                    table.table_id,
                    (
                        String::from_utf8(table.table.to_vec()).unwrap(),
                        RowsDecoder::<Minimal>::new(&table).unwrap(),
                    ),
                );
            }
            EventData::Rows(rows) => {
                if rows.table_id != 0x00ff_ffff || rows.flags & STMT_END_F == 0 {
                    let (table, decoder) = tables
                        .get(&rows.table_id)
                        .expect("ROWS must have a current TABLE_MAP");
                    if !full_mode() && rows.kind != RowsKind::Write {
                        let mut full = RowsEvent::<Full>::with_layout(rows.kind, 10, true);
                        unsupported(
                            full.decode(body),
                            "FULL row images must contain every table column",
                        );
                    }
                    for row in decoder.rows(&rows).unwrap() {
                        let (before, after) = row.unwrap().operation.into_images();
                        let before =
                            image(before.map(|image| image.as_ref().clone()), event.raw_data);
                        let after =
                            image(after.map(|image| image.as_ref().clone()), event.raw_data);
                        actual.push((table.clone(), rows.kind, before, after));
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
                Some(if full_mode() {
                    vec![
                        (0, Integer(u64::MAX)),
                        (1, Bytes(b"Alice".to_vec())),
                        (2, Decimal("123.40".into())),
                    ]
                } else {
                    vec![(0, Integer(u64::MAX))]
                }),
                Some(if full_mode() {
                    vec![
                        (0, Integer(u64::MAX)),
                        (1, Null),
                        (2, Decimal("-123.40".into())),
                    ]
                } else {
                    vec![(1, Null), (2, Decimal("-123.40".into()))]
                })
            ),
            (
                "items".into(),
                RowsKind::Update,
                Some(if full_mode() {
                    vec![
                        (0, Integer(u64::MAX)),
                        (1, Null),
                        (2, Decimal("-123.40".into())),
                    ]
                } else {
                    vec![(0, Integer(u64::MAX))]
                }),
                Some(if full_mode() {
                    vec![(0, Integer(42)), (1, Null), (2, Decimal("-123.40".into()))]
                } else {
                    vec![(0, Integer(42))]
                })
            ),
            (
                "items".into(),
                RowsKind::Delete,
                Some(if full_mode() {
                    vec![(0, Integer(42)), (1, Null), (2, Decimal("-123.40".into()))]
                } else {
                    vec![(0, Integer(42))]
                }),
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
    fn bound_images<M: BinlogMode>(
        config: ReplicationConfig,
        schema: &str,
    ) -> Vec<(String, RowsKind, Image, Image)> {
        let mut stream =
            ReplicationStream::<M>::connect(&ConnectionConfig::new("CDC_TEST_MYSQL_URL"), config)
                .unwrap();
        let mut output = Vec::new();
        while let Some(event) = stream.next_event().unwrap() {
            if let EventData::Rows(rows) = event.event {
                assert_eq!(&*rows.table().unwrap().schema, schema.as_bytes());
                let table = std::str::from_utf8(&rows.table().unwrap().table)
                    .unwrap()
                    .to_owned();
                let columns = rows.columns().unwrap();
                assert_eq!(columns.len(), if table == "items" { 3 } else { 2 });
                if let Some(unsigned) = columns[0].unsigned {
                    assert_eq!(unsigned, table == "items");
                }
                for row in rows.rows().unwrap() {
                    let (before, after) = row.unwrap().operation.into_images();
                    for image in before.iter().chain(&after) {
                        assert!(std::ptr::eq(
                            image.as_ref().columns().as_ptr(),
                            columns.as_ptr()
                        ));
                    }
                    output.push((
                        table.clone(),
                        rows.kind,
                        image(before.map(|image| image.as_ref().clone()), event.raw_data),
                        image(after.map(|image| image.as_ref().clone()), event.raw_data),
                    ));
                }
            }
        }
        output
    }
    let bound = if full_mode() {
        bound_images::<Full>(typed_start, &fixture.schema)
    } else {
        bound_images::<Minimal>(typed_start, &fixture.schema)
    };
    assert_eq!(bound, actual);
}

fn full_mode() -> bool {
    match std::env::var("CDC_TEST_ROW_IMAGE")
        .as_deref()
        .unwrap_or("minimal")
    {
        "minimal" => false,
        "full" => true,
        value => panic!("unsupported CDC_TEST_ROW_IMAGE {value}"),
    }
}

#[test]
#[ignore = "requires dedicated MySQL; run with --ignored --test-threads=1"]
fn mysql_row_image_requirement_rejects_mismatch_and_none_consumes_without_changing_setting() {
    let mut fixture = Fixture::new();
    fixture.sql("CREATE TABLE settings_test (id INT PRIMARY KEY, note VARCHAR(20)) ENGINE=InnoDB");
    let config = fixture.position();
    fixture.sql("INSERT INTO settings_test VALUES (7,'setting')");
    let original: String = fixture
        .conn
        .query_first("SELECT @@global.binlog_row_image")
        .unwrap()
        .unwrap();
    assert!(original.eq_ignore_ascii_case(if full_mode() { "FULL" } else { "MINIMAL" }));
    let mut mismatch = config.clone();
    mismatch.required_row_image = Some(if full_mode() {
        RequiredImage::Minimal
    } else {
        RequiredImage::Full
    });
    unsupported(
        ReplicationConnection::connect(&ConnectionConfig::new("CDC_TEST_MYSQL_URL"), mismatch),
        if full_mode() {
            "requires binlog_row_image=MINIMAL"
        } else {
            "requires binlog_row_image=FULL"
        },
    );
    let mut unrestricted = config;
    unrestricted.required_row_image = None;
    let mut stream =
        ReplicationConnection::connect(&ConnectionConfig::new("CDC_TEST_MYSQL_URL"), unrestricted)
            .unwrap();
    let mut decoder = None;
    let mut parser = mysql_replication::BinlogParser::<Minimal>::default();
    let mut actual = Vec::new();
    while let Some(frame) = stream.next_frame().unwrap() {
        let body = frame.body;
        let event = parser.parse(frame).unwrap();
        match event.event {
            EventData::FormatDescription(_)
            | EventData::Rotate(_)
            | EventData::RowsStatementEnd(_) => decoder = None,
            EventData::TableMap(table) => {
                assert_eq!(table.schema, fixture.schema.as_bytes());
                assert_eq!(table.table, b"settings_test");
                decoder = Some(body.to_vec());
            }
            EventData::Rows(rows) => {
                if rows.table_id != 0x00ff_ffff || rows.flags & STMT_END_F == 0 {
                    assert_eq!(rows.kind, RowsKind::Write);
                    actual.extend(typed_images::<Full>(
                        body,
                        rows.kind,
                        true,
                        decoder.as_ref().unwrap(),
                        event.raw_data,
                    ));
                }
                if rows.flags & STMT_END_F != 0 {
                    decoder = None;
                }
            }
            _ => {}
        }
    }
    assert_eq!(
        actual,
        vec![(
            None,
            Some(vec![
                (0, Value::Integer(7)),
                (1, Value::Bytes(b"setting".to_vec()))
            ])
        )]
    );
    let unchanged: String = fixture
        .conn
        .query_first("SELECT @@global.binlog_row_image")
        .unwrap()
        .unwrap();
    assert_eq!(unchanged, original);
}

fn wide_values(key: u64, updated: bool) -> Vec<(u16, Value)> {
    (0..80)
        .map(|index| {
            let value = match index {
                70 => Value::Integer(key),
                65 if updated => Value::Null,
                75 if updated => Value::Bytes(b"updated".to_vec()),
                75 => Value::Null,
                1 | 65 => Value::Bytes(format!("row{key}-c{index}").into_bytes()),
                _ => Value::Integer(key * 100 + u64::from(index)),
            };
            (index, value)
        })
        .collect()
}

#[test]
#[ignore = "requires dedicated MySQL; run with --ignored --test-threads=1"]
fn mysql_next_view_decodes_typed_wide_multirow_images_in_both_modes() {
    let mut fixture = Fixture::new();
    let columns = (0..80)
        .map(|index| match index {
            70 => "id BIGINT UNSIGNED PRIMARY KEY".into(),
            1 | 65 | 75 => format!("c{index} VARCHAR(32) NULL"),
            _ => format!("c{index} INT NULL"),
        })
        .collect::<Vec<_>>()
        .join(",");
    fixture.sql(&format!("CREATE TABLE wide ({columns}) ENGINE=InnoDB"));
    let config = fixture.position();
    let values = [1, 2]
        .map(|key| {
            let values = wide_values(key, false)
                .into_iter()
                .map(|(_, value)| match value {
                    Value::Integer(value) => value.to_string(),
                    Value::Bytes(value) => format!("'{}'", String::from_utf8(value).unwrap()),
                    Value::Null => "NULL".into(),
                    value => panic!("unexpected SQL fixture value {value:?}"),
                })
                .collect::<Vec<_>>()
                .join(",");
            format!("({values})")
        })
        .join(",");
    fixture.sql(&format!("INSERT INTO wide VALUES {values}"));
    fixture.sql("UPDATE wide SET c65=NULL,c75='updated' WHERE id IN (1,2) ORDER BY id");
    fixture.sql("DELETE FROM wide WHERE id IN (1,2) ORDER BY id");
    let mut stream =
        ReplicationConnection::connect(&ConnectionConfig::new("CDC_TEST_MYSQL_URL"), config)
            .unwrap();
    let mut headers = Vec::new();
    let mut tables = BTreeMap::new();
    let mut actual = Vec::new();
    while let Some(view) = stream.next_view().unwrap() {
        match view.header.event_type {
            FORMAT_DESCRIPTION_EVENT => {
                let mut format = FormatDescriptionEvent::default();
                format.decode(view.body).unwrap();
                assert_eq!(format.header_length, 19);
                headers = format.post_header_lengths.to_vec();
                tables.clear();
            }
            ROTATE_EVENT => tables.clear(),
            TABLE_MAP_EVENT => {
                let mut table = TableMapEvent::with_post_header(usize::from(
                    headers[usize::from(TABLE_MAP_EVENT) - 1],
                ));
                table.decode(view.body).unwrap();
                assert_eq!(table.schema, fixture.schema.as_bytes());
                assert_eq!(table.table, b"wide");
                tables.insert(table.table_id, view.body.to_vec());
            }
            kind @ (WRITE_ROWS_EVENT | UPDATE_ROWS_EVENT | DELETE_ROWS_EVENT) => {
                let operation = match kind {
                    WRITE_ROWS_EVENT => RowsKind::Write,
                    UPDATE_ROWS_EVENT => RowsKind::Update,
                    _ => RowsKind::Delete,
                };
                let mut event = RowsEvent::<Minimal>::with_layout(
                    operation,
                    usize::from(headers[usize::from(kind) - 1]),
                    true,
                );
                event.decode(view.body).unwrap();
                if event.table_id != 0x00ff_ffff || event.flags & STMT_END_F == 0 {
                    let decoder = tables
                        .get(&event.table_id)
                        .expect("ROWS must have a current TABLE_MAP");
                    let rows = if full_mode() {
                        typed_images::<Full>(view.body, operation, true, decoder, view.body)
                    } else {
                        typed_images::<Minimal>(view.body, operation, true, decoder, view.body)
                    };
                    assert_eq!(rows.len(), 2);
                    actual.extend(
                        rows.into_iter()
                            .map(|(before, after)| (operation, before, after)),
                    );
                }
                if event.flags & STMT_END_F != 0 {
                    tables.clear();
                }
            }
            _ => {}
        }
    }
    let mut expected = Vec::new();
    for operation in [RowsKind::Write, RowsKind::Update, RowsKind::Delete] {
        for key in [1, 2] {
            let before = match operation {
                RowsKind::Write => None,
                RowsKind::Update if full_mode() => Some(wide_values(key, false)),
                RowsKind::Delete if full_mode() => Some(wide_values(key, true)),
                _ => Some(vec![(70, Value::Integer(key))]),
            };
            let after = match operation {
                RowsKind::Write => Some(wide_values(key, false)),
                RowsKind::Update if full_mode() => Some(wide_values(key, true)),
                RowsKind::Update => Some(vec![
                    (65, Value::Null),
                    (75, Value::Bytes(b"updated".to_vec())),
                ]),
                RowsKind::Delete => None,
            };
            expected.push((operation, before, after));
        }
    }
    assert_eq!(actual, expected);
}
