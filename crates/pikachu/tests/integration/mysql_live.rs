//! Real SQL through the CLI, including JSON output and persisted checkpoints.
use mysql::{Conn, Opts, prelude::Queryable};
use mysql_canal::{
    config::RowImageMode, engine::checkpoint::State, protocol::mysql::version::ServerVersion,
};
use serde_json::{Value, json};
use std::{
    fs,
    path::PathBuf,
    process::{Command, Output},
    time::{SystemTime, UNIX_EPOCH},
};

struct Fixture {
    conn: Conn,
    schema: String,
    directory: PathBuf,
    config: PathBuf,
    state: PathBuf,
}
impl Fixture {
    fn new() -> Self {
        let url = std::env::var("CDC_TEST_MYSQL_URL")
            .expect("CDC_TEST_MYSQL_URL must name a disposable test server");
        let opts = Opts::from_url(&url).unwrap_or_else(|_| panic!("invalid test MySQL URL"));
        let mut conn = Conn::new(opts).unwrap();
        let raw: String = conn.query_first("SELECT VERSION()").unwrap().unwrap();
        let version = ServerVersion::parse(&raw).unwrap();
        if let Ok(expected) = std::env::var("CDC_TEST_MYSQL_SERIES") {
            assert_eq!(
                format!("{}.{}", version.major, version.minor),
                expected,
                "wrong MySQL image series"
            );
        }
        eprintln!("Testing pikachu-cdc on MySQL {raw}");
        let schema = format!(
            "cli_{}_{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        );
        let directory = std::env::temp_dir().join(&schema);
        fs::create_dir(&directory).unwrap();
        let mut fixture = Self {
            conn,
            schema,
            config: directory.join("cdc.toml"),
            state: directory.join("state.json"),
            directory,
        };
        fixture.sql(&format!(
            "CREATE DATABASE `{}` CHARACTER SET utf8mb4",
            fixture.schema
        ));
        fixture.sql(&format!("USE `{}`", fixture.schema));
        fixture.sql(&format!(
            "SET SESSION binlog_format='ROW', binlog_row_image='{}'",
            row_image_mode().server_value()
        ));
        fixture.sql("CREATE TABLE items (id INT PRIMARY KEY, note VARCHAR(80)) ENGINE=InnoDB");
        fixture.sql("CREATE TABLE excluded (id INT) ENGINE=InnoDB");
        fs::write(&fixture.config, format!(
            "[[sources]]\nid = 'cli-live'\nstate_file = 'state.json'\n[sources.source]\nurl_env = 'CDC_TEST_MYSQL_URL'\nserver_id = 903001\nrow_image = '{}'\n[[sources.tables]]\nschema = '{}'\ntable = 'items'\n", row_image_mode(), fixture.schema
        )).unwrap();
        fixture
    }
    fn sql(&mut self, sql: &str) {
        self.conn
            .query_drop(sql)
            .unwrap_or_else(|error| panic!("SQL {sql:?} failed: {error}"));
    }
    fn execute(&self, command: &str) -> Output {
        let mut process = Command::new(env!("CARGO_BIN_EXE_pikachu-cdc"));
        process.arg(command).arg(&self.config);
        if command == "run" {
            process.arg("--once");
        }
        process.output().unwrap()
    }
    fn success(&self, command: &str) -> Vec<Value> {
        let result = self.execute(command);
        assert!(
            result.status.success(),
            "{command}: {}",
            String::from_utf8_lossy(&result.stderr)
        );
        String::from_utf8(result.stdout)
            .unwrap()
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect()
    }
    fn expected(&self, operation: &str, id: i32, changed: &[&str]) -> Value {
        let changed = if row_image_mode() == RowImageMode::Full && operation != "delete" {
            &["id", "note"]
        } else {
            changed
        };
        json!({"source":"cli-live", "schema":self.schema, "table":"items", "operation":operation, "pk":{"type":"signed", "value":id}, "changed":changed})
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        if let Err(error) = self.conn.query_drop("ROLLBACK") {
            eprintln!("CLI fixture rollback failed: {error}");
        }
        if let Err(error) = self
            .conn
            .query_drop(format!("DROP DATABASE IF EXISTS `{}`", self.schema))
        {
            eprintln!("CLI fixture schema cleanup failed: {error}");
        }
        if let Err(error) = fs::remove_dir_all(&self.directory) {
            eprintln!("CLI fixture state cleanup failed: {error}");
        }
    }
}

#[test]
#[ignore = "requires dedicated MySQL; run with --ignored --test-threads=1"]
fn mysql_cli_checks_initializes_consumes_resumes_and_stops_at_ddl() {
    let mut fixture = Fixture::new();
    assert!(fixture.success("check").is_empty());
    assert!(
        !fixture.state.exists(),
        "check must not create a checkpoint"
    );
    assert!(fixture.success("init").is_empty());
    let initial = State::read(&fixture.state).unwrap();
    assert_eq!(initial.source_id, "cli-live");
    assert_eq!(initial.source_plugin, "mysql");
    assert!(!initial.source_uuid.is_empty());
    let before = fs::read(&fixture.state).unwrap();
    let existing = fixture.execute("init");
    assert!(!existing.status.success());
    assert!(existing.stdout.is_empty());
    assert!(String::from_utf8_lossy(&existing.stderr).contains("state file exists"));
    assert_eq!(fs::read(&fixture.state).unwrap(), before);

    fixture.sql("INSERT INTO items VALUES (1,'created')");
    fixture.sql("UPDATE items SET note=NULL WHERE id=1");
    fixture.sql("INSERT INTO excluded VALUES (7)");
    fixture.sql("BEGIN");
    fixture.sql("INSERT INTO items VALUES (2,'rolled back')");
    fixture.sql("ROLLBACK");
    fixture.sql("DELETE FROM items WHERE id=1");
    assert_eq!(
        fixture.success("run"),
        [
            fixture.expected("insert", 1, &["id", "note"]),
            fixture.expected("update", 1, &["note"]),
            fixture.expected("delete", 1, &[])
        ]
    );
    let committed = State::read(&fixture.state).unwrap();
    assert_eq!(committed.file, initial.file);
    assert!(committed.position > initial.position);
    assert_eq!(committed.source_uuid, initial.source_uuid);
    assert_eq!(committed.tables[0].columns.len(), 2);
    assert!(
        fixture.success("run").is_empty(),
        "acknowledged rows must not be replayed"
    );
    fixture.sql("INSERT INTO items VALUES (3,'resumed')");
    assert_eq!(
        fixture.success("run"),
        [fixture.expected("insert", 3, &["id", "note"])]
    );
    let before = fs::read(&fixture.state).unwrap();
    fixture.sql("ALTER TABLE items ADD extra INT");
    fixture.sql("INSERT INTO items VALUES (4,'after DDL',8)");
    let ddl = fixture.execute("run");
    assert!(!ddl.status.success());
    assert!(ddl.stdout.is_empty());
    assert!(String::from_utf8_lossy(&ddl.stderr).contains("ALTER TABLE items ADD extra INT"));
    assert_eq!(
        fs::read(&fixture.state).unwrap(),
        before,
        "DDL must not advance the saved checkpoint"
    );
}

#[test]
#[ignore = "requires dedicated MySQL; run with --ignored --test-threads=1"]
fn mysql_cli_primary_key_errors_preserve_fresh_and_existing_state() {
    for (alter, reason) in [
        (
            "DROP PRIMARY KEY",
            "requires exactly one primary-key column; found 0",
        ),
        ("MODIFY id BINARY(4) NOT NULL", "primary-key type binary"),
    ] {
        let mut fixture = Fixture::new();
        assert!(fixture.success("init").is_empty());
        let before = fs::read(&fixture.state).unwrap();
        fixture.sql(&format!("ALTER TABLE items {alter}"));
        let lock = fixture.state.with_extension("json.lock");
        for resume in [true, false] {
            if !resume {
                fs::remove_file(&fixture.state).unwrap();
                fs::remove_file(&lock).unwrap();
            }
            for command in ["check", "init", "run"] {
                let result = fixture.execute(command);
                assert!(!result.status.success(), "{command} must reject {alter}");
                assert!(
                    result.stdout.is_empty(),
                    "preflight errors must not emit change records"
                );
                let stderr = String::from_utf8_lossy(&result.stderr);
                assert!(
                    stderr.contains(&fixture.schema) && stderr.contains(reason),
                    "{command}: {stderr}"
                );
                if resume {
                    assert_eq!(fs::read(&fixture.state).unwrap(), before);
                    assert!(lock.exists());
                } else {
                    assert!(!fixture.state.exists());
                    assert!(
                        !lock.exists(),
                        "preflight must fail before creating state locks"
                    );
                }
            }
        }
    }
}

fn row_image_mode() -> RowImageMode {
    std::env::var("CDC_TEST_ROW_IMAGE")
        .unwrap_or_else(|_| "minimal".into())
        .parse()
        .unwrap()
}

#[test]
#[ignore = "requires dedicated MySQL; run with --ignored --test-threads=1"]
fn mysql_cli_source_override_controls_preflight_and_decoder() {
    let mut fixture = Fixture::new();
    let requested = row_image_mode();
    let opposite = if requested == RowImageMode::Full {
        "minimal"
    } else {
        "full"
    };
    let config = fs::read_to_string(&fixture.config).unwrap().replace(
        &format!("row_image = '{requested}'"),
        &format!("row_image = '{opposite}'"),
    );
    fs::write(&fixture.config, config).unwrap();
    let result = fixture.execute("check");
    assert!(!result.status.success());
    assert!(result.stdout.is_empty());
    assert!(String::from_utf8_lossy(&result.stderr).contains("requires binlog_row_image="));
    assert!(!fixture.state.exists());
    let execute = |command: &str| {
        let mut process = Command::new(env!("CARGO_BIN_EXE_pikachu-cdc"));
        process
            .arg(command)
            .arg(&fixture.config)
            .arg("--row-image")
            .arg(format!("cli-live={requested}"));
        if command == "run" {
            process.arg("--once");
        }
        process.output().unwrap()
    };
    for command in ["check", "init"] {
        let result = execute(command);
        assert!(
            result.status.success(),
            "{}",
            String::from_utf8_lossy(&result.stderr)
        );
        assert!(result.stdout.is_empty());
    }
    fixture.sql("INSERT INTO items VALUES (1,'old')");
    fixture.sql("UPDATE items SET note=NULL WHERE id=1");
    let result = Command::new(env!("CARGO_BIN_EXE_pikachu-cdc"))
        .arg("run")
        .arg(&fixture.config)
        .args(["--once", "--row-image"])
        .arg(format!("cli-live={requested}"))
        .output()
        .unwrap();
    assert!(
        result.status.success(),
        "{}",
        String::from_utf8_lossy(&result.stderr)
    );
    let actual: Vec<Value> = String::from_utf8(result.stdout)
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    assert_eq!(
        actual,
        [
            fixture.expected("insert", 1, &["id", "note"]),
            fixture.expected("update", 1, &["note"])
        ]
    );
}
