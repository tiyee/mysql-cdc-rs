//! Shared real-MySQL setup. No protocol bytes are mocked here.
use mysql::{Conn, prelude::Queryable};
use pikachu_cdc::{
    Result,
    config::{Config, SourceConfig, WhitelistTable},
    engine::{checkpoint::State, processor::Processor},
    protocol::mysql::version::VersionRegistry,
    sink::{JsonLinesSink, TransactionSink},
    source::{EventStream, SourceRegistry, SourceSession, mysql as mysql_source},
};
use serde_json::{Value, json};
use std::{
    fs,
    io::{self, Write},
    path::PathBuf,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU32, Ordering},
    },
    time::{SystemTime, UNIX_EPOCH},
};

pub const SOURCE_ID: &str = "mysql-integration";

pub struct Fixture {
    pub schema: String,
    pub excluded_schema: String,
    pub config: Config,
    pub initial: State,
    cleanup: Cleanup,
}

struct Cleanup {
    connection: Conn,
    schemas: [String; 2],
    directory: PathBuf,
}
impl Drop for Cleanup {
    fn drop(&mut self) {
        // Release any write transaction before dropping the fixture's schemas.
        if let Err(error) = self.connection.query_drop("ROLLBACK") {
            eprintln!("MySQL fixture rollback failed: {error}");
        }
        for schema in &self.schemas {
            if let Err(error) = self
                .connection
                .query_drop(format!("DROP DATABASE IF EXISTS `{schema}`"))
            {
                eprintln!("MySQL fixture cleanup for {schema} failed: {error}");
            }
        }
        if let Err(error) = fs::remove_dir_all(&self.directory) {
            eprintln!("MySQL fixture state cleanup failed: {error}");
        }
    }
}

impl Fixture {
    pub fn new(tables: &[(&str, &str)]) -> Self {
        static NEXT_ID: AtomicU32 = AtomicU32::new(901002);
        let id = NEXT_ID.fetch_add(1, Ordering::Relaxed);
        let schema = format!(
            "cdc_{}_{}_{}",
            std::process::id(),
            id,
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        );
        let excluded_schema = format!("{schema}_other");
        let directory = std::env::temp_dir().join(&schema);
        fs::create_dir(&directory).unwrap();
        let config = Config {
            source: SourceConfig {
                url_env: "CDC_TEST_MYSQL_URL".into(),
                server_id: id,
                tls_ca: None,
                plugin: "mysql".into(),
                adapter: None,
            },
            state_file: directory.join("state.json"),
            max_transaction_bytes: 1024 * 1024,
            tables: tables
                .iter()
                .map(|(table, _)| WhitelistTable {
                    schema: schema.clone(),
                    table: (*table).into(),
                })
                .collect(),
        };
        let mut cleanup = Cleanup {
            connection: mysql_source::connect(&config).unwrap(),
            schemas: [schema.clone(), excluded_schema.clone()],
            directory,
        };
        let connection = &mut cleanup.connection;
        connection
            .query_drop(format!("CREATE DATABASE `{schema}` CHARACTER SET utf8mb4"))
            .unwrap();
        connection
            .query_drop(format!(
                "CREATE DATABASE `{excluded_schema}` CHARACTER SET utf8mb4"
            ))
            .unwrap();
        connection.query_drop(format!("USE `{schema}`")).unwrap();
        connection.query_drop("SET NAMES utf8mb4").unwrap();
        connection
            .query_drop("SET SESSION binlog_format='ROW', binlog_row_image='MINIMAL'")
            .unwrap();
        for (table, definition) in tables {
            connection
                .query_drop(format!(
                    "CREATE TABLE `{table}` ({definition}) ENGINE=InnoDB"
                ))
                .unwrap();
        }
        connection
            .query_drop(
                "CREATE TABLE excluded (id INT PRIMARY KEY, note VARCHAR(80)) ENGINE=InnoDB",
            )
            .unwrap();
        connection.query_drop(format!("CREATE TABLE `{excluded_schema}`.items (id INT PRIMARY KEY, note VARCHAR(80)) ENGINE=InnoDB")).unwrap();
        // Every DDL above precedes the captured schema/checkpoint boundary.
        let mut session = session(&config);
        let info = session.info();
        eprintln!("Testing MySQL {} with {}", info.version.raw, info.adapter);
        if let Ok(expected) = std::env::var("CDC_TEST_MYSQL_SERIES") {
            assert_eq!(
                format!("{}.{}", info.version.major, info.version.minor),
                expected,
                "wrong MySQL image series"
            );
        }
        let mut initial = session.initialize(&config).unwrap();
        initial.source_id = Some(SOURCE_ID.into());
        initial.source_plugin = Some("mysql".into());
        initial.save(&config.state_file).unwrap();
        Self {
            schema,
            excluded_schema,
            config,
            initial,
            cleanup,
        }
    }

    pub fn sql(&mut self, sql: &str) {
        self.cleanup
            .connection
            .query_drop(sql)
            .unwrap_or_else(|error| panic!("SQL {sql:?} failed: {error}"));
    }
    pub fn connection(&mut self) -> &mut Conn {
        &mut self.cleanup.connection
    }
    pub fn saved(&self) -> State {
        State::read(&self.config.state_file).unwrap()
    }
    pub fn processor(&self, checkpoint: &State) -> Processor {
        Processor::new(
            checkpoint.clone(),
            Some(self.config.state_file.clone()),
            self.config.max_transaction_bytes,
        )
        .unwrap()
    }
    pub fn stream(&self, checkpoint: &State, once: bool) -> Box<dyn EventStream> {
        session(&self.config)
            .start(&self.config, checkpoint, once)
            .unwrap()
    }
    pub fn consume(&self, checkpoint: &State, sink: &dyn TransactionSink) -> Result<State> {
        let mut processor = self.processor(checkpoint);
        let mut stream = self.stream(checkpoint, true);
        while let Some(event) = stream.next_event()? {
            processor.process_to(event, SOURCE_ID, sink)?;
        }
        processor.finish()?;
        Ok(self.saved())
    }
    pub fn collect(&self, checkpoint: &State) -> (Vec<Value>, State) {
        let output = CapturedOutput::default();
        let sink = JsonLinesSink::new(output.clone());
        let saved = self.consume(checkpoint, &sink).unwrap();
        (output.changes(), saved)
    }
}

fn session(config: &Config) -> Box<dyn SourceSession> {
    SourceRegistry::default()
        .resolve("mysql")
        .unwrap()
        .connect(config, &VersionRegistry::default())
        .unwrap()
}

#[derive(Clone, Default)]
pub struct CapturedOutput(Arc<Mutex<Vec<u8>>>);
impl CapturedOutput {
    pub fn is_empty(&self) -> bool {
        self.0.lock().unwrap().is_empty()
    }
    pub fn changes(&self) -> Vec<Value> {
        String::from_utf8(self.0.lock().unwrap().clone())
            .unwrap()
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect()
    }
}
impl Write for CapturedOutput {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

pub fn expected(schema: &str, table: &str, operation: &str, pk: Value, changed: &[&str]) -> Value {
    json!({"source": SOURCE_ID, "schema": schema, "table": table, "operation": operation, "pk": pk, "changed": changed})
}

pub fn assert_unordered(actual: Vec<Value>, expected: Vec<Value>) {
    // SQL does not promise row visit order for a multi-row statement. Retain
    // duplicates when comparing so lost/duplicated row events still fail.
    let ordered = |rows: Vec<Value>| {
        let mut rows: Vec<_> = rows.iter().map(Value::to_string).collect();
        rows.sort();
        rows
    };
    assert_eq!(ordered(actual), ordered(expected));
}
