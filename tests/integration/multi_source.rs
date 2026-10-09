#[path = "../support/mod.rs"]
mod common;
use common::*;
use pikachu_rs::{
    Error, OwnedChange, Result, TableMeta,
    config::{AppConfig, Config, SourceConfig, SourceEntry, WhitelistTable},
    engine::checkpoint::State,
    engine::processor::Processor,
    engine::runtime::Runtime,
    protocol::mysql::file::EventView,
    protocol::mysql::version::{
        EventAction, FormatDescription, RowDecoder, ServerVersion, VersionAdapter, VersionRegistry,
    },
    sink::{JsonLinesSink, TransactionSink},
    source::{EventStream, ServerInfo, SourcePlugin, SourceRegistry, SourceSession},
};
use std::{
    io::Write,
    path::PathBuf,
    sync::{
        Arc, Barrier, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::{SystemTime, UNIX_EPOCH},
};

struct Temp(PathBuf);
impl Temp {
    fn new() -> Self {
        static NEXT_TEMP: AtomicUsize = AtomicUsize::new(0);
        let path = std::env::temp_dir().join(format!(
            "pikachu-multi-{}-{}-{}",
            NEXT_TEMP.fetch_add(1, Ordering::Relaxed),
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir(&path).unwrap();
        Self(path)
    }
    fn config(&self, id: &str) -> SourceEntry {
        SourceEntry {
            id: id.into(),
            config: Config {
                state_file: self.0.join(format!("{id}.json")),
                max_transaction_bytes: 1024 * 1024,
                source: SourceConfig {
                    plugin: "fake".into(),
                    adapter: None,
                    url_env: id.into(),
                    server_id: 100,
                    tls_ca: None,
                },
                tables: vec![WhitelistTable {
                    schema: "app".into(),
                    table: "users".into(),
                }],
            },
        }
    }
}
impl Drop for Temp {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}
fn format_for(version: &str) -> Vec<u8> {
    let mut body = format();
    body[2..52].fill(0);
    body[2..2 + version.len()].copy_from_slice(version.as_bytes());
    if version.starts_with("5.7") || version.starts_with("8.0") {
        for kind in [
            WRITE_ROWS_EVENT_V1,
            UPDATE_ROWS_EVENT_V1,
            DELETE_ROWS_EVENT_V1,
        ] {
            body[57 + usize::from(kind) - 1] = 8;
        }
        body.truncate(57 + if version.starts_with("5.7") { 38 } else { 41 });
    }
    body
}
#[derive(Clone)]
struct Packet {
    kind: u8,
    position: u32,
    body: Vec<u8>,
}
fn packets(version: &str, key: i32, changed_age: bool) -> Vec<Packet> {
    let mut values = vec![0];
    values.extend(key.to_le_bytes());
    if changed_age {
        values.extend([0]);
        values.extend(25i32.to_le_bytes());
    } else {
        values.extend([0, 1, b'x']);
    }
    let mut meta = basic();
    if changed_age {
        meta.columns.swap(0, 2);
    }
    vec![
        Packet {
            kind: FORMAT_DESCRIPTION_EVENT,
            position: 0,
            body: format_for(version),
        },
        Packet {
            kind: QUERY_EVENT,
            position: 120,
            body: query("BEGIN"),
        },
        Packet {
            kind: TABLE_MAP_EVENT,
            position: 160,
            body: table_map(7, &meta, &[3, 15, 3], &[100, 0]),
        },
        Packet {
            kind: UPDATE_ROWS_EVENT,
            position: 200,
            body: rows(
                7,
                UPDATE_ROWS_EVENT,
                3,
                &[if changed_age { 4 } else { 1 }],
                &[if changed_age { 1 } else { 2 }],
                &values,
            ),
        },
        Packet {
            kind: XID_EVENT,
            position: 240,
            body: vec![0; 8],
        },
    ]
}
struct FakePlugin {
    barrier: Option<Arc<Barrier>>,
    same_uuid: bool,
    broken: bool,
}
impl SourcePlugin for FakePlugin {
    fn name(&self) -> &str {
        "fake"
    }
    fn connect(
        &self,
        config: &Config,
        registry: &VersionRegistry,
    ) -> Result<Box<dyn SourceSession>> {
        let east = config.source.url_env == "east";
        let version = ServerVersion::parse(if east { "5.7.44-log" } else { "9.0.1" })?;
        let adapter = registry.resolve(&version, config.source.adapter.as_deref())?;
        let info = ServerInfo {
            uuid: if self.same_uuid {
                "same".into()
            } else {
                config.source.url_env.clone()
            },
            version: version.clone(),
            adapter: adapter.name().into(),
        };
        Ok(Box::new(FakeSession {
            info,
            barrier: self.barrier.clone(),
            packets: if self.broken {
                vec![Packet {
                    kind: FORMAT_DESCRIPTION_EVENT,
                    position: 0,
                    body: vec![],
                }]
            } else {
                packets(&version.raw, if east { 11 } else { 22 }, !east)
            },
        }))
    }
}
struct FakeSession {
    info: ServerInfo,
    barrier: Option<Arc<Barrier>>,
    packets: Vec<Packet>,
}
impl SourceSession for FakeSession {
    fn info(&self) -> &ServerInfo {
        &self.info
    }
    fn load_tables(&mut self, _config: &Config) -> Result<Vec<TableMeta>> {
        let mut meta = basic();
        if self.info.version.major == 9 {
            meta.columns.swap(0, 2);
        }
        Ok(vec![meta])
    }
    fn initialize(&mut self, _config: &Config) -> Result<State> {
        let mut s = state();
        s.source_uuid.clone_from(&self.info.uuid);
        s.tables = self.load_tables(_config)?;
        Ok(s)
    }
    fn start(
        self: Box<Self>,
        _config: &Config,
        _state: &State,
        _once: bool,
    ) -> Result<Box<dyn EventStream>> {
        if let Some(barrier) = self.barrier {
            barrier.wait();
        }
        Ok(Box::new(FakeEvents {
            packets: self.packets,
            index: 0,
        }))
    }
}
struct FakeEvents {
    packets: Vec<Packet>,
    index: usize,
}
impl EventStream for FakeEvents {
    fn next_event(&mut self) -> Result<Option<EventView<'_>>> {
        let Some(packet) = self.packets.get(self.index) else {
            return Ok(None);
        };
        self.index += 1;
        Ok(Some(EventView {
            event_type: packet.kind,
            flags: 0,
            log_pos: packet.position,
            data: &packet.body,
        }))
    }
}
fn runtime(plugin: FakePlugin) -> Runtime {
    let mut sources = SourceRegistry::empty();
    sources.register(Arc::new(plugin)).unwrap();
    Runtime {
        sources,
        versions: VersionRegistry::default(),
    }
}
#[derive(Clone, Default)]
struct SharedWriter(Arc<Mutex<Vec<u8>>>);
impl Write for SharedWriter {
    fn write(&mut self, data: &[u8]) -> std::io::Result<usize> {
        self.0.lock().unwrap().extend(data);
        Ok(data.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}
#[test]
fn concurrent_instances_have_independent_table_maps_commits_and_source_output() {
    let temp = Temp::new();
    let east = temp.config("east");
    let west = temp.config("west");
    let east_path = east.config.state_file.clone();
    let west_path = west.config.state_file.clone();
    let output = SharedWriter::default();
    let rt = runtime(FakePlugin {
        barrier: Some(Arc::new(Barrier::new(2))),
        same_uuid: false,
        broken: false,
    });
    rt.run(
        AppConfig {
            sources: vec![east, west],
        },
        true,
        Arc::new(JsonLinesSink::new(output.clone())),
    )
    .unwrap();
    let mut lines: Vec<serde_json::Value> = String::from_utf8(output.0.lock().unwrap().clone())
        .unwrap()
        .lines()
        .map(|s| serde_json::from_str(s).unwrap())
        .collect();
    lines.sort_by_key(|v| v["source"].as_str().unwrap().to_owned());
    assert_eq!(lines.len(), 2);
    assert_eq!(lines[0]["source"], "east");
    assert_eq!(lines[0]["pk"]["value"], 11);
    assert_eq!(lines[0]["changed"], serde_json::json!(["name"]));
    assert_eq!(lines[1]["source"], "west");
    assert_eq!(lines[1]["pk"]["value"], 22);
    assert_eq!(lines[1]["changed"], serde_json::json!(["age"]));
    for (path, id) in [(east_path, "east"), (west_path, "west")] {
        let state = State::read(&path).unwrap();
        assert_eq!(state.position, 240);
        assert_eq!(state.source_id.as_deref(), Some(id));
        assert_eq!(state.source_uuid, id);
    }
}
#[test]
fn rejects_checkpoint_identity_changes_and_duplicate_replication_ids() {
    let temp = Temp::new();
    let entry = temp.config("east");
    let path = entry.config.state_file.clone();
    let rt = runtime(FakePlugin {
        barrier: None,
        same_uuid: false,
        broken: false,
    });
    rt.initialize(AppConfig {
        sources: vec![entry],
    })
    .unwrap();
    let mut changed = temp.config("east");
    changed.id = "renamed".into();
    assert!(
        rt.run(
            AppConfig {
                sources: vec![changed]
            },
            true,
            Arc::new(JsonLinesSink::new(Vec::new()))
        )
        .is_err()
    );
    assert_eq!(State::read(&path).unwrap().position, 4);
    let same = runtime(FakePlugin {
        barrier: None,
        same_uuid: true,
        broken: false,
    });
    assert!(
        same.check(&AppConfig {
            sources: vec![temp.config("east"), temp.config("west")]
        })
        .is_err()
    );
    let mut west = temp.config("west");
    west.config.source.server_id = 101;
    same.check(&AppConfig {
        sources: vec![temp.config("east"), west],
    })
    .unwrap();
}
#[test]
fn broken_stream_stops_and_keeps_all_checkpoints_at_last_commit() {
    let temp = Temp::new();
    let rt = runtime(FakePlugin {
        barrier: Some(Arc::new(Barrier::new(2))),
        same_uuid: false,
        broken: true,
    });
    assert!(
        rt.run(
            AppConfig {
                sources: vec![temp.config("east"), temp.config("west")]
            },
            true,
            Arc::new(JsonLinesSink::new(Vec::new()))
        )
        .is_err()
    );
    for id in ["east", "west"] {
        assert_eq!(
            State::read(&temp.0.join(format!("{id}.json")))
                .unwrap()
                .position,
            4
        );
    }
}
#[test]
fn config_accepts_multi_and_legacy_rejects_duplicates_aliases_and_typos() {
    let temp = Temp::new();
    let path = temp.0.join("cdc.toml");
    let multi = include_str!("../../examples/config/multi-source.toml");
    std::fs::write(&path, multi).unwrap();
    let app = AppConfig::read(&path).unwrap();
    assert_eq!(app.sources.len(), 2);
    assert_eq!(app.sources[0].config.source.plugin, "mysql");
    for bad in [
        multi.replace("id = \"west\"", "id = \"east\""),
        multi.replace("west-state.json", "./east-state.json"),
        multi.replace("server_id = 901002", "server_id = 0"),
        multi.replace("server_id = 901002", "server_idd = 901002"),
        multi.replace("id = \"west\"", "id = \"west\"\ntypo = true"),
    ] {
        std::fs::write(&path, bad).unwrap();
        assert!(AppConfig::read(&path).is_err());
    }
    std::fs::write(
        &path,
        include_str!("../../examples/config/single-source.toml"),
    )
    .unwrap();
    let app = AppConfig::read(&path).unwrap();
    assert_eq!(app.sources[0].id, "default");
    assert_eq!(
        app.sources[0].config.state_file,
        temp.0.join("cdc-state.json")
    );
}

struct Custom {
    created: Arc<AtomicUsize>,
    rows: Arc<AtomicUsize>,
}
impl VersionAdapter for Custom {
    fn name(&self) -> &str {
        "test91"
    }
    fn supports(&self, v: &ServerVersion) -> bool {
        v.major == 9 && v.minor == 1
    }
    fn status_query(&self) -> &str {
        "SHOW CUSTOM STATUS"
    }
    fn classify<'a>(&self, event: EventView<'a>) -> Result<EventAction<'a>> {
        if event.event_type == 200 {
            Ok(EventAction::Ignore)
        } else {
            pikachu_rs::protocol::mysql::version::classify_mysql_event(event)
        }
    }
    fn create_decoder(
        &self,
        tables: Vec<TableMeta>,
        _format: &FormatDescription<'_>,
    ) -> Result<Box<dyn RowDecoder>> {
        self.created.fetch_add(1, Ordering::Relaxed);
        Ok(Box::new(CountingDecoder {
            inner: pikachu_rs::Decoder::new(tables)?,
            count: self.rows.clone(),
        }))
    }
}
struct CountingDecoder {
    inner: pikachu_rs::Decoder,
    count: Arc<AtomicUsize>,
}
impl RowDecoder for CountingDecoder {
    fn clear_table_maps(&mut self) {
        self.inner.clear_table_maps();
    }
    fn table_map(&mut self, body: &[u8]) -> Result<()> {
        self.inner.table_map(body)
    }
    fn rows(
        &mut self,
        kind: u8,
        body: &[u8],
        emit: &mut dyn FnMut(pikachu_rs::Change<'_>) -> Result<()>,
    ) -> Result<()> {
        self.count.fetch_add(1, Ordering::Relaxed);
        self.inner.rows(kind, body, emit)
    }
}
#[test]
fn version_profiles_validate_each_supported_series_and_reject_unknown_series() {
    let registry = VersionRegistry::default();
    for (version, name, query) in [
        ("5.7.44-log", "mysql57", "SHOW MASTER STATUS"),
        ("8.0.44", "mysql80", "SHOW MASTER STATUS"),
        ("8.4.6", "mysql84", "SHOW BINARY LOG STATUS"),
        ("9.0.1", "mysql90", "SHOW BINARY LOG STATUS"),
        ("9.7.2", "mysql97", "SHOW BINARY LOG STATUS"),
    ] {
        let body = format_for(version);
        let f = FormatDescription::parse(&body).unwrap();
        let adapter = registry.resolve(&f.server, None).unwrap();
        assert_eq!(adapter.name(), name);
        assert_eq!(adapter.status_query(), query);
        adapter.validate_format(&f).unwrap();
        let mut p = Processor::new(state(), None, 100_000).unwrap();
        let mut out = Vec::new();
        for packet in packets(version, 33, false) {
            p.process(
                EventView {
                    event_type: packet.kind,
                    flags: 0,
                    log_pos: packet.position,
                    data: &packet.body,
                },
                &mut out,
            )
            .unwrap();
        }
        p.finish().unwrap();
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(&out).unwrap()["pk"]["value"],
            33
        );
    }
    for version in [
        "5.8.1",
        "9.1.0",
        "9.8.0",
        "26.7.0",
        "5.7.44-MariaDB",
        "9.7.2-MariaDB",
        "10.0.0",
    ] {
        assert!(
            registry
                .resolve(&ServerVersion::parse(version).unwrap(), None)
                .is_err()
        );
    }
    assert!(
        registry
            .resolve(&ServerVersion::parse("5.7.44").unwrap(), Some("mysql90"))
            .is_err()
    );
}
#[test]
fn custom_adapter_supplies_control_events_and_row_decoder_without_engine_changes() {
    let created = Arc::new(AtomicUsize::new(0));
    let rows = Arc::new(AtomicUsize::new(0));
    let mut registry = VersionRegistry::default();
    registry
        .register(Arc::new(Custom {
            created: created.clone(),
            rows: rows.clone(),
        }))
        .unwrap();
    assert!(
        registry
            .register(Arc::new(Custom {
                created: created.clone(),
                rows: rows.clone()
            }))
            .is_err()
    );
    let mut processor = Processor::with_registry(state(), None, 100_000, registry, None).unwrap();
    let mut out = Vec::new();
    for packet in packets("9.1.0-test", 99, false) {
        processor
            .process(
                EventView {
                    event_type: packet.kind,
                    flags: 0,
                    log_pos: packet.position,
                    data: &packet.body,
                },
                &mut out,
            )
            .unwrap();
    }
    processor
        .process(
            EventView {
                event_type: 200,
                flags: 0,
                log_pos: 0,
                data: &[],
            },
            &mut out,
        )
        .unwrap();
    assert_eq!(created.load(Ordering::Relaxed), 1);
    assert_eq!(rows.load(Ordering::Relaxed), 1);
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&out).unwrap()["pk"]["value"],
        99
    );
    // A file produced before an upgrade chooses its own adapter on rotation.
    processor
        .process(
            EventView {
                event_type: FORMAT_DESCRIPTION_EVENT,
                flags: 0,
                log_pos: 0,
                data: &format_for("5.7.44"),
            },
            &mut out,
        )
        .unwrap();
    assert_eq!(created.load(Ordering::Relaxed), 1);
    assert!(
        processor
            .process(
                EventView {
                    event_type: 200,
                    flags: 0,
                    log_pos: 0,
                    data: &[]
                },
                &mut out
            )
            .is_err()
    );
}
struct FailingWriter;
impl Write for FailingWriter {
    fn write(&mut self, _data: &[u8]) -> std::io::Result<usize> {
        Err(std::io::Error::other("broken output"))
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Err(std::io::Error::other("broken output"))
    }
}
#[test]
fn shared_sink_failure_never_acknowledges_another_source_or_advances_checkpoint() {
    let sink = JsonLinesSink::new(FailingWriter);
    let state = state();
    assert!(sink.commit("east", &state, &[]).is_err());
    assert!(matches!(
        sink.commit("west", &state, &[]),
        Err(Error::Sink(_))
    ));
    let temp = Temp::new();
    let rt = runtime(FakePlugin {
        barrier: None,
        same_uuid: false,
        broken: false,
    });
    assert!(
        rt.run(
            AppConfig {
                sources: vec![temp.config("east")]
            },
            true,
            Arc::new(JsonLinesSink::new(FailingWriter))
        )
        .is_err()
    );
    assert_eq!(State::read(&temp.0.join("east.json")).unwrap().position, 4);
}
#[test]
fn transaction_sink_can_receive_checkpoint_and_all_changes_in_one_call() {
    struct Capture(Mutex<Vec<(String, u64, usize)>>);
    impl TransactionSink for Capture {
        fn commit(&self, id: &str, state: &State, changes: &[OwnedChange]) -> Result<()> {
            self.0
                .lock()
                .unwrap()
                .push((id.into(), state.position, changes.len()));
            Ok(())
        }
    }
    let sink = Capture(Mutex::new(Vec::new()));
    let mut processor = Processor::new(state(), None, 100_000).unwrap();
    for packet in packets("8.0.44", 55, false) {
        processor
            .process_to(
                EventView {
                    event_type: packet.kind,
                    flags: 0,
                    log_pos: packet.position,
                    data: &packet.body,
                },
                "tenant-a",
                &sink,
            )
            .unwrap();
    }
    assert_eq!(*sink.0.lock().unwrap(), vec![("tenant-a".into(), 240, 1)]);
}

#[test]
fn capabilities_are_checked_per_source_with_absent_57_variables_allowed() {
    use std::collections::BTreeMap;
    let registry = VersionRegistry::default();
    let old = registry
        .resolve(&ServerVersion::parse("5.7.44").unwrap(), None)
        .unwrap();
    let mut vars: BTreeMap<String, String> = [
        ("log_bin", "ON"),
        ("binlog_format", "ROW"),
        ("binlog_row_image", "MINIMAL"),
        ("server_id", "1"),
    ]
    .into_iter()
    .map(|(k, v)| (k.into(), v.into()))
    .collect();
    old.validate_server(&vars, 100).unwrap();
    assert!(old.validate_server(&vars, 1).is_err());
    for (key, value) in [
        ("binlog_format", "STATEMENT"),
        ("binlog_row_image", "FULL"),
        ("log_bin", "OFF"),
        ("binlog_transaction_compression", "ON"),
        ("binlog_row_value_options", "PARTIAL_JSON"),
    ] {
        let mut bad = vars.clone();
        bad.insert(key.into(), value.into());
        assert!(old.validate_server(&bad, 100).is_err());
    }
    vars.insert("binlog_transaction_compression".into(), "OFF".into());
    vars.insert("binlog_row_value_options".into(), "".into());
    registry
        .resolve(&ServerVersion::parse("9.0.1").unwrap(), None)
        .unwrap()
        .validate_server(&vars, 100)
        .unwrap();
    let mut overlap = VersionRegistry::default();
    struct Overlap;
    impl VersionAdapter for Overlap {
        fn name(&self) -> &str {
            "mysql57-custom"
        }
        fn supports(&self, v: &ServerVersion) -> bool {
            v.major == 5 && v.minor == 7
        }
        fn status_query(&self) -> &str {
            "SHOW MASTER STATUS"
        }
    }
    overlap.register(Arc::new(Overlap)).unwrap();
    let v = ServerVersion::parse("5.7.44").unwrap();
    assert!(overlap.resolve(&v, None).is_err());
    assert_eq!(
        overlap.resolve(&v, Some("mysql57-custom")).unwrap().name(),
        "mysql57-custom"
    );
    let mut sources = SourceRegistry::default();
    assert!(
        sources
            .register(Arc::new(pikachu_rs::source::MysqlSource))
            .is_err()
    );
    assert!(sources.resolve("not-registered").is_err());
}

#[test]
fn trait_object_row_decoder_preserves_borrowed_utf8_key() {
    let mut key = column("id", "varchar", true);
    key.charset = Some("utf8mb4".into());
    key.octet_length = Some(100);
    let meta = TableMeta {
        schema: "app".into(),
        table: "users".into(),
        columns: vec![key],
    };
    let mut decoder: Box<dyn RowDecoder> =
        Box::new(pikachu_rs::Decoder::new(vec![meta.clone()]).unwrap());
    decoder
        .table_map(&table_map(7, &meta, &[15], &[100, 0]))
        .unwrap();
    let body = rows(7, WRITE_ROWS_EVENT, 1, &[1], &[], b"\0\x05hello");
    let mut seen = false;
    decoder
        .rows(WRITE_ROWS_EVENT, &body, &mut |change| {
            match change.pk {
                pikachu_rs::PrimaryKey::Text(std::borrow::Cow::Borrowed(text)) => {
                    assert_eq!(text, "hello");
                    assert_eq!(text.as_ptr(), body[body.len() - 5..].as_ptr());
                }
                _ => panic!("plugin boundary copied or erased the borrowed key"),
            }
            seen = true;
            Ok(())
        })
        .unwrap();
    assert!(seen);
}
