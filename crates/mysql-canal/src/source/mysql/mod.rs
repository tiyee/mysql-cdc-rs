//! MySQL transport, capability discovery, schema loading and initial position capture.
use super::{EventStream, ServerInfo, SourcePlugin, SourceSession};
use crate::{
    config::Config,
    engine::checkpoint::State,
    error::invalid,
    protocol::mysql::{
        file::EventView,
        version::{VersionAdapter, VersionRegistry},
    },
    ColumnMeta, Error, Result, TableMeta,
};
use ::mysql::{params, prelude::Queryable, Conn};
use mysql_replication::stream::{ConnectionConfig, ReplicationConfig, ReplicationStream, RowImage};
use std::{collections::BTreeMap, sync::Arc};

pub struct MysqlSource;
impl SourcePlugin for MysqlSource {
    fn name(&self) -> &str {
        "mysql"
    }
    fn connect(
        &self,
        config: &Config,
        versions: &VersionRegistry,
    ) -> Result<Box<dyn SourceSession>> {
        let mut conn = connect(config)?;
        let (uuid, version, adapter) = inspect_server(
            &mut conn,
            config.source.server_id,
            versions,
            config.source.adapter.as_deref(),
        )?;
        let info = ServerInfo {
            uuid,
            version,
            adapter: adapter.name().into(),
        };
        Ok(Box::new(MysqlSession {
            conn,
            info,
            adapter,
        }))
    }
}
struct MysqlSession {
    conn: Conn,
    info: ServerInfo,
    adapter: Arc<dyn VersionAdapter>,
}
impl SourceSession for MysqlSession {
    fn info(&self) -> &ServerInfo {
        &self.info
    }
    fn load_tables(&mut self, config: &Config) -> Result<Vec<TableMeta>> {
        load_tables(&mut self.conn, config)
    }
    fn initialize(&mut self, source_id: &str, config: &Config) -> Result<State> {
        initialize_with_adapter(
            &mut self.conn,
            source_id,
            config,
            self.info.uuid.clone(),
            self.adapter.as_ref(),
        )
    }
    fn start(
        self: Box<Self>,
        config: &Config,
        state: &State,
        once: bool,
    ) -> Result<Box<dyn EventStream>> {
        let mut replication =
            ReplicationConfig::new(config.source.server_id, &state.file, state.position);
        replication.non_blocking = once;
        replication.required_row_image = Some(RowImage::Minimal);
        replication.parser = mysql_replication::parser::ParserConfig::minimal();
        Ok(Box::new(MysqlEvents {
            stream: ReplicationStream::from_connection(self.conn, replication)?,
        }))
    }
}
struct MysqlEvents {
    stream: ReplicationStream,
}
impl EventStream for MysqlEvents {
    fn next_event(&mut self) -> Result<Option<EventView<'_>>> {
        Ok(self.stream.next_view()?.map(EventView::from))
    }
}
pub fn connect(config: &Config) -> Result<Conn> {
    let mut connection = ConnectionConfig::new(&config.source.url_env);
    connection.tls_ca = config.source.tls_ca.clone();
    Ok(mysql_replication::stream::connect(&connection)?)
}

pub fn inspect_server(
    conn: &mut Conn,
    server_id: u32,
    registry: &crate::protocol::mysql::version::VersionRegistry,
    selected: Option<&str>,
) -> Result<(
    String,
    crate::protocol::mysql::version::ServerVersion,
    std::sync::Arc<dyn crate::protocol::mysql::version::VersionAdapter>,
)> {
    let raw: String = conn
        .query_first("SELECT VERSION()")?
        .ok_or_else(|| invalid("no VERSION()"))?;
    let version = crate::protocol::mysql::version::ServerVersion::parse(&raw)?;
    let adapter = registry.resolve(&version, selected)?;
    let variables: Vec<(String, String)> = conn.query("SHOW GLOBAL VARIABLES WHERE Variable_name IN ('log_bin','binlog_format','binlog_row_image','binlog_transaction_compression','binlog_row_value_options','server_id')")?;
    adapter.validate_server(
        &variables.into_iter().collect::<BTreeMap<_, _>>(),
        server_id,
    )?;
    let uuid = conn
        .query_first("SELECT @@server_uuid")?
        .ok_or_else(|| invalid("no server_uuid"))?;
    Ok((uuid, version, adapter))
}
pub fn load_tables(conn: &mut Conn, config: &Config) -> Result<Vec<TableMeta>> {
    let mut tables = Vec::with_capacity(config.tables.len());
    for table in &config.tables {
        let params = params! { "schema" => &table.schema, "table" => &table.table };
        let engine: Option<String> = conn.exec_first("SELECT ENGINE FROM information_schema.TABLES WHERE TABLE_SCHEMA=:schema AND TABLE_NAME=:table", params.clone())?;
        if engine.as_deref() != Some("InnoDB") {
            return Err(Error::Unsupported(format!(
                "{}.{} must exist and use InnoDB",
                table.schema, table.table
            )));
        }
        let rows: Vec<mysql::Row> = conn.exec("SELECT COLUMN_NAME, DATA_TYPE, COLUMN_TYPE, IS_NULLABLE, COLUMN_KEY, CHARACTER_SET_NAME, CHARACTER_OCTET_LENGTH, NUMERIC_PRECISION, NUMERIC_SCALE, DATETIME_PRECISION, EXTRA FROM information_schema.COLUMNS WHERE TABLE_SCHEMA=:schema AND TABLE_NAME=:table ORDER BY ORDINAL_POSITION", params)?;
        let mut columns = Vec::with_capacity(rows.len());
        for row in rows {
            type InfoRow = (
                String,
                String,
                String,
                String,
                String,
                Option<String>,
                Option<u64>,
                Option<u32>,
                Option<u32>,
                Option<u32>,
                String,
            );
            let (
                name,
                data_type,
                column_type,
                nullable,
                key,
                charset,
                octet_length,
                numeric_precision,
                numeric_scale,
                datetime_precision,
                extra,
            ): InfoRow = mysql::from_row_opt(row).map_err(|e| Error::Schema(e.to_string()))?;
            columns.push(ColumnMeta {
                name,
                data_type,
                column_type,
                nullable: nullable == "YES",
                primary_key: key == "PRI",
                charset,
                octet_length,
                numeric_precision,
                numeric_scale,
                datetime_precision,
                extra,
            });
        }
        let meta = TableMeta {
            schema: table.schema.clone(),
            table: table.table.clone(),
            columns,
        };
        meta.validate()?;
        tables.push(meta);
    }
    Ok(tables)
}

/// Capture source identity, schema and commit boundary under a brief global read lock.
fn initialize_with_adapter(
    conn: &mut Conn,
    source_id: &str,
    config: &Config,
    source_uuid: String,
    adapter: &dyn crate::protocol::mysql::version::VersionAdapter,
) -> Result<State> {
    conn.query_drop("FLUSH TABLES WITH READ LOCK")?;
    let result: Result<State> = (|| {
        let tables = load_tables(conn, config)?;
        let rows: Vec<mysql::Row> = conn.query(adapter.status_query())?;
        let row = rows
            .first()
            .ok_or_else(|| invalid("no binary log status"))?;
        let file = row
            .get_opt::<String, _>(0)
            .ok_or_else(|| invalid("missing log file"))?
            .map_err(|e| invalid(e.to_string()))?;
        let position = row
            .get_opt::<u64, _>(1)
            .ok_or_else(|| invalid("missing log position"))?
            .map_err(|e| invalid(e.to_string()))?;
        let state = State {
            version: 1,
            source_uuid,
            source_id: source_id.into(),
            source_plugin: config.source.plugin.clone(),
            file,
            position,
            tables,
        };
        state.validate()?;
        Ok(state)
    })();
    let unlock = conn.query_drop("UNLOCK TABLES");
    let state = result?;
    unlock?;
    Ok(state)
}
