//! Template only: replace series/version checks after validating the actual wire
//! format. Registration does not certify compatibility with an untested server.
use pikachu_cdc::{
    Result,
    config::AppConfig,
    engine::runtime::Runtime,
    protocol::mysql::{
        events::{EventParser, MysqlEventParser},
        version::{FormatDescription, ServerVersion, VersionAdapter},
    },
    sink::JsonLinesSink,
};
use std::{io::BufWriter, path::Path, sync::Arc};
struct CustomMysql;
impl VersionAdapter for CustomMysql {
    fn name(&self) -> &str {
        "custom-mysql"
    }
    fn supports(&self, version: &ServerVersion) -> bool {
        version.major == 9
            && version.minor == 1
            && !version.raw.to_ascii_lowercase().contains("mariadb")
    }
    fn status_query(&self) -> &str {
        "SHOW BINARY LOG STATUS"
    }
    fn create_event_parser(&self, format: &FormatDescription<'_>) -> Result<Box<dyn EventParser>> {
        // Reuse only after verifying that field order/endianness are unchanged.
        // For another event structure, return your own EventParser here. It
        // should return borrowed TableMapEvent/RowsEvent/Control views, without
        // rebuilding a canonical byte buffer. See tests/integration/event_adapters.rs.
        Ok(Box::new(MysqlEventParser::from_format(format)?))
    }
    // Override validate_server/validate_format for new capability/layout rules.
    // Override create_decoder if the column metadata or row value encoding also
    // changes. classify is the fallback for additional control event codes.
}
fn main() -> Result<()> {
    let Some(path) = std::env::args().nth(1) else {
        eprintln!("usage: cargo run --example custom_version -- CONFIG.toml");
        return Ok(());
    };
    let mut runtime = Runtime::default();
    runtime.versions.register(Arc::new(CustomMysql))?;
    runtime.run(
        AppConfig::read(Path::new(&path))?,
        false,
        Arc::new(JsonLinesSink::new(BufWriter::new(std::io::stdout()))),
    )
}
