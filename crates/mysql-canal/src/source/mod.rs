//! Source plugin interfaces and registry; concrete transports live in submodules.
pub mod mysql;

use crate::{
    Error, Result, TableMeta,
    config::Config,
    protocol::mysql::{
        file::EventView,
        version::{ServerVersion, VersionRegistry},
    },
};
use std::sync::Arc;

use crate::engine::checkpoint::State;
pub use mysql::MysqlSource;

#[derive(Debug, Clone)]
pub struct ServerInfo {
    pub uuid: String,
    pub version: ServerVersion,
    pub adapter: String,
}
pub trait EventStream: Send {
    /// Borrowed until the next call; no event-body copy at the trait boundary.
    fn next_event(&mut self) -> Result<Option<EventView<'_>>>;
}
pub trait SourceSession: Send {
    fn info(&self) -> &ServerInfo;
    fn load_tables(&mut self, config: &Config) -> Result<Vec<TableMeta>>;
    /// Capture a checkpoint with this ID, the configured plugin and the server UUID.
    fn initialize(&mut self, source_id: &str, config: &Config) -> Result<State>;
    fn start(
        self: Box<Self>,
        config: &Config,
        state: &State,
        once: bool,
    ) -> Result<Box<dyn EventStream>>;
}
pub trait SourcePlugin: Send + Sync {
    fn name(&self) -> &str;
    fn connect(
        &self,
        config: &Config,
        versions: &VersionRegistry,
    ) -> Result<Box<dyn SourceSession>>;
}
#[derive(Clone)]
pub struct SourceRegistry {
    plugins: Vec<Arc<dyn SourcePlugin>>,
}
impl SourceRegistry {
    pub fn empty() -> Self {
        Self {
            plugins: Vec::new(),
        }
    }
    pub fn register(&mut self, plugin: Arc<dyn SourcePlugin>) -> Result<()> {
        if plugin.name().is_empty() || self.plugins.iter().any(|p| p.name() == plugin.name()) {
            return Err(Error::Schema("empty/duplicate source plugin name".into()));
        }
        self.plugins.push(plugin);
        Ok(())
    }
    pub fn resolve(&self, name: &str) -> Result<Arc<dyn SourcePlugin>> {
        self.plugins
            .iter()
            .find(|p| p.name() == name)
            .cloned()
            .ok_or_else(|| Error::Unsupported(format!("source plugin {name}")))
    }
}
impl Default for SourceRegistry {
    fn default() -> Self {
        Self {
            plugins: vec![Arc::new(MysqlSource)],
        }
    }
}
