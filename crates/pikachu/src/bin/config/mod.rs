//! Product configuration composes canal without repeating its lower-layer fields.
use mysql_canal::{Result, config::AppConfig};
use serde::Deserialize;
use std::path::Path;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    #[serde(rename = "mysql-canal")]
    pub canal: AppConfig,
}

impl Config {
    pub fn read(path: &Path) -> Result<Self> {
        let text = std::fs::read_to_string(path)?;
        let mut config: Self = toml::from_str(&text)?;
        config.canal.resolve_paths(path)?;
        Ok(config)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use mysql_canal::{Error, config::RowImageMode};
    use std::{
        fs,
        path::PathBuf,
        sync::atomic::{AtomicUsize, Ordering},
    };

    const EXAMPLE: &str = include_str!("../../../examples/config/multi-source.toml");

    struct Fixture(PathBuf);
    impl Fixture {
        fn new() -> Self {
            static NEXT_ID: AtomicUsize = AtomicUsize::new(0);
            let directory = std::env::temp_dir().join(format!(
                "pikachu-composed-config-{}-{}",
                std::process::id(),
                NEXT_ID.fetch_add(1, Ordering::Relaxed)
            ));
            fs::create_dir(&directory).unwrap();
            Self(directory)
        }
        fn read(&self, text: &str) -> Result<Config> {
            let path = self.0.join("pikachu.toml");
            fs::write(&path, text).unwrap();
            Config::read(&path)
        }
    }
    impl Drop for Fixture {
        fn drop(&mut self) {
            fs::remove_dir_all(&self.0).unwrap();
        }
    }

    #[test]
    fn product_contains_canal_and_replication_without_redefining_their_fields() {
        let fixture = Fixture::new();
        let text = EXAMPLE
            .replacen("row_image = \"minimal\"", "row_image = \"full\"", 1)
            .replacen(
                "# tls_ca = \"/path/to/ca.pem\"",
                "tls_ca = \"certs/ca.pem\"",
                1,
            );
        let config = fixture.read(&text).unwrap();
        let canal: &AppConfig = &config.canal;
        assert_eq!(canal.sources.len(), 2);
        let east = &canal.sources[0];
        let replication: &mysql_canal::config::ReplicationConfig = &east.config.replication;
        assert_eq!(east.id, "east");
        assert_eq!(east.config.source.plugin, "mysql");
        assert_eq!(east.config.state_file, fixture.0.join("east-state.json"));
        assert_eq!(east.config.tables.len(), 2);
        assert_eq!(replication.url_env, "MYSQL_EAST_URL");
        assert_eq!(replication.server_id, 901001);
        assert_eq!(replication.row_image, RowImageMode::Full);
        assert_eq!(replication.tls_ca, Some(fixture.0.join("certs/ca.pem")));
        let west = &canal.sources[1];
        assert_eq!(west.config.replication.row_image, RowImageMode::Minimal);
        assert_eq!(west.config.replication.server_id, 901002);
        assert_eq!(west.config.state_file, fixture.0.join("west-state.json"));
        assert!(!east.config.state_file.exists());
        assert!(!west.config.state_file.exists());
    }

    #[test]
    fn required_child_configs_and_layer_ownership_are_strict() {
        let fixture = Fixture::new();
        let standalone = include_str!("../../../../mysql-canal/examples/config/multi-source.toml");
        assert!(matches!(fixture.read(""), Err(Error::Toml(error))
            if error.to_string().contains("missing field `mysql-canal`")));
        assert!(matches!(fixture.read(standalone), Err(Error::Toml(error))
            if error.to_string().contains("unknown field `sources`")));
        assert!(
            matches!(fixture.read("[mysql-canal]\n"), Err(Error::Toml(error))
            if error.to_string().contains("missing field `sources`"))
        );
        for text in [
            EXAMPLE.replace("mysql-replication", "replication"),
            EXAMPLE.replace("mysql-canal", "canal"),
            format!("server_id = 1\n{EXAMPLE}"),
            EXAMPLE.replace("[mysql-canal]\n", "[mysql-canal]\nrow_image = \"full\"\n"),
            EXAMPLE.replace(
                "[mysql-canal.sources.source]\n",
                "[mysql-canal.sources.source]\nurl_env = \"MISPLACED_URL\"\n",
            ),
            EXAMPLE.replace(
                "server_id = 901001",
                "server_id = 901001\nstate_file = \"misplaced.json\"",
            ),
        ] {
            assert!(matches!(fixture.read(&text), Err(Error::Toml(_))), "{text}");
        }
    }

    #[test]
    fn embedded_canal_keeps_its_validation_and_standalone_format() {
        let fixture = Fixture::new();
        let duplicate = EXAMPLE.replace("west-state.json", "./east-state.json");
        assert!(
            matches!(fixture.read(&duplicate), Err(Error::Schema(message))
            if message == "sources must use different state files")
        );
        let invalid_replication = EXAMPLE.replace("server_id = 901001", "server_id = 0");
        assert!(
            matches!(fixture.read(&invalid_replication), Err(Error::Invalid(message))
            if message == "replication requires nonempty url_env and nonzero server_id")
        );
        let path = fixture.0.join("canal.toml");
        fs::write(
            &path,
            include_str!("../../../../mysql-canal/examples/config/multi-source.toml"),
        )
        .unwrap();
        assert_eq!(AppConfig::read(&path).unwrap().sources.len(), 2);
        fs::write(&path, EXAMPLE).unwrap();
        assert!(matches!(AppConfig::read(&path), Err(Error::Toml(_))));
    }
}
