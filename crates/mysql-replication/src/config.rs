//! Reusable replication settings; file positions are supplied by the caller.

use crate::{Error, Result, error::invalid, stream};
use serde::Deserialize;
use std::{fmt, path::PathBuf, str::FromStr, time::Duration};

/// Required upstream row-image mode. Both modes support borrowed row decoding.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum RowImageMode {
    #[default]
    Minimal,
    Full,
}

impl RowImageMode {
    pub fn server_value(self) -> &'static str {
        match self {
            Self::Minimal => "MINIMAL",
            Self::Full => "FULL",
        }
    }
}

impl fmt::Display for RowImageMode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Minimal => "minimal",
            Self::Full => "full",
        })
    }
}

impl FromStr for RowImageMode {
    type Err = Error;

    fn from_str(value: &str) -> Result<Self> {
        match value {
            "minimal" => Ok(Self::Minimal),
            "full" => Ok(Self::Full),
            _ => Err(invalid("row_image must be minimal or full")),
        }
    }
}

impl From<RowImageMode> for stream::RowImage {
    fn from(mode: RowImageMode) -> Self {
        match mode {
            RowImageMode::Minimal => Self::Minimal,
            RowImageMode::Full => Self::Full,
        }
    }
}

/// Connection and subscription settings shared by direct and CDC consumers.
/// Credentials remain in the named environment variable. No CDC table/schema,
/// transaction, checkpoint or output policy is part of this configuration.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub url_env: String,
    pub server_id: u32,
    pub tls_ca: Option<PathBuf>,
    #[serde(default)]
    pub row_image: RowImageMode,
    #[serde(default = "connect_timeout_secs")]
    pub connect_timeout_secs: u64,
    #[serde(default = "read_timeout_secs")]
    pub read_timeout_secs: u64,
}

fn connect_timeout_secs() -> u64 {
    10
}

fn read_timeout_secs() -> u64 {
    90
}

impl Config {
    pub fn new(url_env: impl Into<String>, server_id: u32) -> Self {
        Self {
            url_env: url_env.into(),
            server_id,
            tls_ca: None,
            row_image: RowImageMode::Minimal,
            connect_timeout_secs: connect_timeout_secs(),
            read_timeout_secs: read_timeout_secs(),
        }
    }

    pub fn validate(&self) -> Result<()> {
        if self.url_env.is_empty() || self.server_id == 0 {
            return Err(invalid(
                "replication requires nonempty url_env and nonzero server_id",
            ));
        }
        if self.connect_timeout_secs == 0 || self.read_timeout_secs == 0 {
            return Err(invalid("replication timeouts must be nonzero"));
        }
        Ok(())
    }

    pub fn connection_config(&self) -> Result<stream::ConnectionConfig> {
        self.validate()?;
        Ok(stream::ConnectionConfig {
            url_env: self.url_env.clone(),
            tls_ca: self.tls_ca.clone(),
            connect_timeout: Duration::from_secs(self.connect_timeout_secs),
            read_timeout: Duration::from_secs(self.read_timeout_secs),
        })
    }

    /// Binds reusable settings to a caller-supplied binlog position.
    /// This validates upstream mode at stream startup without changing MySQL.
    pub fn replication_config(
        &self,
        file: impl Into<String>,
        position: u64,
    ) -> Result<stream::ReplicationConfig> {
        self.validate()?;
        let mut config = stream::ReplicationConfig::new(self.server_id, file, position);
        config.required_row_image = Some(self.row_image.into());
        Ok(config)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reusable_config_binds_both_modes_and_connection_settings() {
        let mut config = Config::new("MYSQL_URL", 101);
        config.tls_ca = Some("certs/ca.pem".into());
        config.connect_timeout_secs = 4;
        config.read_timeout_secs = 30;
        assert_eq!(config.row_image, RowImageMode::Minimal);
        for (mode, expected) in [
            (RowImageMode::Minimal, stream::RowImage::Minimal),
            (RowImageMode::Full, stream::RowImage::Full),
        ] {
            config.row_image = mode;
            let connection = config.connection_config().unwrap();
            assert_eq!(connection.url_env, "MYSQL_URL");
            assert_eq!(connection.tls_ca, Some("certs/ca.pem".into()));
            assert_eq!(connection.connect_timeout, Duration::from_secs(4));
            assert_eq!(connection.read_timeout, Duration::from_secs(30));
            let subscription = config.replication_config("binlog.000007", 1234).unwrap();
            assert_eq!(subscription.server_id, 101);
            assert_eq!(subscription.file, "binlog.000007");
            assert_eq!(subscription.position, 1234);
            assert_eq!(subscription.required_row_image, Some(expected));
            assert!(!subscription.non_blocking);
        }
    }

    #[test]
    fn invalid_settings_fail_before_connection_or_subscription() {
        let mut missing_id = Config::new("MYSQL_URL", 0);
        let mut missing_url = Config::new("", 101);
        for config in [&mut missing_id, &mut missing_url] {
            assert!(
                matches!(config.connection_config(), Err(Error::Invalid(message))
                if message == "replication requires nonempty url_env and nonzero server_id")
            );
            assert!(
                matches!(config.replication_config("binlog.000001", 4), Err(Error::Invalid(message))
                if message == "replication requires nonempty url_env and nonzero server_id")
            );
        }
        for (connect, read) in [(0, 90), (10, 0)] {
            let mut config = Config::new("MYSQL_URL", 101);
            config.connect_timeout_secs = connect;
            config.read_timeout_secs = read;
            assert!(
                matches!(config.connection_config(), Err(Error::Invalid(message))
                if message == "replication timeouts must be nonzero")
            );
        }
    }
}
