#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("source failure: {0}")]
    Sources(String),
    #[error("sink failure: {0}")]
    Sink(String),
    #[error("truncated input: need {need} bytes, have {have}")]
    Truncated { need: usize, have: usize },
    #[error("invalid binlog: {0}")]
    Invalid(String),
    #[error("unsupported: {0}")]
    Unsupported(String),
    #[error("schema mismatch: {0}")]
    Schema(String),
    #[error("missing or NULL primary key")]
    MissingPrimaryKey,
    #[error("table id {0} has no TABLE_MAP in this statement")]
    MissingTable(u64),
    #[error("CRC32 checksum mismatch")]
    Checksum,
    #[error("transaction exceeds configured {0}-byte memory budget")]
    TransactionLimit(usize),
    #[error("DDL/statement encountered; stop and migrate the schema snapshot: {0}")]
    Statement(String),
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    Mysql(#[from] mysql::Error),
    #[error(transparent)]
    Json(#[from] serde_json::Error),
    #[error(transparent)]
    Toml(#[from] toml::de::Error),
    #[error(transparent)]
    Utf8(#[from] std::str::Utf8Error),
}

pub type Result<T> = std::result::Result<T, Error>;

pub(crate) fn invalid(message: impl Into<String>) -> Error {
    Error::Invalid(message.into())
}

impl From<mysql_replication::Error> for Error {
    fn from(error: mysql_replication::Error) -> Self {
        match error {
            mysql_replication::Error::Truncated { need, have } => Self::Truncated { need, have },
            mysql_replication::Error::Invalid(message) => Self::Invalid(message),
            mysql_replication::Error::Unsupported(message) => Self::Unsupported(message),
            mysql_replication::Error::Checksum => Self::Checksum,
            mysql_replication::Error::Io(error) => Self::Io(error),
            mysql_replication::Error::Mysql(error) => Self::Mysql(error),
            mysql_replication::Error::Utf8(error) => Self::Utf8(error),
        }
    }
}
