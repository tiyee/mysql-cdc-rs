use std::io;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("truncated input: need {need} bytes, have {have}")]
    Truncated { need: usize, have: usize },
    #[error("invalid binlog: {0}")]
    Invalid(String),
    #[error("unsupported: {0}")]
    Unsupported(String),
    #[error("CRC32 checksum mismatch")]
    Checksum,
    #[error(transparent)]
    Io(#[from] io::Error),
    #[error(transparent)]
    Mysql(#[from] mysql::Error),
    #[error(transparent)]
    Utf8(#[from] std::str::Utf8Error),
}
pub type Result<T> = std::result::Result<T, Error>;
pub(crate) fn invalid(message: impl Into<String>) -> Error {
    Error::Invalid(message.into())
}
