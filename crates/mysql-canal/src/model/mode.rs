//! Fixed row image modes with statically dispatched CDC validation.
use crate::{Error, Operation, Result};
pub use mysql_replication::events::{Full, Minimal};

/// A fixed image mode whose row validation uses static dispatch.
pub trait CdcImageMode: mysql_replication::BinlogMode {
    #[doc(hidden)]
    fn validate_image(
        operation: Operation,
        count: usize,
        before_has_key: bool,
        before: usize,
        after: usize,
    ) -> Result<()>;
}

impl CdcImageMode for Minimal {
    fn validate_image(
        operation: Operation,
        _: usize,
        before_has_key: bool,
        before: usize,
        _: usize,
    ) -> Result<()> {
        if operation != Operation::Insert && (before != 1 || !before_has_key) {
            return Err(Error::Unsupported(
                "expected MINIMAL before image containing only the primary key".into(),
            ));
        }
        Ok(())
    }
}

impl CdcImageMode for Full {
    fn validate_image(
        operation: Operation,
        count: usize,
        _: bool,
        before: usize,
        after: usize,
    ) -> Result<()> {
        if before != count || operation == Operation::Update && after != count {
            return Err(Error::Unsupported(
                "expected FULL row images containing every column".into(),
            ));
        }
        Ok(())
    }
}
