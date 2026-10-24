//! Image policies fixed at construction, with a configuration-driven entry point.
use crate::{
    Error, Operation, Result,
    config::{AppConfig, RowImageMode},
};
pub use mysql_replication::events::{Full, Minimal};

mod sealed {
    pub trait Sealed {}
}

/// Selects each source's policy from its required replication configuration.
#[derive(Debug, Clone, Copy)]
pub struct Configured;

#[doc(hidden)]
pub type ImageValidator = fn(Operation, usize, bool, usize, usize) -> Result<()>;

/// A sealed CDC policy. Fixed modes reject conflicting configuration at startup.
pub trait CdcMode: sealed::Sealed + Send + Sync + 'static {
    #[doc(hidden)]
    type Policy: Send;
    const REQUIRED_ROW_IMAGE: Option<RowImageMode>;
    #[doc(hidden)]
    fn policy(mode: RowImageMode) -> Result<Self::Policy>;
    #[doc(hidden)]
    fn validate_image(
        policy: &Self::Policy,
        operation: Operation,
        count: usize,
        before_has_key: bool,
        before: usize,
        after: usize,
    ) -> Result<()>;

    fn validate_config(config: &AppConfig) -> Result<()> {
        if let Some(expected) = Self::REQUIRED_ROW_IMAGE {
            for source in &config.sources {
                if source.config.replication.row_image != expected {
                    return Err(Error::Schema(format!(
                        "source {} row_image conflicts with generic {} mode",
                        source.id, expected
                    )));
                }
            }
        }
        Ok(())
    }
}

impl sealed::Sealed for Configured {}
impl CdcMode for Configured {
    type Policy = ImageValidator;
    const REQUIRED_ROW_IMAGE: Option<RowImageMode> = None;
    fn policy(mode: RowImageMode) -> Result<Self::Policy> {
        Ok(match mode {
            RowImageMode::Minimal => validate_minimal,
            RowImageMode::Full => validate_full,
        })
    }
    fn validate_image(
        policy: &Self::Policy,
        operation: Operation,
        count: usize,
        before_has_key: bool,
        before: usize,
        after: usize,
    ) -> Result<()> {
        policy(operation, count, before_has_key, before, after)
    }
}

macro_rules! fixed_mode {
    ($mode:ty, $value:expr, $validate:ident) => {
        impl sealed::Sealed for $mode {}
        impl CdcMode for $mode {
            type Policy = ();
            const REQUIRED_ROW_IMAGE: Option<RowImageMode> = Some($value);
            fn policy(mode: RowImageMode) -> Result<()> {
                if mode != $value {
                    return Err(Error::Schema(
                        "decoder configuration conflicts with generic image mode".into(),
                    ));
                }
                Ok(())
            }
            fn validate_image(
                _: &(),
                operation: Operation,
                count: usize,
                before_has_key: bool,
                before: usize,
                after: usize,
            ) -> Result<()> {
                $validate(operation, count, before_has_key, before, after)
            }
        }
    };
}
fixed_mode!(Minimal, RowImageMode::Minimal, validate_minimal);
fixed_mode!(Full, RowImageMode::Full, validate_full);

fn validate_minimal(
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
fn validate_full(
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
