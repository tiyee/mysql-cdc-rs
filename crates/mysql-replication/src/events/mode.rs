//! Compile-time policies for protocol event envelopes.
use super::{CompleteImage, RowImage, RowImages, RowsEvent, SparseImage};
use crate::config::RowImageMode;
use crate::{Error, Result};
use std::fmt::Debug;

mod sealed {
    pub trait Sealed {}
}

/// Sparse images preserve exactly the columns present in the log.
#[derive(Debug, Clone, Copy)]
pub struct Minimal;
/// Every existing image contains every table column.
#[derive(Debug, Clone, Copy)]
pub struct Full;

/// A sealed policy carried by the parser, event envelope and ROWS payload.
/// Common control events retain the same representation for every policy.
pub trait BinlogMode: sealed::Sealed + Debug + Clone + Copy + Send + Sync + 'static {
    type Image<'a>: TryFrom<RowImage<'a>, Error = Error> + AsRef<RowImage<'a>> + Debug + Clone;
    const ROW_IMAGE: RowImageMode;
    #[doc(hidden)]
    fn validate_event(event: &RowsEvent<'_, Self>) -> Result<()>;
    #[doc(hidden)]
    fn validate_images(images: &RowImages<'_>) -> Result<()>;
}

impl sealed::Sealed for Minimal {}
impl BinlogMode for Minimal {
    type Image<'a> = SparseImage<'a>;
    const ROW_IMAGE: RowImageMode = RowImageMode::Minimal;
    fn validate_event(_: &RowsEvent<'_, Self>) -> Result<()> {
        Ok(())
    }
    fn validate_images(_: &RowImages<'_>) -> Result<()> {
        Ok(())
    }
}

impl sealed::Sealed for Full {}
impl BinlogMode for Full {
    type Image<'a> = CompleteImage<'a>;
    const ROW_IMAGE: RowImageMode = RowImageMode::Full;
    fn validate_event(event: &RowsEvent<'_, Self>) -> Result<()> {
        // This sentinel ends a statement and carries no row images.
        if event.is_statement_end() {
            return Ok(());
        }
        event.images().map(|_| ())
    }
    fn validate_images(images: &RowImages<'_>) -> Result<()> {
        let complete = |bitmap: &[u8]| {
            (0..images.column_count).all(|index| {
                bitmap
                    .get(index / 8)
                    .is_some_and(|byte| byte & (1 << (index % 8)) != 0)
            })
        };
        if !complete(images.before_columns) || !complete(images.after_columns) {
            return Err(Error::Unsupported(
                "FULL row images must contain every table column".into(),
            ));
        }
        Ok(())
    }
}
