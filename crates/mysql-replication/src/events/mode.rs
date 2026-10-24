//! Compile-time policies for protocol event envelopes.
use super::{Full, Minimal, RowImages, RowsEvent};
use crate::{Error, Result};
use std::fmt::Debug;

mod sealed {
    pub trait Sealed {}
}

/// An unclassified wire event, preserving the original deferred parsing API.
#[derive(Debug, Clone, Copy)]
pub struct Unspecified;

/// A sealed policy carried by the parser, event envelope and ROWS payload.
/// Common control events retain the same representation for every policy.
pub trait BinlogMode: sealed::Sealed + Debug + Clone + Copy {
    #[doc(hidden)]
    fn validate_event(event: &RowsEvent<'_, Self>) -> Result<()>;
    #[doc(hidden)]
    fn validate_images(images: &RowImages<'_>) -> Result<()>;
}

impl sealed::Sealed for Unspecified {}
impl BinlogMode for Unspecified {
    fn validate_event(_: &RowsEvent<'_, Self>) -> Result<()> {
        Ok(())
    }
    fn validate_images(_: &RowImages<'_>) -> Result<()> {
        Ok(())
    }
}

impl sealed::Sealed for Minimal {}
impl BinlogMode for Minimal {
    fn validate_event(_: &RowsEvent<'_, Self>) -> Result<()> {
        Ok(())
    }
    fn validate_images(_: &RowImages<'_>) -> Result<()> {
        Ok(())
    }
}

impl sealed::Sealed for Full {}
impl BinlogMode for Full {
    fn validate_event(event: &RowsEvent<'_, Self>) -> Result<()> {
        // This sentinel ends a statement and carries no row images.
        if event.table_id == 0x00ff_ffff && event.flags & 1 != 0 {
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
