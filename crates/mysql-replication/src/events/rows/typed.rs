//! Typed, borrowed image views over the common ROWS wire layout.
use super::{
    ColumnDefinition, RowFields, RowImage, RowValue, Rows, RowsDecoder, RowsEvent, RowsKind,
};
use crate::{
    Error, Result,
    config::RowImageMode,
    error::invalid,
    events::{BinlogMode, TableMapEvent},
};
use std::{fmt::Debug, iter::FusedIterator, marker::PhantomData};

mod sealed {
    pub trait Sealed {}
}

/// A supported image policy with its own borrowed image representation.
///
/// This trait is sealed so that a typed FULL event cannot bypass completeness
/// validation. Modes describe accepted shapes, not a producer setting inferred
/// from the bytes. MINIMAL may contain all columns and does not require a key.
pub trait ImageMode: sealed::Sealed + BinlogMode + Send + Sync + 'static {
    type Image<'a>: TryFrom<RowImage<'a>, Error = Error> + AsRef<RowImage<'a>> + Debug + Clone;
    #[doc(hidden)]
    const REQUIRE_COMPLETE: bool;
    const ROW_IMAGE: RowImageMode;
    #[doc(hidden)]
    fn validate_rows(rows: &Rows<'_>) -> Result<()>;
}

/// Sparse images retain only the columns actually present in the binlog.
#[derive(Debug, Clone, Copy)]
pub struct Minimal;
impl sealed::Sealed for Minimal {}
impl ImageMode for Minimal {
    type Image<'a> = SparseImage<'a>;
    const REQUIRE_COMPLETE: bool = false;
    const ROW_IMAGE: RowImageMode = RowImageMode::Minimal;
    fn validate_rows(_: &Rows<'_>) -> Result<()> {
        Ok(())
    }
}

/// Every column must be present in every existing image.
#[derive(Debug, Clone, Copy)]
pub struct Full;
impl sealed::Sealed for Full {}
impl ImageMode for Full {
    type Image<'a> = CompleteImage<'a>;
    const REQUIRE_COMPLETE: bool = true;
    const ROW_IMAGE: RowImageMode = RowImageMode::Full;
    fn validate_rows(rows: &Rows<'_>) -> Result<()> {
        rows.require_complete()
    }
}

/// A borrowed sparse image. A missing field is distinct from SQL NULL.
///
/// An image cannot outlive its input bytes, even after its decoder is dropped.
///
/// ```compile_fail
/// use mysql_replication::events::SparseImage;
/// fn retain_forever<'a>(image: SparseImage<'a>) -> SparseImage<'static> {
///     image
/// }
/// ```
#[derive(Debug, Clone)]
pub struct SparseImage<'a> {
    image: RowImage<'a>,
}
impl<'a> SparseImage<'a> {
    pub fn column_count(&self) -> usize {
        self.image.column_count()
    }
    pub fn column_indices(&self) -> &[u16] {
        self.image.column_indices()
    }
    pub fn fields(&self) -> RowFields<'_, 'a> {
        self.image.fields()
    }
    /// Reads through present fields up to this absolute index, without allocating.
    /// Returns `None` for an absent column, including an out-of-range index.
    pub fn field(&self, column_index: usize) -> Result<Option<RowValue<'a>>> {
        field(&self.image, column_index)
    }
}
impl<'a> TryFrom<RowImage<'a>> for SparseImage<'a> {
    type Error = Error;
    fn try_from(image: RowImage<'a>) -> Result<Self> {
        Ok(Self { image })
    }
}
impl<'a> AsRef<RowImage<'a>> for SparseImage<'a> {
    fn as_ref(&self) -> &RowImage<'a> {
        &self.image
    }
}

/// A borrowed image whose columns have all been validated as present.
///
/// Construction is checked; converting a sparse prefix cannot claim completeness.
/// This does not allocate a full row or interpret SQL signedness/character sets.
#[derive(Debug, Clone)]
pub struct CompleteImage<'a> {
    image: RowImage<'a>,
}
impl<'a> CompleteImage<'a> {
    pub fn column_count(&self) -> usize {
        self.image.column_count()
    }
    pub fn fields(&self) -> RowFields<'_, 'a> {
        self.image.fields()
    }
    /// Reads through fields up to this absolute index, without allocating.
    /// Every in-range column exists; SQL NULL is `RowValue::Null`.
    pub fn field(&self, column_index: usize) -> Result<RowValue<'a>> {
        if column_index >= self.column_count() {
            return Err(invalid("column index outside complete row image"));
        }
        field(&self.image, column_index)?.ok_or_else(|| invalid("incomplete FULL row image"))
    }
}
impl<'a> TryFrom<RowImage<'a>> for CompleteImage<'a> {
    type Error = Error;
    fn try_from(image: RowImage<'a>) -> Result<Self> {
        if image.column_indices().len() != image.column_count() {
            return Err(Error::Unsupported(
                "FULL row images must contain every table column".into(),
            ));
        }
        Ok(Self { image })
    }
}
impl<'a> AsRef<RowImage<'a>> for CompleteImage<'a> {
    fn as_ref(&self) -> &RowImage<'a> {
        &self.image
    }
}

fn field<'a>(image: &RowImage<'a>, index: usize) -> Result<Option<RowValue<'a>>> {
    for field in image.fields() {
        let field = field?;
        let column_index = usize::from(field.column_index);
        if column_index == index {
            return Ok(Some(field.value));
        }
        if column_index > index {
            break;
        }
    }
    Ok(None)
}

/// One operation with mode-specific image types. Missing images remain `None`.
///
/// The modes have distinct image types even when their bitmaps happen to match.
///
/// ```compile_fail
/// use mysql_replication::events::{Full, Minimal, TypedRow};
/// fn sparse(row: TypedRow<'_, Full>) -> TypedRow<'_, Minimal> {
///     row
/// }
/// ```
#[derive(Debug, Clone)]
pub struct TypedRow<'a, M: ImageMode> {
    /// Before image for UPDATE/DELETE; `None` for INSERT. The image is sparse
    /// under `Minimal` and complete under `Full`; SQL NULL is a field value.
    pub before: Option<M::Image<'a>>,
    /// After image for INSERT/UPDATE; `None` for DELETE, with the same mode's
    /// image type as `before`.
    pub after: Option<M::Image<'a>>,
}

/// A ROWS event with checked image shape and an owned TABLE_MAP execution plan.
///
/// Private fields prevent bypassing the mode check. Values still borrow the
/// input and are validated while iterating; a malformed row never yields a
/// partially decoded operation. The source decoder can be dropped immediately.
pub struct TypedRowsEvent<'a, M: ImageMode> {
    event: RowsEvent<'a>,
    rows: Rows<'a>,
    column_count: usize,
    mode: PhantomData<M>,
}
impl<'a, M: ImageMode> TypedRowsEvent<'a, M> {
    pub(super) fn new(event: RowsEvent<'a>, decoder: &RowsDecoder) -> Result<Self> {
        let rows = decoder.rows(&event)?;
        M::validate_rows(&rows)?;
        Ok(Self {
            event,
            rows,
            column_count: decoder.columns().len(),
            mode: PhantomData,
        })
    }
    pub fn table_id(&self) -> u64 {
        self.event.table_id
    }
    pub fn flags(&self) -> u16 {
        self.event.flags
    }
    pub fn kind(&self) -> RowsKind {
        self.event.kind
    }
    pub fn column_count(&self) -> usize {
        self.column_count
    }
    pub fn rows(self) -> TypedRows<'a, M> {
        TypedRows {
            rows: self.rows,
            done: false,
            mode: PhantomData,
        }
    }
}

/// A mode-specific streaming iterator. It yields one error and then stops.
pub struct TypedRows<'a, M: ImageMode> {
    rows: Rows<'a>,
    done: bool,
    mode: PhantomData<M>,
}
impl<'a, M: ImageMode> Iterator for TypedRows<'a, M> {
    type Item = Result<TypedRow<'a, M>>;
    fn next(&mut self) -> Option<Self::Item> {
        if self.done {
            return None;
        }
        let result = self.rows.next()?.and_then(|row| {
            Ok(TypedRow {
                before: row.before.map(M::Image::try_from).transpose()?,
                after: row.after.map(M::Image::try_from).transpose()?,
            })
        });
        self.done = result.is_err();
        Some(result)
    }
}
impl<M: ImageMode> FusedIterator for TypedRows<'_, M> {}

/// A reusable generic decoder, bound to a TABLE_MAP and an image policy.
#[derive(Debug)]
pub struct TypedRowsDecoder<M: ImageMode> {
    decoder: RowsDecoder,
    mode: PhantomData<M>,
}
impl<M: ImageMode> TypedRowsDecoder<M> {
    pub fn new(table: &TableMapEvent<'_>) -> Result<Self> {
        RowsDecoder::new(table).map(Self::from)
    }
    pub fn table_id(&self) -> u64 {
        self.decoder.table_id()
    }
    pub fn columns(&self) -> &[ColumnDefinition] {
        self.decoder.columns()
    }
    pub fn event<'a>(&self, event: &RowsEvent<'a>) -> Result<TypedRowsEvent<'a, M>> {
        event.with_mode::<M>(&self.decoder)
    }
    pub fn rows<'a>(&self, event: &RowsEvent<'a>) -> Result<TypedRows<'a, M>> {
        self.event(event).map(TypedRowsEvent::rows)
    }
}
impl<M: ImageMode> From<RowsDecoder> for TypedRowsDecoder<M> {
    fn from(decoder: RowsDecoder) -> Self {
        Self {
            decoder,
            mode: PhantomData,
        }
    }
}
