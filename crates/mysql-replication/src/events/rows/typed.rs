//! Typed, borrowed image views over the common ROWS wire layout.
use super::{ColumnInfo, Fields, RowImage, RowValue, TableDefinition, decoder::DecodedRows};
use crate::{Error, Result, error::invalid, events::BinlogMode};
use std::{iter::FusedIterator, marker::PhantomData};

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
    pub fn table(&self) -> &TableDefinition {
        self.image.table()
    }
    pub fn columns(&self) -> &[ColumnInfo] {
        self.image.columns()
    }
    pub fn fields(&self) -> Fields<'_, 'a> {
        self.image.fields()
    }
    /// Reads through present fields up to this absolute index, without allocating.
    /// Returns `None` only for an absent column; out-of-range indices are errors.
    pub fn field(&self, column_index: usize) -> Result<Option<Field<'_, 'a>>> {
        field(&self.image, column_index)
    }
}
impl<'a> TryFrom<RowImage<'a>> for SparseImage<'a> {
    type Error = Error;
    fn try_from(image: RowImage<'a>) -> Result<Self> {
        Ok(Self { image })
    }
}
impl<'a> From<SparseImage<'a>> for RowImage<'a> {
    fn from(image: SparseImage<'a>) -> Self {
        image.image
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
    pub fn table(&self) -> &TableDefinition {
        self.image.table()
    }
    pub fn columns(&self) -> &[ColumnInfo] {
        self.image.columns()
    }
    pub fn fields(&self) -> Fields<'_, 'a> {
        self.image.fields()
    }
    /// Reads through fields up to this absolute index, without allocating.
    /// Every in-range column exists; SQL NULL is `RowValue::Null`.
    pub fn field(&self, column_index: usize) -> Result<Field<'_, 'a>> {
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
impl<'a> From<CompleteImage<'a>> for RowImage<'a> {
    fn from(image: CompleteImage<'a>) -> Self {
        image.image
    }
}
impl<'a> AsRef<RowImage<'a>> for CompleteImage<'a> {
    fn as_ref(&self) -> &RowImage<'a> {
        &self.image
    }
}

/// A present field, borrowing its column definition and original event value.
///
/// ```compile_fail
/// use mysql_replication::events::{CompleteImage, Field};
/// fn retain_metadata<'a>(image: CompleteImage<'a>) -> Field<'static, 'a> {
///     image.field(0).unwrap()
/// }
/// ```
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Field<'meta, 'event> {
    pub column: &'meta ColumnInfo,
    pub value: RowValue<'event>,
}

fn field<'image, 'event>(
    image: &'image RowImage<'event>,
    index: usize,
) -> Result<Option<Field<'image, 'event>>> {
    let column = image
        .columns()
        .get(index)
        .ok_or_else(|| invalid("column index outside row image"))?;
    for field in image.fields() {
        let field = field?;
        let column_index = usize::from(field.column.index);
        if column_index == index {
            return Ok(Some(Field {
                column,
                value: field.value,
            }));
        }
        if column_index > index {
            break;
        }
    }
    Ok(None)
}

/// An operation whose shape cannot contain missing required images.
#[derive(Debug, Clone)]
pub enum RowOperation<I> {
    Insert { after: I },
    Update { before: I, after: I },
    Delete { before: I },
}
impl<I> RowOperation<I> {
    pub fn before(&self) -> Option<&I> {
        match self {
            Self::Insert { .. } => None,
            Self::Update { before, .. } | Self::Delete { before } => Some(before),
        }
    }
    pub fn after(&self) -> Option<&I> {
        match self {
            Self::Delete { .. } => None,
            Self::Update { after, .. } | Self::Insert { after } => Some(after),
        }
    }
    pub fn into_images(self) -> (Option<I>, Option<I>) {
        match self {
            Self::Insert { after } => (None, Some(after)),
            Self::Update { before, after } => (Some(before), Some(after)),
            Self::Delete { before } => (Some(before), None),
        }
    }
}

/// A statically selected operation; FULL and MINIMAL remain distinct types.
///
/// ```compile_fail
/// use mysql_replication::events::{Full, Minimal, Row};
/// fn sparse(row: Row<'_, Full>) -> Row<'_, Minimal> { row }
/// ```
#[derive(Debug, Clone)]
pub struct Row<'a, M: BinlogMode> {
    pub operation: RowOperation<M::Image<'a>>,
}

/// Row decoding failure with the absolute ordinal for a violated NOT NULL constraint.
#[derive(Debug, thiserror::Error)]
pub enum RowReadError {
    #[error(transparent)]
    Protocol(#[from] Error),
    #[error("NULL in NOT NULL column {0}")]
    UnexpectedNull(u16),
}

/// A typed image and the selected column value, or None when it was not logged.
pub type ProjectedOperation<'a, M> =
    RowOperation<(<M as BinlogMode>::Image<'a>, Option<RowValue<'a>>)>;

/// A mode-specific streaming iterator. It yields one error and then stops.
pub struct Rows<'a, M: BinlogMode> {
    rows: DecodedRows<'a>,
    done: bool,
    mode: PhantomData<M>,
}
impl<'a, M: BinlogMode> Rows<'a, M> {
    pub(crate) fn new(rows: DecodedRows<'a>) -> Self {
        Self {
            rows,
            done: false,
            mode: PhantomData,
        }
    }
    /// Reads one operation and captures a selected value while validating each image.
    /// The row iterator is exhausted after an error, just as for Iterator::next.
    pub fn next_with_column(
        &mut self,
        index: u16,
    ) -> Option<std::result::Result<ProjectedOperation<'a, M>, RowReadError>> {
        self.read(Some(index), |image, value| {
            Ok((M::Image::try_from(image)?, value))
        })
    }
    fn read<T>(
        &mut self,
        selected: Option<u16>,
        map: impl Fn(RowImage<'a>, Option<RowValue<'a>>) -> Result<T>,
    ) -> Option<std::result::Result<RowOperation<T>, RowReadError>> {
        if self.done {
            return None;
        }
        let result = self.rows.read(selected, map)?;
        self.done = result.is_err();
        Some(result)
    }
}
impl<'a, M: BinlogMode> Iterator for Rows<'a, M> {
    type Item = Result<Row<'a, M>>;
    fn next(&mut self) -> Option<Self::Item> {
        self.read(None, |image, _| M::Image::try_from(image))
            .map(|result| {
                result
                    .map(|operation| Row { operation })
                    .map_err(|err| match err {
                        RowReadError::Protocol(error) => error,
                        RowReadError::UnexpectedNull(_) => invalid("NULL in a NOT NULL column"),
                    })
            })
    }
}
impl<M: BinlogMode> FusedIterator for Rows<'_, M> {}
