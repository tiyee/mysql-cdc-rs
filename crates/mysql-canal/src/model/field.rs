//! Borrowed field values and owned images with historical canal column metadata.
use crate::{ColumnMeta, Error, Result, TableMeta, error::invalid};
pub use mysql_replication::events::{DecimalValue, IntegerValue, RowOperation, RowValue};
use mysql_replication::events::{Fields as RawFields, OwnedRowImage, RowImage};
use serde::{
    Serialize, Serializer,
    ser::{SerializeSeq, SerializeStruct},
};
use std::{iter::FusedIterator, sync::Arc};

/// A present field. The ordinal is absolute; SQL NULL is `RowValue::Null`.
#[derive(Debug, Clone, Copy)]
pub struct Field<'meta, 'event> {
    pub index: u16,
    pub column: &'meta ColumnMeta,
    pub value: RowValue<'event>,
}

/// One applicable image containing only the fields actually logged by MySQL.
/// The generic decoder validates FULL completeness before constructing changes.
#[derive(Debug, Clone)]
pub struct FieldImage<'a> {
    table: &'a Arc<TableMeta>,
    image: RowImage<'a>,
}
impl<'a> FieldImage<'a> {
    pub(crate) fn new(table: &'a Arc<TableMeta>, image: RowImage<'a>) -> Self {
        Self { table, image }
    }
    pub fn table(&self) -> &TableMeta {
        self.table
    }
    pub fn columns(&self) -> &[ColumnMeta] {
        &self.table.columns
    }
    pub fn column_indices(&self) -> &[u16] {
        self.image.column_indices()
    }
    pub fn fields(&self) -> Fields<'_, 'a> {
        Fields {
            columns: &self.table.columns,
            fields: self.image.fields(),
        }
    }
    /// Returns `None` for an unlogged column; out-of-range indices are errors.
    pub fn field(&self, index: usize) -> Result<Option<Field<'_, 'a>>> {
        find_field(self.fields(), self.columns().len(), index)
    }
    pub(crate) fn memory_bytes(&self) -> usize {
        self.image.retained_bytes()
    }
    pub fn into_owned(self) -> OwnedFieldImage {
        OwnedFieldImage {
            table: Arc::clone(self.table),
            image: self.image.into_owned(),
        }
    }
}

/// An image retained across events. Its values borrow the owned image on access.
#[derive(Debug, Clone)]
pub struct OwnedFieldImage {
    table: Arc<TableMeta>,
    image: OwnedRowImage,
}
impl OwnedFieldImage {
    pub fn table(&self) -> &TableMeta {
        &self.table
    }
    pub fn columns(&self) -> &[ColumnMeta] {
        &self.table.columns
    }
    pub fn column_indices(&self) -> &[u16] {
        self.image.column_indices()
    }
    pub fn fields(&self) -> Fields<'_, '_> {
        Fields {
            columns: &self.table.columns,
            fields: self.image.fields(),
        }
    }
    pub fn field(&self, index: usize) -> Result<Option<Field<'_, '_>>> {
        find_field(self.fields(), self.columns().len(), index)
    }
    pub(crate) fn memory_bytes(&self) -> usize {
        self.image.retained_bytes()
    }
}

/// A streaming iterator over present fields; definitions and values remain borrowed.
pub struct Fields<'meta, 'event> {
    columns: &'meta [ColumnMeta],
    fields: RawFields<'meta, 'event>,
}
impl<'meta, 'event> Iterator for Fields<'meta, 'event> {
    type Item = Result<Field<'meta, 'event>>;
    fn next(&mut self) -> Option<Self::Item> {
        self.fields.next().map(|field| {
            let field = field?;
            let index = field.column.index;
            let column = self
                .columns
                .get(usize::from(index))
                .ok_or_else(|| Error::Schema("field index outside historical schema".into()))?;
            Ok(Field {
                index,
                column,
                value: field.value,
            })
        })
    }
}
impl FusedIterator for Fields<'_, '_> {}

fn find_field<'meta, 'event>(
    fields: Fields<'meta, 'event>,
    count: usize,
    index: usize,
) -> Result<Option<Field<'meta, 'event>>> {
    if index >= count {
        return Err(invalid("field index outside historical schema"));
    }
    for field in fields {
        let field = field?;
        match usize::from(field.index).cmp(&index) {
            std::cmp::Ordering::Equal => return Ok(Some(field)),
            std::cmp::Ordering::Greater => break,
            std::cmp::Ordering::Less => {}
        }
    }
    Ok(None)
}

impl Serialize for Field<'_, '_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error> {
        let mut out = serializer.serialize_struct("Field", 3)?;
        out.serialize_field("index", &self.index)?;
        out.serialize_field("column", self.column)?;
        out.serialize_field("value", &Value(self.value, self.column))?;
        out.end()
    }
}
struct Value<'a>(RowValue<'a>, &'a ColumnMeta);
impl Serialize for Value<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error> {
        use serde::ser::Error as _;
        match self.0 {
            RowValue::Null => serializer.serialize_none(),
            RowValue::Integer(value) => {
                if self.1.is_unsigned() {
                    serializer.serialize_u64(value.unsigned())
                } else {
                    serializer.serialize_i64(value.signed())
                }
            }
            RowValue::Decimal(value) => serializer.collect_str(&value),
            RowValue::Float(value) if value.is_finite() => serializer.serialize_f32(value),
            RowValue::Double(value) if value.is_finite() => serializer.serialize_f64(value),
            RowValue::Float(_) | RowValue::Double(_) => {
                Err(S::Error::custom("non-finite field value"))
            }
            RowValue::Bytes(value) => match self.1.charset.as_deref() {
                Some("utf8" | "utf8mb3" | "utf8mb4") => {
                    serializer.serialize_str(std::str::from_utf8(value).map_err(S::Error::custom)?)
                }
                Some("ascii") => {
                    if !value.is_ascii() {
                        return Err(S::Error::custom("non-ASCII field value"));
                    }
                    serializer.serialize_str(std::str::from_utf8(value).map_err(S::Error::custom)?)
                }
                Some("latin1") => {
                    let text = encoding_rs::WINDOWS_1252
                        .decode_without_bom_handling_and_without_replacement(value)
                        .ok_or_else(|| S::Error::custom("invalid latin1 field value"))?;
                    serializer.serialize_str(&text)
                }
                _ => serializer.serialize_bytes(value),
            },
            RowValue::Encoded(value) => serializer.serialize_bytes(value),
            RowValue::Bit(value) | RowValue::Set(value) => serializer.serialize_u64(value),
            RowValue::Enum(value) | RowValue::Year(value) => serializer.serialize_u16(value),
            _ => Err(S::Error::custom("unsupported field value")),
        }
    }
}
fn serialize_fields<S: Serializer>(
    fields: Fields<'_, '_>,
    serializer: S,
) -> std::result::Result<S::Ok, S::Error> {
    use serde::ser::Error as _;
    let mut seq = serializer.serialize_seq(None)?;
    for field in fields {
        seq.serialize_element(&field.map_err(S::Error::custom)?)?;
    }
    seq.end()
}
impl Serialize for FieldImage<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error> {
        serialize_fields(self.fields(), serializer)
    }
}
impl Serialize for OwnedFieldImage {
    fn serialize<S: Serializer>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error> {
        serialize_fields(self.fields(), serializer)
    }
}

pub(crate) struct Images<'a, I>(pub &'a RowOperation<I>);
impl<I: Serialize> Serialize for Images<'_, I> {
    fn serialize<S: Serializer>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error> {
        let mut out = serializer.serialize_struct(
            "Fields",
            if matches!(self.0, RowOperation::Update { .. }) {
                2
            } else {
                1
            },
        )?;
        if let Some(before) = self.0.before() {
            out.serialize_field("before", before)?;
        }
        if let Some(after) = self.0.after() {
            out.serialize_field("after", after)?;
        }
        out.end()
    }
}
