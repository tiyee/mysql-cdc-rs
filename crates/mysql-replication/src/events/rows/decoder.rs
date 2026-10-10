use super::{ColumnType, DecimalValue, IntegerValue, RowValue, RowsEvent, RowsKind};
use crate::{Error, Result, error::invalid, events::TableMapEvent, wire::Cursor};
use std::{iter::FusedIterator, sync::Arc};

#[derive(Debug, Clone, Copy)]
enum ReadOp {
    Fixed(usize),
    Prefixed { width: usize, maximum: u64 },
}
impl ReadOp {
    fn read<'a>(self, input: &mut Cursor<'a>) -> Result<&'a [u8]> {
        let length = match self {
            Self::Fixed(n) => n,
            Self::Prefixed { width, maximum } => {
                let n = input.le(width)?;
                if n > maximum {
                    return Err(invalid("column length exceeds TABLE_MAP maximum"));
                }
                usize::try_from(n).map_err(|_| invalid("column length overflows usize"))?
            }
        };
        input.take(length)
    }
}

/// A validated column layout. SQL names, signedness and character sets are not guessed.
#[derive(Debug, Clone, Copy)]
pub struct ColumnDefinition {
    wire_type: ColumnType,
    column_type: ColumnType,
    nullable: bool,
    metadata: [u8; 2],
    metadata_len: usize,
    read: ReadOp,
}
impl ColumnDefinition {
    pub fn wire_type(&self) -> ColumnType {
        self.wire_type
    }
    /// Resolves ENUM/SET encoded under MYSQL_TYPE_STRING.
    pub fn column_type(&self) -> ColumnType {
        self.column_type
    }
    pub fn nullable(&self) -> bool {
        self.nullable
    }
    pub fn metadata(&self) -> &[u8] {
        &self.metadata[..self.metadata_len]
    }
    fn value<'a>(&self, input: &mut Cursor<'a>) -> Result<RowValue<'a>> {
        use ColumnType::*;
        let data = self.read.read(input)?;
        Ok(match self.column_type {
            MYSQL_TYPE_TINY | MYSQL_TYPE_SHORT | MYSQL_TYPE_LONG | MYSQL_TYPE_INT24
            | MYSQL_TYPE_LONGLONG => RowValue::Integer(IntegerValue { data }),
            MYSQL_TYPE_FLOAT => RowValue::Float(f32::from_le_bytes(
                data.try_into().map_err(|_| invalid("FLOAT width"))?,
            )),
            MYSQL_TYPE_DOUBLE => RowValue::Double(f64::from_le_bytes(
                data.try_into().map_err(|_| invalid("DOUBLE width"))?,
            )),
            MYSQL_TYPE_NEWDECIMAL => {
                RowValue::Decimal(DecimalValue::new(data, self.metadata[0], self.metadata[1])?)
            }
            MYSQL_TYPE_VARCHAR
            | MYSQL_TYPE_VAR_STRING
            | MYSQL_TYPE_STRING
            | MYSQL_TYPE_TINY_BLOB
            | MYSQL_TYPE_MEDIUM_BLOB
            | MYSQL_TYPE_LONG_BLOB
            | MYSQL_TYPE_BLOB => RowValue::Bytes(data),
            MYSQL_TYPE_BIT => {
                let bits = usize::from(self.metadata[0]) + usize::from(self.metadata[1]) * 8;
                if bits % 8 != 0 && data[0] >> (bits % 8) != 0 {
                    return Err(invalid("nonzero BIT padding"));
                }
                RowValue::Bit(data.iter().fold(0, |n, &b| (n << 8) | u64::from(b)))
            }
            MYSQL_TYPE_ENUM => RowValue::Enum(Cursor::new(data).le(data.len())? as u16),
            MYSQL_TYPE_SET => RowValue::Set(Cursor::new(data).le(data.len())?),
            MYSQL_TYPE_YEAR => RowValue::Year(if data[0] == 0 {
                0
            } else {
                1900 + u16::from(data[0])
            }),
            _ => RowValue::Encoded(data),
        })
    }
}

/// Reusable TABLE_MAP plan for opt-in raw row decoding.
/// This owns only column layouts, with no schema queries, filtering or CDC policy.
#[derive(Debug)]
pub struct RowsDecoder {
    table_id: u64,
    columns: Vec<ColumnDefinition>,
}
impl RowsDecoder {
    pub fn new(table: &TableMapEvent<'_>) -> Result<Self> {
        let layout = table.columns()?;
        validate_bitmap(layout.nullable, layout.count)?;
        let mut metadata = Cursor::new(layout.metadata);
        let mut columns = Vec::with_capacity(layout.count);
        for (index, &ty) in layout.types.iter().enumerate() {
            columns.push(compile_column(
                ty,
                bit(layout.nullable, index),
                &mut metadata,
            )?);
        }
        metadata.finish()?;
        // Unknown optional TLVs remain opaque, but their envelopes must be complete.
        let mut optional = Cursor::new(layout.optional_metadata);
        while !optional.remaining().is_empty() {
            optional.byte()?;
            let length = optional.lenenc()?;
            optional.take(length)?;
        }
        Ok(Self {
            table_id: table.table_id,
            columns,
        })
    }
    pub fn table_id(&self) -> u64 {
        self.table_id
    }
    pub fn columns(&self) -> &[ColumnDefinition] {
        &self.columns
    }
    /// The caller selects the current TABLE_MAP for this source and table ID.
    /// A remap requires a new decoder, even when the table ID is unchanged.
    pub fn rows<'a>(&self, event: &RowsEvent<'a>) -> Result<Rows<'a>> {
        if event.table_id != self.table_id {
            return Err(invalid("ROWS table ID differs from TABLE_MAP"));
        }
        let images = event.images()?;
        if images.column_count != self.columns.len() {
            return Err(invalid("ROWS column count differs from TABLE_MAP"));
        }
        let first = ImagePlan::new(&self.columns, images.before_columns)?;
        let (before, after) = match event.kind {
            RowsKind::Write => (None, Some(first)),
            RowsKind::Delete => (Some(first), None),
            RowsKind::Update => (
                Some(first),
                Some(ImagePlan::new(&self.columns, images.after_columns)?),
            ),
        };
        if images.rows.is_empty() {
            return Err(invalid("ROWS event contains no rows"));
        }
        Ok(Rows {
            input: Cursor::new(images.rows),
            before,
            after,
            done: false,
        })
    }
}

#[derive(Debug)]
struct ImagePlan {
    column_count: usize,
    indices: Arc<[u16]>,
    columns: Vec<ColumnDefinition>,
}
impl ImagePlan {
    fn new(columns: &[ColumnDefinition], bitmap: &[u8]) -> Result<Arc<Self>> {
        validate_bitmap(bitmap, columns.len())?;
        let mut indices = Vec::new();
        let mut present = Vec::new();
        for (byte_index, &byte) in bitmap.iter().enumerate() {
            let mut bits = byte;
            while bits != 0 {
                let index = byte_index * 8 + bits.trailing_zeros() as usize;
                if index >= columns.len() {
                    break;
                }
                indices.push(index as u16);
                present.push(columns[index]);
                bits &= bits - 1;
            }
        }
        if present.is_empty() {
            return Err(invalid("empty row image"));
        }
        Ok(Arc::new(Self {
            column_count: columns.len(),
            indices: indices.into(),
            columns: present,
        }))
    }
    fn read<'a>(self: &Arc<Self>, input: &mut Cursor<'a>) -> Result<RowImage<'a>> {
        let nulls = input.take(self.columns.len().div_ceil(8))?;
        let start = input.remaining();
        for (i, column) in self.columns.iter().enumerate() {
            if bit(nulls, i) {
                if !column.nullable {
                    return Err(invalid("NULL in a NOT NULL column"));
                }
            } else {
                column.value(input)?;
            }
        }
        Ok(RowImage {
            plan: Arc::clone(self),
            nulls,
            data: &start[..start.len() - input.remaining().len()],
        })
    }
}

/// One row operation. Missing images and absent columns are distinct from SQL NULL.
#[derive(Debug, Clone)]
pub struct Row<'a> {
    pub before: Option<RowImage<'a>>,
    pub after: Option<RowImage<'a>>,
}

/// An image borrowing the event and sharing its sparse column plan across rows.
#[derive(Debug, Clone)]
pub struct RowImage<'a> {
    plan: Arc<ImagePlan>,
    nulls: &'a [u8],
    data: &'a [u8],
}
impl<'a> RowImage<'a> {
    /// Total number of table columns, including columns absent from this image.
    pub fn column_count(&self) -> usize {
        self.plan.column_count
    }
    /// Absolute zero-based table column indices, in wire order.
    pub fn column_indices(&self) -> &[u16] {
        &self.plan.indices
    }
    /// Fields contain only columns present in this image; bytes remain borrowed.
    pub fn fields(&self) -> RowFields<'_, 'a> {
        RowFields {
            image: self,
            input: Cursor::new(self.data),
            index: 0,
            done: false,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct RowField<'a> {
    pub column_index: u16,
    pub value: RowValue<'a>,
}

/// Streaming fields of a previously validated image.
pub struct RowFields<'image, 'event> {
    image: &'image RowImage<'event>,
    input: Cursor<'event>,
    index: usize,
    done: bool,
}
impl<'event> Iterator for RowFields<'_, 'event> {
    type Item = Result<RowField<'event>>;
    fn next(&mut self) -> Option<Self::Item> {
        if self.done {
            return None;
        }
        let column = self.image.plan.columns.get(self.index)?;
        let value = if bit(self.image.nulls, self.index) {
            Ok(RowValue::Null)
        } else {
            column.value(&mut self.input)
        };
        let result = value.map(|value| RowField {
            column_index: self.image.plan.indices[self.index],
            value,
        });
        self.index += 1;
        self.done = result.is_err();
        Some(result)
    }
}
impl FusedIterator for RowFields<'_, '_> {}

/// Streaming row operations. An invalid image yields one error, then ends iteration.
/// Each operation is validated completely before it becomes visible to the caller.
pub struct Rows<'a> {
    input: Cursor<'a>,
    before: Option<Arc<ImagePlan>>,
    after: Option<Arc<ImagePlan>>,
    done: bool,
}
impl<'a> Iterator for Rows<'a> {
    type Item = Result<Row<'a>>;
    fn next(&mut self) -> Option<Self::Item> {
        if self.done || self.input.remaining().is_empty() {
            return None;
        }
        let result = (|| {
            Ok(Row {
                before: self
                    .before
                    .as_ref()
                    .map(|p| p.read(&mut self.input))
                    .transpose()?,
                after: self
                    .after
                    .as_ref()
                    .map(|p| p.read(&mut self.input))
                    .transpose()?,
            })
        })();
        self.done = result.is_err();
        Some(result)
    }
}
impl FusedIterator for Rows<'_> {}

impl Rows<'_> {
    pub(super) fn require_complete(&self) -> Result<()> {
        for plan in self.before.iter().chain(&self.after) {
            if plan.indices.len() != plan.column_count {
                return Err(Error::Unsupported(
                    "FULL row images must contain every table column".into(),
                ));
            }
        }
        Ok(())
    }
}

fn bit(bitmap: &[u8], index: usize) -> bool {
    bitmap[index / 8] & (1 << (index % 8)) != 0
}
fn validate_bitmap(bitmap: &[u8], count: usize) -> Result<()> {
    if bitmap.len() != count.div_ceil(8) {
        return Err(invalid("column bitmap length"));
    }
    Ok(())
}

fn compile_column(ty: u8, nullable: bool, metadata: &mut Cursor<'_>) -> Result<ColumnDefinition> {
    use ColumnType::*;
    use ReadOp::*;
    let wire_type = ColumnType::try_from(ty)
        .map_err(|_| Error::Unsupported(format!("wire column type {ty}")))?;
    let start = metadata.remaining();
    let mut column_type = wire_type;
    let mut m = [0; 2];
    let string = |maximum| Prefixed {
        width: if maximum < 256 { 1 } else { 2 },
        maximum,
    };
    let read = match wire_type {
        MYSQL_TYPE_TINY | MYSQL_TYPE_YEAR => Fixed(1),
        MYSQL_TYPE_SHORT => Fixed(2),
        MYSQL_TYPE_LONG | MYSQL_TYPE_TIMESTAMP => Fixed(4),
        MYSQL_TYPE_LONGLONG | MYSQL_TYPE_DATETIME => Fixed(8),
        MYSQL_TYPE_INT24 | MYSQL_TYPE_DATE | MYSQL_TYPE_NEWDATE | MYSQL_TYPE_TIME => Fixed(3),
        MYSQL_TYPE_FLOAT | MYSQL_TYPE_DOUBLE => {
            m[0] = metadata.byte()?;
            let width = if wire_type == MYSQL_TYPE_FLOAT { 4 } else { 8 };
            if usize::from(m[0]) != width {
                return Err(invalid("FLOAT/DOUBLE metadata width"));
            }
            Fixed(width)
        }
        MYSQL_TYPE_VARCHAR | MYSQL_TYPE_VAR_STRING => {
            m.copy_from_slice(metadata.take(2)?);
            string(u64::from(u16::from_le_bytes(m)))
        }
        MYSQL_TYPE_BIT => {
            m.copy_from_slice(metadata.take(2)?);
            let bits = usize::from(m[0]) + usize::from(m[1]) * 8;
            if m[0] > 7 || bits == 0 || bits > 64 {
                return Err(invalid("BIT metadata width"));
            }
            Fixed(bits.div_ceil(8))
        }
        MYSQL_TYPE_TIMESTAMP2 | MYSQL_TYPE_DATETIME2 | MYSQL_TYPE_TIME2 => {
            m[0] = metadata.byte()?;
            if m[0] > 6 {
                return Err(invalid("temporal precision exceeds 6"));
            }
            let width = match wire_type {
                MYSQL_TYPE_TIMESTAMP2 => 4,
                MYSQL_TYPE_DATETIME2 => 5,
                _ => 3,
            };
            Fixed(width + usize::from(m[0]).div_ceil(2))
        }
        MYSQL_TYPE_NEWDECIMAL => {
            m.copy_from_slice(metadata.take(2)?);
            if m[0] == 0 || m[0] > 65 || m[1] > m[0] || m[1] > 30 {
                return Err(invalid("DECIMAL precision/scale"));
            }
            let integral = usize::from(m[0] - m[1]);
            let scale = usize::from(m[1]);
            Fixed(
                integral / 9 * 4
                    + super::value::decimal_group_size(integral % 9)
                    + scale / 9 * 4
                    + super::value::decimal_group_size(scale % 9),
            )
        }
        MYSQL_TYPE_ENUM | MYSQL_TYPE_SET | MYSQL_TYPE_STRING => {
            m.copy_from_slice(metadata.take(2)?);
            if wire_type == MYSQL_TYPE_STRING {
                column_type = match m[0] | 0x30 {
                    247 => MYSQL_TYPE_ENUM,
                    248 => MYSQL_TYPE_SET,
                    254 => MYSQL_TYPE_STRING,
                    _ if m[0] == 0 => MYSQL_TYPE_STRING,
                    _ => return Err(invalid("invalid STRING real type")),
                };
            }
            if matches!(column_type, MYSQL_TYPE_ENUM | MYSQL_TYPE_SET) {
                let width = usize::from(m[1]);
                if width == 0 || width > if column_type == MYSQL_TYPE_ENUM { 2 } else { 8 } {
                    return Err(invalid("ENUM/SET width"));
                }
                Fixed(width)
            } else {
                let maximum = if m[0] == 0 {
                    u64::from(m[1]) << 8
                } else {
                    u64::from(m[1]) | (u64::from((m[0] & 0x30) ^ 0x30) << 4)
                };
                string(maximum)
            }
        }
        MYSQL_TYPE_JSON
        | MYSQL_TYPE_TINY_BLOB
        | MYSQL_TYPE_MEDIUM_BLOB
        | MYSQL_TYPE_LONG_BLOB
        | MYSQL_TYPE_BLOB
        | MYSQL_TYPE_GEOMETRY
        | MYSQL_TYPE_VECTOR => {
            m[0] = metadata.byte()?;
            let width = usize::from(m[0]);
            if !(1..=4).contains(&width) || wire_type == MYSQL_TYPE_JSON && width != 4 {
                return Err(invalid("BLOB/JSON length-prefix width"));
            }
            Prefixed {
                width,
                maximum: (1u64 << (width * 8)) - 1,
            }
        }
        _ => return Err(Error::Unsupported(format!("wire column type {ty}"))),
    };
    Ok(ColumnDefinition {
        wire_type,
        column_type,
        nullable,
        metadata: m,
        metadata_len: start.len() - metadata.remaining().len(),
        read,
    })
}
