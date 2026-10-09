use super::events::{
    GENERATED_INVISIBLE_PK_F, MysqlEventParser, RowsEvent, STMT_END_F, TableMapEvent,
};
use crate::{
    Change, ColumnMeta, Error, Operation, PrimaryKey, Result, TableMeta,
    error::invalid,
    model::schema::{KeyDecoder, key_decoder},
    protocol::mysql::wire::{Cursor, bit, set_bits},
};
use std::{collections::BTreeMap, sync::Arc};

#[derive(Clone, Copy, Debug)]
enum ReadOp {
    Fixed(usize),
    Prefixed { width: usize, maximum: u64 },
}
impl ReadOp {
    fn read<'a>(self, input: &mut Cursor<'a>) -> Result<&'a [u8]> {
        let len = match self {
            Self::Fixed(n) => n,
            Self::Prefixed { width, maximum } => {
                let n = input.le(width)?;
                if n > maximum {
                    return Err(invalid("column length exceeds TABLE_MAP maximum"));
                }
                usize::try_from(n).map_err(|_| invalid("column length overflows usize"))?
            }
        };
        input.take(len)
    }
}

struct TablePlan {
    meta: Arc<TableMeta>,
    pk_index: usize,
    decode_pk: KeyDecoder,
    columns: Vec<ReadOp>,
    signature: Vec<u8>,
}

struct ReadColumn {
    op: ReadOp,
    is_pk: bool,
    nullable: bool,
}
struct ImagePlan {
    columns: Vec<ReadColumn>,
}
impl ImagePlan {
    fn new(table: &TablePlan, present: &[u8]) -> Self {
        Self {
            columns: set_bits(present)
                .take_while(|&i| i < table.columns.len())
                .map(|i| ReadColumn {
                    op: table.columns[i],
                    is_pk: i == table.pk_index,
                    nullable: table.meta.columns[i].nullable,
                })
                .collect(),
        }
    }
    fn read<'a>(
        &self,
        input: &mut Cursor<'a>,
        table: &TablePlan,
    ) -> Result<Option<PrimaryKey<'a>>> {
        let nulls = input.bitmap(self.columns.len())?;
        let mut pk = None;
        for (i, col) in self.columns.iter().enumerate() {
            if bit(nulls, i) {
                if col.is_pk {
                    return Err(Error::MissingPrimaryKey);
                }
                if !col.nullable {
                    return Err(Error::Schema("NULL in a NOT NULL column".into()));
                }
                continue;
            }
            let data = col.op.read(input)?;
            if col.is_pk {
                pk = Some((table.decode_pk)(
                    data,
                    &table.meta.columns[table.pk_index],
                )?);
            }
        }
        Ok(pk)
    }
}

/// TABLE_MAP preprocessing + sparse ROWS decoding. The callback borrows event bytes.
/// Only white-listed tables are compiled; other tables' ROWS payloads are skipped.
pub struct Decoder {
    tables: BTreeMap<String, BTreeMap<String, Arc<TableMeta>>>,
    compiled: BTreeMap<String, BTreeMap<String, Arc<TablePlan>>>,
    index: Vec<(u64, Option<Arc<TablePlan>>)>,
}

impl Decoder {
    pub fn new(tables: Vec<TableMeta>) -> Result<Self> {
        let mut out = Self {
            tables: BTreeMap::new(),
            compiled: BTreeMap::new(),
            index: Vec::new(),
        };
        if tables.is_empty() {
            return Err(Error::Schema("empty whitelist".into()));
        }
        for table in tables {
            table.validate()?;
            let schema = out.tables.entry(table.schema.clone()).or_default();
            if schema
                .insert(table.table.clone(), Arc::new(table))
                .is_some()
            {
                return Err(Error::Schema("duplicate whitelist table".into()));
            }
        }
        Ok(out)
    }

    pub fn clear_table_maps(&mut self) {
        self.index.clear();
    }

    /// Input excludes the 19-byte common header and checksum. Modern 6-byte IDs only.
    pub fn table_map(&mut self, data: &[u8]) -> Result<()> {
        self.table_map_event(MysqlEventParser::standard().table_map(data)?)
    }

    pub fn table_map_event(&mut self, event: TableMapEvent<'_>) -> Result<()> {
        let TableMapEvent {
            table_id: id,
            flags,
            schema,
            table: name,
            column_data: signature,
            ..
        } = event;
        let meta = self.tables.get(schema).and_then(|s| s.get(name));
        let plan = if let Some(meta) = meta {
            if flags & GENERATED_INVISIBLE_PK_F != 0 {
                return Err(Error::Unsupported("generated invisible primary key".into()));
            }
            if let Some(existing) = self
                .compiled
                .get(schema)
                .and_then(|s| s.get(name))
                .filter(|p| p.signature == signature)
            {
                Some(Arc::clone(existing))
            } else {
                let plan = Arc::new(compile(Arc::clone(meta), signature)?);
                self.compiled
                    .entry(schema.into())
                    .or_default()
                    .insert(name.into(), Arc::clone(&plan));
                Some(plan)
            }
        } else {
            None
        };
        let i = self.index.partition_point(|(table_id, _)| *table_id < id);
        if self
            .index
            .get(i)
            .is_some_and(|(table_id, _)| *table_id == id)
        {
            self.index[i].1 = plan;
        } else {
            self.index.insert(i, (id, plan));
        }
        Ok(())
    }

    /// Supports WRITE_ROWS_EVENT, UPDATE_ROWS_EVENT and DELETE_ROWS_EVENT, v1/v2.
    pub fn rows(
        &mut self,
        event_type: u8,
        data: &[u8],
        mut emit: impl FnMut(Change<'_>) -> Result<()>,
    ) -> Result<()> {
        self.rows_event(
            MysqlEventParser::standard().rows(event_type, data)?,
            &mut emit,
        )
    }

    pub fn rows_event(
        &mut self,
        event: RowsEvent<'_>,
        mut emit: impl FnMut(Change<'_>) -> Result<()>,
    ) -> Result<()> {
        let id = event.table_id;
        let flags = event.flags;
        let operation = event.operation;
        if id == 0x00ff_ffff && flags & STMT_END_F != 0 {
            self.clear_table_maps();
            return Ok(());
        }
        let i = self.index.partition_point(|(table_id, _)| *table_id < id);
        let table = self
            .index
            .get(i)
            .filter(|(table_id, _)| *table_id == id)
            .ok_or(Error::MissingTable(id))?;
        // Excluded tables never enter column or row decoding.
        let Some(table) = table.1.as_ref() else {
            if flags & STMT_END_F != 0 {
                self.clear_table_maps();
            }
            return Ok(());
        };
        let images = event.images()?;
        let count = images.column_count;
        if count != table.columns.len() {
            return Err(Error::Schema(
                "ROWS column count differs from TABLE_MAP".into(),
            ));
        }
        let first = images.before_columns;
        let second = images.after_columns;
        let mut input = Cursor::new(images.rows);
        let before = ImagePlan::new(table, first);
        let after = ImagePlan::new(table, second);
        if before.columns.is_empty() {
            return Err(invalid("empty first row image"));
        }
        if operation == Operation::Update && after.columns.is_empty() {
            return Err(invalid("empty after row image"));
        }
        // With MINIMAL + a single PK, the before image must contain that PK only.
        if operation != Operation::Insert
            && (before.columns.len() != 1 || !bit(first, table.pk_index))
        {
            return Err(Error::Unsupported(
                "expected MINIMAL before image containing only the primary key".into(),
            ));
        }
        let changed: Arc<[u16]> = if operation == Operation::Delete {
            Arc::from([])
        } else {
            set_bits(second)
                .take_while(|&i| i < count)
                .map(|i| i as u16)
                .collect()
        };
        if input.remaining().is_empty() {
            return Err(invalid("ROWS event contains no rows"));
        }
        while !input.remaining().is_empty() {
            let old = before
                .read(&mut input, table)?
                .ok_or(Error::MissingPrimaryKey)?;
            let (pk, previous_pk) = if operation == Operation::Update {
                match after.read(&mut input, table)? {
                    Some(new) if new != old => (new, Some(old)),
                    Some(new) => (new, None),
                    None => (old, None),
                }
            } else {
                (old, None)
            };
            emit(Change {
                table: &table.meta,
                operation,
                pk,
                previous_pk,
                changed: &changed,
            })?;
        }
        if flags & STMT_END_F != 0 {
            self.clear_table_maps();
        }
        Ok(())
    }
}

fn compile(meta: Arc<TableMeta>, data: &[u8]) -> Result<TablePlan> {
    let pk_index = meta.validate()?;
    let decode_pk = key_decoder(&meta.columns[pk_index])?;
    let mut input = Cursor::new(data);
    let count = input.lenenc()?;
    if count != meta.columns.len() {
        return Err(Error::Schema(format!(
            "{}.{} column count differs",
            meta.schema, meta.table
        )));
    }
    let types = input.take(count)?;
    let metadata_len = input.lenenc()?;
    let mut metadata = Cursor::new(input.take(metadata_len)?);
    let nullable = input.bitmap(count)?;
    let mut columns = Vec::with_capacity(count);
    for (i, (&ty, col)) in types.iter().zip(&meta.columns).enumerate() {
        if bit(nullable, i) != col.nullable {
            return Err(Error::Schema(format!("{} nullability differs", col.name)));
        }
        let (op, logical_type) = read_op(ty, &mut metadata, col)?;
        if !compatible(&col.data_type, logical_type) {
            return Err(Error::Schema(format!(
                "{}: {} differs from wire type {logical_type}",
                col.name, col.data_type
            )));
        }
        columns.push(op);
    }
    if !metadata.remaining().is_empty() {
        return Err(invalid("unused TABLE_MAP column metadata"));
    }
    // Validate optional column names when binlog_row_metadata=FULL.
    while !input.remaining().is_empty() {
        let ty = input.byte()?;
        let len = input.lenenc()?;
        let value = input.take(len)?;
        if ty == 4 {
            let mut names = Cursor::new(value);
            for col in &meta.columns {
                let len = names.lenenc()?;
                if names.take(len)? != col.name.as_bytes() {
                    return Err(Error::Schema(
                        "optional TABLE_MAP column name differs".into(),
                    ));
                }
            }
            if !names.remaining().is_empty() {
                return Err(invalid("extra optional column names"));
            }
        }
    }
    Ok(TablePlan {
        meta,
        pk_index,
        decode_pk,
        columns,
        signature: data.to_vec(),
    })
}

fn compatible(sql: &str, ty: u8) -> bool {
    matches!(
        (sql, ty),
        ("tinyint", 1)
            | ("smallint", 2)
            | ("int", 3)
            | ("float", 4)
            | ("double", 5)
            | ("timestamp", 7 | 17)
            | ("bigint", 8)
            | ("mediumint", 9)
            | ("date", 10 | 14)
            | ("time", 11 | 19)
            | ("datetime", 12 | 18)
            | ("year", 13)
            | ("varchar" | "varbinary", 15)
            | ("bit", 16)
            | ("vector", 242)
            | ("json", 245)
            | ("decimal", 246)
            | ("enum", 247)
            | ("set", 248)
            | (
                "tinyblob"
                    | "tinytext"
                    | "blob"
                    | "text"
                    | "mediumblob"
                    | "mediumtext"
                    | "longblob"
                    | "longtext",
                249..=252
            )
            | ("char" | "binary", 254)
            | (
                "geometry"
                    | "point"
                    | "linestring"
                    | "polygon"
                    | "multipoint"
                    | "multilinestring"
                    | "multipolygon"
                    | "geometrycollection",
                255
            )
    )
}

fn read_op(ty: u8, m: &mut Cursor<'_>, col: &ColumnMeta) -> Result<(ReadOp, u8)> {
    use ReadOp::*;
    let fixed = |n| Ok((Fixed(n), ty));
    match ty {
        1 | 13 => fixed(1),
        2 => fixed(2),
        3 | 7 => fixed(4),
        8 | 12 => fixed(8),
        9..=11 | 14 => fixed(3),
        4 | 5 => {
            let width = if ty == 4 { 4 } else { 8 };
            if usize::from(m.byte()?) != width {
                return Err(invalid("FLOAT/DOUBLE metadata width"));
            }
            fixed(width)
        }
        15 => {
            let maximum = m.le(2)?;
            check_length(col, maximum)?;
            Ok((
                Prefixed {
                    width: if maximum < 256 { 1 } else { 2 },
                    maximum,
                },
                ty,
            ))
        }
        16 => {
            let bits = u64::from(m.byte()?) + u64::from(m.byte()?) * 8;
            if bits == 0 || bits > 64 || col.numeric_precision.map(u64::from) != Some(bits) {
                return Err(Error::Schema("BIT width differs".into()));
            }
            fixed(bits.div_ceil(8) as usize)
        }
        17..=19 => {
            let fsp = m.byte()?;
            if fsp > 6 || col.datetime_precision != Some(u32::from(fsp)) {
                return Err(Error::Schema("temporal precision differs".into()));
            }
            fixed(
                match ty {
                    17 => 4,
                    18 => 5,
                    _ => 3,
                } + usize::from(fsp).div_ceil(2),
            )
        }
        246 => {
            let precision = m.byte()?;
            let scale = m.byte()?;
            if precision == 0
                || precision > 65
                || scale > precision
                || scale > 30
                || col.numeric_precision != Some(u32::from(precision))
                || col.numeric_scale != Some(u32::from(scale))
            {
                return Err(Error::Schema("DECIMAL precision/scale differs".into()));
            }
            const PART: [usize; 10] = [0, 1, 1, 2, 2, 3, 3, 4, 4, 4];
            let int = usize::from(precision - scale);
            let frac = usize::from(scale);
            fixed(int / 9 * 4 + PART[int % 9] + frac / 9 * 4 + PART[frac % 9])
        }
        247 | 248 => {
            m.byte()?;
            let width = usize::from(m.byte()?);
            if width == 0 || width > if ty == 247 { 2 } else { 8 } {
                return Err(invalid("ENUM/SET width"));
            }
            fixed(width)
        }
        254 => {
            let a = m.byte()?;
            let b = m.byte()?;
            let logical = a | 0x30;
            if logical == 247 || logical == 248 {
                let width = usize::from(b);
                if width == 0 || width > if logical == 247 { 2 } else { 8 } {
                    return Err(invalid("ENUM/SET width"));
                }
                Ok((Fixed(width), logical))
            } else if logical == 254 || a == 0 {
                let maximum = if a == 0 {
                    u64::from(b) << 8
                } else {
                    u64::from(b) | (u64::from((a & 0x30) ^ 0x30) << 4)
                };
                check_length(col, maximum)?;
                Ok((
                    Prefixed {
                        width: if maximum < 256 { 1 } else { 2 },
                        maximum,
                    },
                    254,
                ))
            } else {
                Err(invalid("invalid STRING real type"))
            }
        }
        242 | 245 | 249..=252 | 255 => {
            let width = usize::from(m.byte()?);
            if !(1..=4).contains(&width) || ty == 245 && width != 4 {
                return Err(invalid("BLOB/JSON length-prefix width"));
            }
            Ok((
                Prefixed {
                    width,
                    maximum: (1u64 << (width * 8)) - 1,
                },
                ty,
            ))
        }
        other => Err(Error::Unsupported(format!("wire column type {other}"))),
    }
}

fn check_length(col: &ColumnMeta, maximum: u64) -> Result<()> {
    if col.octet_length != Some(maximum) {
        return Err(Error::Schema(format!(
            "{} maximum byte length differs",
            col.name
        )));
    }
    Ok(())
}
