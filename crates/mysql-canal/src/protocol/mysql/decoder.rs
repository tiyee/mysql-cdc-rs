use super::events::{GENERATED_INVISIBLE_PK_F, RowsEvent, STMT_END_F, TableMapEvent};
use crate::{
    CdcImageMode, Change, Error, FieldImage, Operation, Result, RowOperation, TableMeta,
    error::invalid,
    model::schema::{KeyDecoder, key_decoder},
    protocol::mysql::wire::{bit, set_bits},
};
use std::{collections::BTreeMap, sync::Arc};

struct TablePlan<M: CdcImageMode> {
    meta: Arc<TableMeta>,
    pk_index: usize,
    decode_pk: KeyDecoder,
    signature: Vec<u8>,
    fields: mysql_replication::events::RowsDecoder<M>,
}

/// TABLE_MAP preprocessing + sparse ROWS decoding. The callback borrows event bytes.
/// Only white-listed tables are compiled; other tables' ROWS payloads are skipped.
pub struct Decoder<M: CdcImageMode> {
    mode: std::marker::PhantomData<M>,
    tables: BTreeMap<String, BTreeMap<String, Arc<TableMeta>>>,
    compiled: BTreeMap<String, BTreeMap<String, Arc<TablePlan<M>>>>,
    index: Vec<(u64, Option<Arc<TablePlan<M>>>)>,
}

impl<M: CdcImageMode> Decoder<M> {
    pub fn new(tables: Vec<TableMeta>) -> Result<Self> {
        let mut out = Self {
            mode: std::marker::PhantomData,
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
                let plan = Arc::new(compile::<M>(Arc::clone(meta), signature)?);
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
    pub fn rows_event(
        &mut self,
        event: RowsEvent<'_>,
        mut emit: impl FnMut(Change<'_>) -> Result<()>,
    ) -> Result<()> {
        let id = event.table_id;
        let flags = event.flags;
        let operation = event.operation;
        let i = self.index.partition_point(|(table_id, _)| *table_id < id);
        let Some(table) = self.index.get(i).filter(|(table_id, _)| *table_id == id) else {
            if event.is_statement_end_marker() {
                self.clear_table_maps();
                return Ok(());
            }
            return Err(Error::MissingTable(id));
        };
        // Excluded tables never enter column or row decoding.
        let Some(table) = table.1.as_ref() else {
            if flags & STMT_END_F != 0 {
                self.clear_table_maps();
            }
            return Ok(());
        };
        let images = event.images()?;
        let count = images.column_count;
        if count != table.meta.columns.len() {
            return Err(Error::Schema(
                "ROWS column count differs from TABLE_MAP".into(),
            ));
        }
        let first = images.before_columns;
        let second = images.after_columns;
        let present_count = |bits: &[u8]| set_bits(bits).take_while(|i| *i < count).count();
        let before_count = present_count(first);
        let after_count = if operation == Operation::Update {
            present_count(second)
        } else {
            before_count
        };
        if before_count == 0 || after_count == 0 {
            return Err(invalid("empty row image"));
        }
        M::validate_image(
            operation,
            count,
            bit(first, table.pk_index),
            before_count,
            after_count,
        )?;
        let changed: Arc<[u16]> = if operation == Operation::Delete {
            Arc::from([])
        } else {
            set_bits(second)
                .take_while(|&i| i < count)
                .map(|i| i as u16)
                .collect()
        };
        let kind = match operation {
            Operation::Insert => mysql_replication::events::RowsKind::Write,
            Operation::Update => mysql_replication::events::RowsKind::Update,
            Operation::Delete => mysql_replication::events::RowsKind::Delete,
        };
        let mut field_rows = table.fields.rows_from_images(
            kind,
            mysql_replication::events::RowImages {
                extra_data: &[],
                column_count: count,
                before_columns: first,
                after_columns: second,
                rows: images.rows,
            },
        )?;
        let decode_key = |value| {
            table
                .decode_pk
                .decode(value, &table.meta.columns[table.pk_index])
        };
        while let Some(result) = field_rows.next_with_column(table.pk_index as u16) {
            use mysql_replication::events::RowReadError;
            let row = result.map_err(|error| match error {
                RowReadError::UnexpectedNull(i) if usize::from(i) == table.pk_index => {
                    Error::MissingPrimaryKey
                }
                RowReadError::UnexpectedNull(_) => {
                    Error::Schema("NULL in a NOT NULL column".into())
                }
                RowReadError::Protocol(e) => e.into(),
            })?;
            let (pk, previous_pk, fields) = match row {
                RowOperation::Insert {
                    after: (image, key),
                } => (
                    decode_key(key.ok_or(Error::MissingPrimaryKey)?)?,
                    None,
                    RowOperation::Insert {
                        after: FieldImage::new(&table.meta, image.into()),
                    },
                ),
                RowOperation::Delete {
                    before: (image, key),
                } => (
                    decode_key(key.ok_or(Error::MissingPrimaryKey)?)?,
                    None,
                    RowOperation::Delete {
                        before: FieldImage::new(&table.meta, image.into()),
                    },
                ),
                RowOperation::Update {
                    before: (before, old),
                    after: (after, new),
                } => {
                    let old = decode_key(old.ok_or(Error::MissingPrimaryKey)?)?;
                    let (key, previous) = match new.map(decode_key).transpose()? {
                        Some(new) if new != old => (new, Some(old)),
                        _ => (old, None),
                    };
                    (
                        key,
                        previous,
                        RowOperation::Update {
                            before: FieldImage::new(&table.meta, before.into()),
                            after: FieldImage::new(&table.meta, after.into()),
                        },
                    )
                }
            };
            emit(Change {
                table: &table.meta,
                operation,
                pk,
                previous_pk,
                changed: &changed,
                fields,
            })?;
        }
        if flags & STMT_END_F != 0 {
            self.clear_table_maps();
        }
        Ok(())
    }
}

fn compile<M: CdcImageMode>(meta: Arc<TableMeta>, data: &[u8]) -> Result<TablePlan<M>> {
    let pk_index = meta.validate()?;
    let decode_pk = key_decoder(&meta.columns[pk_index])?;
    // Only normalized column data is used; no transport body is reconstructed.
    let mut mapping = mysql_replication::events::TableMapEvent::with_post_header(8);
    mapping.schema = meta.schema.as_bytes();
    mapping.table = meta.table.as_bytes();
    mapping.column_data = data;
    let fields = mysql_replication::events::RowsDecoder::<M>::new(&mapping)?;
    if fields.columns().len() != meta.columns.len() {
        return Err(Error::Schema(format!(
            "{}.{} column count differs",
            meta.schema, meta.table
        )));
    }
    for (column, wire) in meta.columns.iter().zip(fields.columns()) {
        use mysql_replication::events::ColumnType::*;
        let definition = wire.definition;
        let logical = definition.column_type();
        if !compatible(&column.data_type, logical as u8) || definition.nullable() != column.nullable
        {
            return Err(Error::Schema(format!(
                "{} type or nullability differs",
                column.name
            )));
        }
        if wire.name.as_deref().is_some_and(|name| name != column.name) {
            return Err(Error::Schema(
                "optional TABLE_MAP column name differs".into(),
            ));
        }
        match (logical, definition.metadata()) {
            (MYSQL_TYPE_VARCHAR | MYSQL_TYPE_STRING, _)
                if column.octet_length != definition.maximum_length() =>
            {
                return Err(Error::Schema(format!(
                    "{} maximum byte length differs",
                    column.name
                )));
            }
            (MYSQL_TYPE_BIT, &[partial, bytes])
                if column.numeric_precision != Some(u32::from(partial) + u32::from(bytes) * 8) =>
            {
                return Err(Error::Schema("BIT width differs".into()));
            }
            (MYSQL_TYPE_TIMESTAMP2 | MYSQL_TYPE_DATETIME2 | MYSQL_TYPE_TIME2, &[fsp])
                if column.datetime_precision != Some(u32::from(fsp)) =>
            {
                return Err(Error::Schema("temporal precision differs".into()));
            }
            (MYSQL_TYPE_NEWDECIMAL, &[precision, scale])
                if (column.numeric_precision, column.numeric_scale)
                    != (Some(u32::from(precision)), Some(u32::from(scale))) =>
            {
                return Err(Error::Schema("DECIMAL precision/scale differs".into()));
            }
            _ => {}
        }
        if let Some(id) = wire.collation_id {
            use mysql_common::collations::{Collation, CollationId};
            let collation_id = u16::try_from(id)
                .ok()
                .map(CollationId::from)
                .filter(|v| *v != CollationId::UNKNOWN_COLLATION_ID)
                .ok_or_else(|| {
                    Error::Unsupported(format!("unknown collation {id} for {}", column.name))
                })?;
            let collation = Collation::resolve(collation_id);
            let canonical = |s| if s == "utf8" { "utf8mb3" } else { s };
            if canonical(collation.charset())
                != canonical(column.charset.as_deref().unwrap_or("binary"))
            {
                return Err(Error::Schema(format!(
                    "{} character set differs",
                    column.name
                )));
            }
        }
        // Signedness changes integer interpretation; absent attributes stay unknown.
        if matches!(
            wire.definition.column_type(),
            MYSQL_TYPE_TINY
                | MYSQL_TYPE_SHORT
                | MYSQL_TYPE_INT24
                | MYSQL_TYPE_LONG
                | MYSQL_TYPE_LONGLONG
        ) && wire
            .unsigned
            .is_some_and(|unsigned| unsigned != column.is_unsigned())
        {
            return Err(Error::Schema(format!("{} signedness differs", column.name)));
        }
        if wire
            .primary_key
            .is_some_and(|key| key != column.primary_key)
        {
            return Err(Error::Schema(format!(
                "{} primary-key membership differs",
                column.name
            )));
        }
    }
    Ok(TablePlan {
        fields,
        meta,
        pk_index,
        decode_pk,
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
