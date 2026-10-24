//! Owned TABLE_MAP definitions shared by events and borrowed row images.
use super::ColumnDefinition;
use crate::{Result, error::invalid, events::TableMapEvent, wire::Cursor};
use mysql_common::binlog::consts::OptionalMetadataFieldType as Tag;
use std::sync::Arc;

/// Protocol metadata only; absent optional attributes are never guessed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnInfo {
    pub index: u16,
    pub name: Option<Box<str>>,
    pub definition: ColumnDefinition,
    pub unsigned: Option<bool>,
    pub collation_id: Option<u32>,
    pub primary_key: Option<bool>,
    /// Character prefix length; zero means the entire key column.
    pub primary_key_prefix: Option<usize>,
}
/// A historical definition copied once from TABLE_MAP, with no server queries.
#[derive(Debug)]
pub struct TableDefinition {
    pub schema: Box<[u8]>,
    pub table: Box<[u8]>,
    pub columns: Arc<[ColumnInfo]>,
    /// Original optional TLVs, including attributes not interpreted by this API.
    pub optional_metadata: Box<[u8]>,
}
impl TableDefinition {
    pub(super) fn new(
        table: &TableMapEvent<'_>,
        definitions: Vec<ColumnDefinition>,
        optional: &[u8],
    ) -> Result<Self> {
        let mut columns: Vec<_> = definitions
            .into_iter()
            .enumerate()
            .map(|(index, definition)| ColumnInfo {
                index: index as u16,
                name: None,
                definition,
                unsigned: None,
                collation_id: None,
                primary_key: None,
                primary_key_prefix: None,
            })
            .collect();
        let mut input = Cursor::new(optional);
        let mut seen = [false; 256];
        while !input.remaining().is_empty() {
            let tag = input.byte()?;
            if std::mem::replace(&mut seen[usize::from(tag)], true) {
                return Err(invalid("duplicate TABLE_MAP optional metadata"));
            }
            let length = input.lenenc()?;
            let data = input.take(length)?;
            let mut value = Cursor::new(data);
            match tag {
                x if x == Tag::SIGNEDNESS as u8 => {
                    let numeric: Vec<_> = columns
                        .iter_mut()
                        .filter(|c| c.definition.column_type().is_numeric_type())
                        .collect();
                    if data.len() != numeric.len().div_ceil(8) {
                        return Err(invalid("TABLE_MAP signedness bitmap length"));
                    }
                    for (i, column) in numeric.into_iter().enumerate() {
                        column.unsigned = Some(data[i / 8] & (0x80 >> (i % 8)) != 0);
                    }
                }
                x if x == Tag::COLUMN_NAME as u8 => {
                    for column in &mut columns {
                        let length = value.lenenc()?;
                        let name = std::str::from_utf8(value.take(length)?)?;
                        if name.is_empty() {
                            return Err(invalid("empty TABLE_MAP column name"));
                        }
                        column.name = Some(name.into());
                    }
                    value.finish()?;
                }
                x if x == Tag::DEFAULT_CHARSET as u8 || x == Tag::COLUMN_CHARSET as u8 => {
                    charsets(
                        &mut columns,
                        &mut value,
                        x == Tag::DEFAULT_CHARSET as u8,
                        false,
                    )?;
                }
                x if x == Tag::ENUM_AND_SET_DEFAULT_CHARSET as u8
                    || x == Tag::ENUM_AND_SET_COLUMN_CHARSET as u8 =>
                {
                    charsets(
                        &mut columns,
                        &mut value,
                        x == Tag::ENUM_AND_SET_DEFAULT_CHARSET as u8,
                        true,
                    )?;
                }
                x if x == Tag::SIMPLE_PRIMARY_KEY as u8
                    || x == Tag::PRIMARY_KEY_WITH_PREFIX as u8 =>
                {
                    if columns.iter().any(|c| c.primary_key.is_some()) {
                        return Err(invalid("conflicting TABLE_MAP primary-key metadata"));
                    }
                    for column in &mut columns {
                        column.primary_key = Some(false);
                    }
                    while !value.remaining().is_empty() {
                        let index = value.lenenc()?;
                        let column = columns.get_mut(index).ok_or_else(|| {
                            invalid("TABLE_MAP primary-key index outside columns")
                        })?;
                        if column.primary_key == Some(true) {
                            return Err(invalid("duplicate TABLE_MAP primary-key index"));
                        }
                        column.primary_key = Some(true);
                        column.primary_key_prefix =
                            Some(if x == Tag::PRIMARY_KEY_WITH_PREFIX as u8 {
                                value.lenenc()?
                            } else {
                                0
                            });
                    }
                }
                _ => {}
            }
        }
        Ok(Self {
            schema: table.schema.into(),
            table: table.table.into(),
            columns: columns.into(),
            optional_metadata: optional.into(),
        })
    }
}

fn charsets(
    columns: &mut [ColumnInfo],
    input: &mut Cursor<'_>,
    default: bool,
    enum_set: bool,
) -> Result<()> {
    let selected: Vec<_> = columns
        .iter_mut()
        .filter(|c| {
            if enum_set {
                c.definition.column_type().is_enum_or_set_type()
            } else {
                c.definition.column_type().is_character_type()
            }
        })
        .collect();
    if selected.iter().any(|c| c.collation_id.is_some()) {
        return Err(invalid("conflicting TABLE_MAP charset metadata"));
    }
    let read_collation = |input: &mut Cursor<'_>| -> Result<u32> {
        u32::try_from(input.lenenc()?).map_err(|_| invalid("TABLE_MAP collation overflows u32"))
    };
    if default {
        let collation = read_collation(input)?;
        let mut selected = selected;
        let mut seen = vec![false; selected.len()];
        for column in &mut selected {
            column.collation_id = Some(collation);
        }
        while !input.remaining().is_empty() {
            let index = input.lenenc()?;
            let column = selected
                .get_mut(index)
                .ok_or_else(|| invalid("TABLE_MAP charset index outside character columns"))?;
            if std::mem::replace(&mut seen[index], true) {
                return Err(invalid("duplicate TABLE_MAP charset index"));
            }
            column.collation_id = Some(read_collation(input)?);
        }
    } else {
        for column in selected {
            column.collation_id = Some(read_collation(input)?);
        }
        if !input.remaining().is_empty() {
            return Err(invalid("TABLE_MAP charset count differs from columns"));
        }
    }
    Ok(())
}
