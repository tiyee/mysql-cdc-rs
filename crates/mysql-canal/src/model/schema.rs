use crate::{Error, Result};
use serde::{Deserialize, Serialize};
use std::collections::HashSet;

/// Metadata is ordered by INFORMATION_SCHEMA.COLUMNS.ORDINAL_POSITION.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ColumnMeta {
    pub name: String,
    pub data_type: String,
    pub column_type: String,
    pub nullable: bool,
    pub primary_key: bool,
    pub charset: Option<String>,
    pub octet_length: Option<u64>,
    pub numeric_precision: Option<u32>,
    pub numeric_scale: Option<u32>,
    pub datetime_precision: Option<u32>,
    pub extra: String,
}

impl ColumnMeta {
    pub(crate) fn is_unsigned(&self) -> bool {
        self.column_type
            .split_ascii_whitespace()
            .any(|part| part.eq_ignore_ascii_case("unsigned"))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TableMeta {
    pub schema: String,
    pub table: String,
    pub columns: Vec<ColumnMeta>,
}

impl TableMeta {
    pub fn validate(&self) -> Result<usize> {
        if self.schema.is_empty()
            || self.table.is_empty()
            || self.columns.is_empty()
            || self.columns.len() > 4096
        {
            return Err(Error::Schema(
                "empty table name/columns or more than 4096 columns".into(),
            ));
        }
        let mut names = HashSet::new();
        for col in &self.columns {
            if col.name.is_empty() || !names.insert(&col.name) {
                return Err(Error::Schema("empty or duplicate column name".into()));
            }
            if col.extra.contains("VIRTUAL GENERATED")
                || col.extra.contains("STORED GENERATED")
                || col.extra.contains("INVISIBLE")
            {
                return Err(Error::Unsupported(format!(
                    "{}.{}.{}: generated/invisible columns",
                    self.schema, self.table, col.name
                )));
            }
        }
        let keys: Vec<_> = self
            .columns
            .iter()
            .enumerate()
            .filter(|(_, c)| c.primary_key)
            .collect();
        if keys.len() != 1 || keys[0].1.nullable {
            return Err(Error::Schema(format!(
                "{}.{} needs exactly one NOT NULL primary-key column",
                self.schema, self.table
            )));
        }
        key_decoder(keys[0].1)?;
        Ok(keys[0].0)
    }
}

pub(crate) enum KeyDecoder {
    Signed,
    Unsigned,
    Decimal,
    Text(for<'a> fn(&'a [u8], &ColumnMeta) -> Result<crate::PrimaryKey<'a>>),
}
impl KeyDecoder {
    pub fn decode<'a>(
        &self,
        value: crate::RowValue<'a>,
        col: &ColumnMeta,
    ) -> Result<crate::PrimaryKey<'a>> {
        use crate::{PrimaryKey, RowValue};
        Ok(match (self, value) {
            (Self::Signed, RowValue::Integer(n)) => PrimaryKey::Signed(n.signed()),
            (Self::Unsigned, RowValue::Integer(n)) => PrimaryKey::Unsigned(n.unsigned()),
            (Self::Decimal, RowValue::Decimal(n)) => PrimaryKey::Decimal(n.to_string()),
            (Self::Text(decode), RowValue::Bytes(bytes)) => return decode(bytes, col),
            (_, RowValue::Null) => return Err(Error::MissingPrimaryKey),
            _ => return Err(Error::Schema("logged primary-key type differs".into())),
        })
    }
}
pub(crate) fn key_decoder(col: &ColumnMeta) -> Result<KeyDecoder> {
    Ok(match col.data_type.as_str() {
        "tinyint" | "smallint" | "mediumint" | "int" | "bigint" => {
            if col.is_unsigned() {
                KeyDecoder::Unsigned
            } else {
                KeyDecoder::Signed
            }
        }
        "decimal" => {
            if !matches!((col.numeric_precision, col.numeric_scale), (Some(p), Some(s)) if p > 0 && p <= 65 && s <= p && s <= 30)
            {
                return Err(Error::Schema("invalid DECIMAL precision/scale".into()));
            }
            KeyDecoder::Decimal
        }
        "char" | "varchar" => KeyDecoder::Text(match col.charset.as_deref() {
            Some("utf8" | "utf8mb3" | "utf8mb4") => text,
            Some("ascii") => ascii,
            Some("latin1") => latin1,
            other => return Err(Error::Unsupported(format!("primary-key charset {other:?}"))),
        }),
        data_type => {
            return Err(Error::Unsupported(format!(
                "primary-key type {data_type}; use integer, DECIMAL, CHAR or VARCHAR"
            )));
        }
    })
}

fn text<'a>(data: &'a [u8], col: &ColumnMeta) -> Result<crate::PrimaryKey<'a>> {
    let value = std::str::from_utf8(data)?;
    // SQL CHAR retrieval removes trailing space padding.
    let value = if col.data_type == "char" {
        value.trim_end_matches(' ')
    } else {
        value
    };
    Ok(crate::PrimaryKey::Text(value.into()))
}
fn ascii<'a>(data: &'a [u8], col: &ColumnMeta) -> Result<crate::PrimaryKey<'a>> {
    if !data.is_ascii() {
        return Err(crate::error::invalid("non-ASCII primary key"));
    }
    text(data, col)
}
fn latin1<'a>(data: &'a [u8], col: &ColumnMeta) -> Result<crate::PrimaryKey<'a>> {
    let value = encoding_rs::WINDOWS_1252
        .decode_without_bom_handling_and_without_replacement(data)
        .ok_or_else(|| crate::error::invalid("invalid latin1 primary key"))?;
    Ok(crate::PrimaryKey::Text(if col.data_type == "char" {
        value.trim_end_matches(' ').to_owned().into()
    } else {
        value
    }))
}
