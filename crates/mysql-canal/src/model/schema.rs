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

pub(crate) type KeyDecoder = for<'a> fn(&'a [u8], &ColumnMeta) -> Result<crate::PrimaryKey<'a>>;

pub(crate) fn key_decoder(col: &ColumnMeta) -> Result<KeyDecoder> {
    Ok(match (col.data_type.as_str(), col.is_unsigned()) {
        ("tinyint", false) => integer::<1, false>,
        ("tinyint", true) => integer::<1, true>,
        ("smallint", false) => integer::<2, false>,
        ("smallint", true) => integer::<2, true>,
        ("mediumint", false) => integer::<3, false>,
        ("mediumint", true) => integer::<3, true>,
        ("int", false) => integer::<4, false>,
        ("int", true) => integer::<4, true>,
        ("bigint", false) => integer::<8, false>,
        ("bigint", true) => integer::<8, true>,
        ("decimal", _) => {
            if !matches!((col.numeric_precision, col.numeric_scale), (Some(p), Some(s)) if p > 0 && p <= 65 && s <= p && s <= 30)
            {
                return Err(Error::Schema("invalid DECIMAL precision/scale".into()));
            }
            decimal
        }
        ("char" | "varchar", _) => match col.charset.as_deref() {
            Some("utf8" | "utf8mb3" | "utf8mb4") => text,
            Some("ascii") => ascii,
            Some("latin1") => latin1,
            other => return Err(Error::Unsupported(format!("primary-key charset {other:?}"))),
        },
        (data_type, _) => {
            return Err(Error::Unsupported(format!(
                "primary-key type {data_type}; use integer, DECIMAL, CHAR or VARCHAR"
            )));
        }
    })
}

fn integer<'a, const N: usize, const UNSIGNED: bool>(
    data: &'a [u8],
    _: &ColumnMeta,
) -> Result<crate::PrimaryKey<'a>> {
    if data.len() != N {
        return Err(crate::error::invalid("incorrect integer width"));
    }
    let mut bytes = [0; 8];
    bytes[..N].copy_from_slice(data);
    let value = u64::from_le_bytes(bytes);
    Ok(if UNSIGNED {
        crate::PrimaryKey::Unsigned(value)
    } else {
        let shift = (8 - N) * 8;
        crate::PrimaryKey::Signed(((value << shift) as i64) >> shift)
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
fn decimal<'a>(data: &'a [u8], col: &ColumnMeta) -> Result<crate::PrimaryKey<'a>> {
    use std::fmt::Write;
    let precision =
        col.numeric_precision
            .ok_or_else(|| crate::error::invalid("missing decimal precision"))? as usize;
    let scale = col
        .numeric_scale
        .ok_or_else(|| crate::error::invalid("missing decimal scale"))? as usize;
    if precision == 0 || precision > 65 || scale > precision || scale > 30 {
        return Err(crate::error::invalid("invalid decimal precision/scale"));
    }
    const BYTES: [usize; 10] = [0, 1, 1, 2, 2, 3, 3, 4, 4, 4];
    let negative = data
        .first()
        .ok_or_else(|| crate::error::invalid("empty decimal"))?
        & 0x80
        == 0;
    let mut first = true;
    let mut cursor = crate::protocol::mysql::wire::Cursor::new(data);
    let mut nonzero = false;
    let mut group = |digits: usize| -> Result<u32> {
        let mut value = 0u32;
        for &raw in cursor.take(BYTES[digits])? {
            let mut b = raw;
            if first {
                b ^= 0x80;
                first = false;
            }
            if negative {
                b = !b;
            }
            value = (value << 8) | u32::from(b);
        }
        if value >= 10u32.pow(digits as u32) {
            return Err(crate::error::invalid("invalid packed decimal digit group"));
        }
        nonzero |= value != 0;
        Ok(value)
    };
    let mut value = String::with_capacity(precision + 3);
    if negative {
        value.push('-');
    }
    let integral = precision - scale;
    let mut started = false;
    let groups = std::iter::once(integral % 9)
        .filter(|&n| n != 0)
        .chain(std::iter::repeat_n(9, integral / 9));
    for digits in groups {
        let n = group(digits)?;
        if started {
            write!(&mut value, "{n:09}")
                .map_err(|_| crate::error::invalid("decimal formatting"))?;
        } else if n != 0 {
            write!(&mut value, "{n}").map_err(|_| crate::error::invalid("decimal formatting"))?;
            started = true;
        }
    }
    if !started {
        value.push('0');
    }
    if scale != 0 {
        value.push('.');
        for digits in
            std::iter::repeat_n(9, scale / 9).chain(std::iter::once(scale % 9).filter(|&n| n != 0))
        {
            let n = group(digits)?;
            write!(&mut value, "{n:0digits$}")
                .map_err(|_| crate::error::invalid("decimal formatting"))?;
        }
    }
    if !cursor.remaining().is_empty() {
        return Err(crate::error::invalid("extra decimal bytes"));
    }
    if negative && !nonzero {
        value.remove(0);
    }
    Ok(crate::PrimaryKey::Decimal(value))
}
