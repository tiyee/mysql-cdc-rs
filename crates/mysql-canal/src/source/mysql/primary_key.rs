//! CDC startup eligibility checks, using the same key rules as row conversion.

use crate::{ColumnMeta, Error, Result, error::invalid, model::schema::key_decoder};
use mysql::{Conn, params, prelude::Queryable};

type KeyColumn = (
    String,
    String,
    String,
    Option<String>,
    Option<u32>,
    Option<u32>,
);

/// Checks selected tables before replication starts, without modifying the server.
///
/// Each table must have one primary-key column: an integer, exact DECIMAL, or
/// CHAR/VARCHAR using utf8, utf8mb3, utf8mb4, ascii or latin1. A UNIQUE index is
/// not a substitute for a PRIMARY KEY. Names are matched exactly.
pub(super) fn validate_primary_keys<'a>(
    conn: &mut Conn,
    tables: impl IntoIterator<Item = (&'a str, &'a str)>,
) -> Result<()> {
    let mut checked = false;
    for (schema, table) in tables {
        checked = true;
        if schema.is_empty() || table.is_empty() {
            return Err(invalid(
                "primary-key check requires a database and table name",
            ));
        }
        let columns: Vec<KeyColumn> = conn.exec(
            "SELECT c.COLUMN_NAME, c.DATA_TYPE, c.COLUMN_TYPE, c.CHARACTER_SET_NAME, c.NUMERIC_PRECISION, c.NUMERIC_SCALE \
             FROM information_schema.KEY_COLUMN_USAGE k \
             JOIN information_schema.COLUMNS c \
             ON BINARY c.TABLE_SCHEMA=BINARY k.TABLE_SCHEMA \
             AND BINARY c.TABLE_NAME=BINARY k.TABLE_NAME AND BINARY c.COLUMN_NAME=BINARY k.COLUMN_NAME \
             WHERE BINARY k.TABLE_SCHEMA=:schema AND BINARY k.TABLE_NAME=:table \
             AND k.CONSTRAINT_NAME='PRIMARY' ORDER BY k.ORDINAL_POSITION",
            params! { "schema" => schema, "table" => table },
        )?;
        validate_columns(schema, table, &columns)?;
    }
    if !checked {
        return Err(invalid("primary-key check requires at least one table"));
    }
    Ok(())
}

fn validate_columns(schema: &str, table: &str, columns: &[KeyColumn]) -> Result<()> {
    let [column] = columns else {
        return Err(Error::Unsupported(format!(
            "{schema}.{table} requires exactly one primary-key column; found {}",
            columns.len()
        )));
    };
    let (name, data_type, column_type, charset, precision, scale) = column;
    // Startup needs only the key metadata; historical row decoding uses the
    // complete schema saved at the checkpoint, never this current-server view.
    let key = ColumnMeta {
        name: name.clone(),
        data_type: data_type.clone(),
        column_type: column_type.clone(),
        nullable: false,
        primary_key: true,
        charset: charset.clone(),
        octet_length: None,
        numeric_precision: *precision,
        numeric_scale: *scale,
        datetime_precision: None,
        extra: String::new(),
    };
    key_decoder(&key).map(|_| ()).map_err(|error| match error {
        Error::Unsupported(message) => {
            Error::Unsupported(format!("{schema}.{table}.{name}: {message}"))
        }
        Error::Schema(message) => invalid(format!("{schema}.{table}.{name}: {message}")),
        other => other,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn column(data_type: &str) -> KeyColumn {
        (
            "id".into(),
            data_type.into(),
            data_type.into(),
            None,
            None,
            None,
        )
    }

    #[test]
    fn rejects_missing_and_composite_primary_keys() {
        for columns in [vec![], vec![column("int"), column("varchar")]] {
            let error = validate_columns("app", "users", &columns).unwrap_err();
            assert!(matches!(error, Error::Unsupported(message)
                if message == format!("app.users requires exactly one primary-key column; found {}", columns.len())));
        }
    }

    #[test]
    fn accepts_integer_decimal_and_supported_text_keys() {
        for data_type in ["tinyint", "smallint", "mediumint", "int", "bigint"] {
            validate_columns("app", "users", &[column(data_type)]).unwrap();
        }
        for (precision, scale) in [(1, 0), (10, 2), (65, 30)] {
            let mut key = column("decimal");
            key.4 = Some(precision);
            key.5 = Some(scale);
            validate_columns("app", "users", &[key]).unwrap();
        }
        for data_type in ["char", "varchar"] {
            for charset in ["utf8", "utf8mb3", "utf8mb4", "ascii", "latin1"] {
                let mut key = column(data_type);
                key.3 = Some(charset.into());
                validate_columns("app", "users", &[key]).unwrap();
            }
        }
    }

    #[test]
    fn rejects_unsupported_types_charsets_and_invalid_decimal_metadata() {
        for data_type in [
            "float",
            "double",
            "binary",
            "varbinary",
            "date",
            "json",
            "enum",
            "bit",
        ] {
            let error = validate_columns("app", "users", &[column(data_type)]).unwrap_err();
            assert!(matches!(error, Error::Unsupported(message)
                if message == format!("app.users.id: primary-key type {data_type}; use integer, DECIMAL, CHAR or VARCHAR")));
        }
        for charset in [None, Some("gbk"), Some("binary")] {
            let mut key = column("varchar");
            key.3 = charset.map(str::to_owned);
            assert!(matches!(validate_columns("app", "users", &[key]),
                Err(Error::Unsupported(message)) if message == format!("app.users.id: primary-key charset {charset:?}")));
        }
        for (precision, scale) in [
            (None, None),
            (Some(0), Some(0)),
            (Some(66), Some(0)),
            (Some(10), Some(11)),
            (Some(65), Some(31)),
        ] {
            let mut key = column("decimal");
            key.4 = precision;
            key.5 = scale;
            assert!(matches!(validate_columns("app", "users", &[key]),
                Err(Error::Invalid(message)) if message == "app.users.id: invalid DECIMAL precision/scale"));
        }
    }
}
