//! Exact, single-operation ALTER forms whose column metadata can be reconstructed.
use crate::{ColumnMeta, Error, Result, TableMeta, protocol::mysql::version::ServerVersion};
use mysql_replication::events::QueryEvent;

#[derive(Clone, Copy)]
struct Sql<'a>(&'a str);

impl<'a> Sql<'a> {
    fn word(&mut self) -> Option<&'a str> {
        let input = self.0.trim_start_matches(|c: char| c.is_ascii_whitespace());
        let len = input
            .bytes()
            .take_while(|b| b.is_ascii_alphanumeric() || matches!(b, b'_' | b'$'))
            .count();
        if len == 0 {
            return None;
        }
        self.0 = &input[len..];
        Some(&input[..len])
    }

    fn keyword(&mut self, expected: &str) -> bool {
        let mut next = *self;
        if next
            .word()
            .is_some_and(|word| word.eq_ignore_ascii_case(expected))
        {
            *self = next;
            return true;
        }
        false
    }

    fn symbol(&mut self, symbol: char) -> bool {
        let Some(rest) = self.0.trim_ascii_start().strip_prefix(symbol) else {
            return false;
        };
        self.0 = rest;
        true
    }

    fn identifier(&mut self) -> Option<&'a str> {
        if let Some(quoted) = self.0.trim_ascii_start().strip_prefix('`') {
            let (name, rest) = quoted.split_once('`')?;
            if name.is_empty()
                || !name
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'_' | b'$'))
            {
                return None;
            }
            self.0 = rest;
            Some(name)
        } else {
            self.word()
        }
    }
}

enum Alter<'a> {
    Rename {
        old: &'a str,
        new: &'a str,
    },
    AddInt {
        name: &'a str,
        unsigned: bool,
        nullable: bool,
    },
}

fn parse<'a>(sql: &'a str, database: &'a str) -> Option<(&'a str, &'a str, Alter<'a>)> {
    let mut tokens = Sql(sql);
    if !tokens.keyword("ALTER") || !tokens.keyword("TABLE") {
        return None;
    }
    let first = tokens.identifier()?;
    let (database, table) = if tokens.symbol('.') {
        (first, tokens.identifier()?)
    } else {
        (database, first)
    };
    let alter = if tokens.keyword("RENAME") {
        if !tokens.keyword("COLUMN") {
            return None;
        }
        let old = tokens.identifier()?;
        if !tokens.keyword("TO") {
            return None;
        }
        Alter::Rename {
            old,
            new: tokens.identifier()?,
        }
    } else if tokens.keyword("ADD") {
        tokens.keyword("COLUMN");
        let name = tokens.identifier()?;
        if !tokens.keyword("INT") && !tokens.keyword("INTEGER") {
            return None;
        }
        let unsigned = tokens.keyword("UNSIGNED");
        let nullable = !tokens.keyword("NOT");
        if !tokens.keyword("NULL") && !nullable {
            return None;
        }
        Alter::AddInt {
            name,
            unsigned,
            nullable,
        }
    } else {
        return None;
    };
    tokens.symbol(';');
    tokens
        .0
        .trim_ascii()
        .is_empty()
        .then_some((database, table, alter))
}

pub(super) fn apply(
    tables: &[TableMeta],
    query: QueryEvent<'_>,
    producer: &ServerVersion,
) -> Result<Option<Vec<TableMeta>>> {
    let sql = std::str::from_utf8(query.query)?;
    let database = std::str::from_utf8(query.schema)?;
    let Some((database, name, alter)) = parse(sql, database) else {
        return Ok(None);
    };
    let Some(index) = tables
        .iter()
        .position(|t| t.schema == database && t.table == name)
    else {
        return Ok(None);
    };
    let mut tables = tables.to_vec();
    let table = &mut tables[index];
    match alter {
        Alter::Rename { old, new } => {
            let i = table
                .columns
                .iter()
                .position(|c| c.name.eq_ignore_ascii_case(old))
                .ok_or_else(|| {
                    Error::Schema(format!("unknown historical column {database}.{name}.{old}"))
                })?;
            if table
                .columns
                .iter()
                .enumerate()
                .any(|(j, c)| i != j && c.name.eq_ignore_ascii_case(new))
            {
                return Err(Error::Schema(format!("duplicate column {new}")));
            }
            table.columns[i].name = new.into();
        }
        Alter::AddInt {
            name,
            unsigned,
            nullable,
        } => {
            if table
                .columns
                .iter()
                .any(|c| c.name.eq_ignore_ascii_case(name))
            {
                return Err(Error::Schema(format!("duplicate column {name}")));
            }
            let legacy_width = (producer.major, producer.minor, producer.patch) < (8, 0, 19);
            let column_type = match (legacy_width, unsigned) {
                (true, true) => "int(10) unsigned",
                (true, false) => "int(11)",
                (false, true) => "int unsigned",
                (false, false) => "int",
            };
            table.columns.push(ColumnMeta {
                name: name.into(),
                data_type: "int".into(),
                column_type: column_type.into(),
                nullable,
                primary_key: false,
                charset: None,
                octet_length: None,
                numeric_precision: Some(10),
                numeric_scale: Some(0),
                datetime_precision: None,
                extra: String::new(),
            });
        }
    }
    table.validate()?;
    Ok(Some(tables))
}
