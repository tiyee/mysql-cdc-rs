//! Select a typed image policy once, then stream borrowed row operations.
use mysql_replication::{
    Error, EventData, Result,
    events::{Full, ImageMode, Minimal, RowImage, RowValue, TypedRowsDecoder},
    file::BinlogFile,
};
use std::{
    collections::BTreeMap,
    fs::File,
    io::{BufReader, Write},
};

const STMT_END_F: u16 = 1;

fn print_image(output: &mut dyn Write, label: &str, image: &RowImage<'_>) -> Result<()> {
    for field in image.fields() {
        let field = field?;
        match field.value {
            RowValue::Decimal(value) => {
                writeln!(output, "{label}[{}] = {value}", field.column_index)?
            }
            // TABLE_MAP does not provide mandatory SQL signedness metadata.
            RowValue::Integer(value) => writeln!(
                output,
                "{label}[{}] = signed:{} unsigned:{}",
                field.column_index,
                value.signed(),
                value.unsigned()
            )?,
            value => writeln!(output, "{label}[{}] = {value:?}", field.column_index)?,
        }
    }
    Ok(())
}

fn run<M: ImageMode>(path: &str, output: &mut dyn Write) -> Result<()> {
    let mut reader = BinlogFile::new(BufReader::new(File::open(path)?))?;
    let mut tables = BTreeMap::new();
    while let Some(event) = reader.next_event()? {
        match event.event {
            EventData::FormatDescription(_) | EventData::Rotate(_) => tables.clear(),
            EventData::TableMap(table) => {
                tables.insert(table.table_id, TypedRowsDecoder::<M>::new(&table)?);
            }
            EventData::Rows(event) => {
                if event.table_id != 0x00ff_ffff || event.flags & STMT_END_F == 0 {
                    let decoder = tables.get(&event.table_id).ok_or_else(|| {
                        Error::Invalid(format!("missing TABLE_MAP for {}", event.table_id))
                    })?;
                    for row in decoder.rows(&event)? {
                        let row = row?;
                        for (label, image) in [("before", row.before), ("after", row.after)] {
                            if let Some(image) = image {
                                print_image(output, label, image.as_ref())?;
                            }
                        }
                    }
                }
                if event.flags & STMT_END_F != 0 {
                    tables.clear();
                }
            }
            _ => {}
        }
    }
    Ok(())
}

fn main() -> Result<()> {
    let args: Vec<_> = std::env::args().collect();
    execute(&args, &mut std::io::stdout().lock())
}

fn execute(args: &[String], output: &mut dyn Write) -> Result<()> {
    match args {
        [_, path] => run::<Minimal>(path, output),
        [_, path, flag, mode] if flag == "--row-image" => match mode.as_str() {
            "minimal" => run::<Minimal>(path, output),
            "full" => run::<Full>(path, output),
            _ => Err(Error::Invalid("row image must be minimal or full".into())),
        },
        _ => {
            eprintln!(
                "usage: cargo run -p mysql-replication --example decode_typed_rows -- BINLOG_FILE [--row-image minimal|full]"
            );
            Ok(())
        }
    }
}

#[cfg(test)]
#[path = "../tests/support/typed_rows_example.rs"]
mod tests;
