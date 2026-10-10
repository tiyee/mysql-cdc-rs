//! Inspect raw row images without CDC filtering or transaction delivery.
use mysql_replication::{
    Error, EventData, Result,
    events::{RowImage, RowValue, RowsDecoder},
    file::BinlogFile,
};
use std::{collections::BTreeMap, fs::File, io::BufReader};

const STMT_END_F: u16 = 1;

fn print_image(label: &str, image: Option<RowImage<'_>>) -> Result<()> {
    let Some(image) = image else { return Ok(()) };
    for field in image.fields() {
        let field = field?;
        match field.value {
            RowValue::Decimal(value) => println!("{label}[{}] = {value}", field.column_index),
            // IntegerValue deliberately requires the caller to choose SQL signedness.
            RowValue::Integer(value) => println!(
                "{label}[{}] = signed:{} unsigned:{}",
                field.column_index,
                value.signed(),
                value.unsigned()
            ),
            value => println!("{label}[{}] = {value:?}", field.column_index),
        }
    }
    Ok(())
}

fn main() -> Result<()> {
    let args: Vec<_> = std::env::args().collect();
    if args.len() != 2 {
        eprintln!("usage: cargo run -p mysql-replication --example decode_rows -- BINLOG_FILE");
        return Ok(());
    }
    let mut reader = BinlogFile::new(BufReader::new(File::open(&args[1])?))?;
    let mut tables = BTreeMap::new();
    while let Some(event) = reader.next_event()? {
        match event.event {
            EventData::FormatDescription(_) | EventData::Rotate(_) => tables.clear(),
            EventData::TableMap(table) => {
                // Replace the plan on every remap; never reuse a table ID across files.
                tables.insert(table.table_id, RowsDecoder::new(&table)?);
            }
            EventData::Rows(rows) => {
                // MySQL may send a statement-end marker without row data.
                if rows.table_id != 0x00ff_ffff || rows.flags & STMT_END_F == 0 {
                    let decoder = tables.get(&rows.table_id).ok_or_else(|| {
                        Error::Invalid(format!("missing TABLE_MAP for {}", rows.table_id))
                    })?;
                    for row in rows.rows(decoder)? {
                        let row = row?;
                        print_image("before", row.before)?;
                        print_image("after", row.after)?;
                    }
                }
                if rows.flags & STMT_END_F != 0 {
                    tables.clear();
                }
            }
            _ => {}
        }
    }
    Ok(())
}
