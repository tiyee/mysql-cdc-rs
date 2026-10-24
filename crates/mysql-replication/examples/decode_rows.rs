//! Select a typed image policy once, then stream borrowed row operations.
use mysql_replication::{
    Error, EventData, Result,
    events::{BinlogMode, Full, Minimal, RowImage, RowValue},
    file::BinlogFile,
    parser::ParserConfig,
};
use std::{
    fs::File,
    io::{BufReader, Write},
};

fn print_image(output: &mut dyn Write, label: &str, image: &RowImage<'_>) -> Result<()> {
    for field in image.fields() {
        let field = field?;
        match field.value {
            RowValue::Decimal(value) => {
                writeln!(output, "{label}[{}] = {value}", field.column.index)?
            }
            // TABLE_MAP does not provide mandatory SQL signedness metadata.
            RowValue::Integer(value) => writeln!(
                output,
                "{label}[{}] = signed:{} unsigned:{}",
                field.column.index,
                value.signed(),
                value.unsigned()
            )?,
            value => writeln!(output, "{label}[{}] = {value:?}", field.column.index)?,
        }
    }
    Ok(())
}

fn run<M: BinlogMode>(path: &str, output: &mut dyn Write) -> Result<()> {
    let mut reader =
        BinlogFile::<_, M>::new(BufReader::new(File::open(path)?), ParserConfig::default())?;
    while let Some(event) = reader.next_event()? {
        if let EventData::Rows(event) = event.event {
            for row in event.rows()? {
                let (before, after) = row?.operation.into_images();
                for (label, image) in [("before", before), ("after", after)] {
                    if let Some(image) = image {
                        print_image(output, label, image.as_ref())?;
                    }
                }
            }
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
                "usage: cargo run -p mysql-replication --example decode_rows -- BINLOG_FILE [--row-image minimal|full]"
            );
            Ok(())
        }
    }
}

#[cfg(test)]
#[path = "../tests/support/rows_example.rs"]
mod tests;
