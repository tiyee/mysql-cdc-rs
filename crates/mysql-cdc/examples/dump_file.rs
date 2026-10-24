use mysql_cdc_rs::replication::{Event, Result, file::BinlogFile, parser::ParserConfig};
use std::{fs::File, io::BufReader};
fn main() -> Result<()> {
    let args: Vec<_> = std::env::args().collect();
    if args.len() != 2 {
        eprintln!(
            "usage: cargo run -p mysql-cdc-rs --example replication_dump_file -- BINLOG_FILE"
        );
        return Ok(());
    }
    let mut reader = BinlogFile::<_, mysql_cdc_rs::replication::Minimal>::new(
        BufReader::new(File::open(&args[1])?),
        ParserConfig::default(),
    )?;
    let mut output = std::io::stdout().lock();
    while let Some(event) = reader.next_event()? {
        event.header.dump(&mut output)?;
        event.event.dump(&mut output)?;
    }
    Ok(())
}
