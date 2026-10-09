#![forbid(unsafe_code)]
mod print_handle;

use mysql_canal::protocol::mysql::event_type as event;
use mysql_canal::{
    config::AppConfig,
    engine::{checkpoint::State, Processor},
    protocol::mysql::file::BinlogFile,
    Cdc, Error, Result,
};
use std::{io::BufReader, path::Path};

fn main() {
    if let Err(error) = run() {
        eprintln!("pikachu-cdc: {error}");
        std::process::exit(1);
    }
}
fn run() -> Result<()> {
    let args: Vec<_> = std::env::args().collect();
    let usage = "usage: pikachu-cdc <check|init|run> CONFIG.toml [--once]\n       pikachu-cdc replay STATE.json BINLOG_FILE\nConfigure [[sources]] and each source's url_env; see README.md";
    if args.len() == 1 || args.get(1).is_some_and(|s| s == "--help" || s == "-h") {
        println!("{usage}");
        return Ok(());
    }
    match args[1].as_str() {
        "replay" if args.len() == 4 => {
            let state = State::read(Path::new(&args[2]))?;
            let path = Path::new(&args[3]);
            if path.file_name().and_then(|n| n.to_str()) != Some(state.file.as_str()) {
                return Err(Error::Schema(
                    "replay filename must match the state's binlog file".into(),
                ));
            }
            let sink = print_handle::PrintHandle::new();
            let start = state.position;
            let mut processor = Processor::new(state, None, 64 * 1024 * 1024)?;
            let mut file = BinlogFile::new(BufReader::new(std::fs::File::open(path)?))?;
            let mut offset = 4u64;
            while let Some(event) = file.next_event()? {
                // Read FDE from the beginning; the saved schema is valid at the
                // saved position, so do not apply earlier DDL/table maps.
                let next = u64::from(event.log_pos);
                if event.event_type == event::FORMAT_DESCRIPTION_EVENT || offset >= start {
                    processor.process_to(event, &sink)?;
                }
                if next <= offset {
                    return Err(Error::Invalid(
                        "non-increasing offline event position".into(),
                    ));
                }
                if offset < start && next > start {
                    return Err(Error::Invalid(
                        "state position is not an event boundary".into(),
                    ));
                }
                offset = next;
            }
            if offset < start {
                return Err(Error::Invalid("state position beyond EOF".into()));
            }
            processor.finish()?;
        }
        command @ ("check" | "init" | "run")
            if args.len() == 3 || command == "run" && args.len() == 4 && args[3] == "--once" =>
        {
            let mut replication = Cdc::new(AppConfig::read(Path::new(&args[2]))?)?;
            match command {
                "check" => replication.check()?,
                "init" => replication.initialize()?,
                _ => {
                    replication.add_handle(print_handle::PrintHandle::new());
                    if args.get(3).is_some_and(|a| a == "--once") {
                        replication.run_once()?;
                    } else {
                        replication.run()?;
                    }
                }
            }
        }
        _ => return Err(Error::Invalid(usage.into())),
    }
    Ok(())
}
