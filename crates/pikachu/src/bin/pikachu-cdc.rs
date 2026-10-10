#![forbid(unsafe_code)]
mod print_handle;

use mysql_canal::protocol::mysql::event_type as event;
use mysql_canal::{
    Cdc, Error, Result,
    config::{AppConfig, RowImageMode},
    engine::{Processor, checkpoint::State},
    protocol::mysql::{file::BinlogFile, version::VersionRegistry},
};
use std::{collections::HashSet, io::BufReader, path::Path};

fn main() {
    if let Err(error) = run() {
        eprintln!("pikachu-cdc: {error}");
        std::process::exit(1);
    }
}
fn run() -> Result<()> {
    let args: Vec<_> = std::env::args().collect();
    let usage = "usage: pikachu-cdc <check|init|run> CONFIG.toml [--row-image SOURCE_ID=minimal|full]... [--once]\n       pikachu-cdc replay STATE.json BINLOG_FILE [--row-image minimal|full]\n--once is only valid for run. Each source defaults to row_image = 'minimal'; see README.md";
    if args.len() == 1 || args.get(1).is_some_and(|s| s == "--help" || s == "-h") {
        println!("{usage}");
        return Ok(());
    }
    match args[1].as_str() {
        "replay" if args.len() >= 4 => {
            let row_image = match &args[4..] {
                [] => RowImageMode::Minimal,
                [flag, value] if flag == "--row-image" => value.parse()?,
                _ => return Err(Error::Invalid(usage.into())),
            };
            let state = State::read(Path::new(&args[2]))?;
            let path = Path::new(&args[3]);
            if path.file_name().and_then(|n| n.to_str()) != Some(state.file.as_str()) {
                return Err(Error::Schema(
                    "replay filename must match the state's binlog file".into(),
                ));
            }
            let sink = print_handle::PrintHandle::new();
            let start = state.position;
            let mut processor = Processor::with_row_image(
                state,
                None,
                64 * 1024 * 1024,
                VersionRegistry::default(),
                None,
                row_image,
            )?;
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
        command @ ("check" | "init" | "run") if args.len() >= 3 => {
            let mut config = AppConfig::read(Path::new(&args[2]))?;
            let once = online_options(&mut config, &args[3..], command == "run")?;
            let mut replication = Cdc::new(config)?;
            match command {
                "check" => replication.check()?,
                "init" => replication.initialize()?,
                _ => {
                    replication.add_handle(print_handle::PrintHandle::new());
                    if once {
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

fn online_options(config: &mut AppConfig, args: &[String], allow_once: bool) -> Result<bool> {
    let mut once = false;
    let mut seen = HashSet::new();
    let mut args = args.iter();
    while let Some(flag) = args.next() {
        match flag.as_str() {
            "--once" if allow_once && !once => once = true,
            "--row-image" => {
                let value = args.next().ok_or_else(|| {
                    Error::Invalid("--row-image requires SOURCE_ID=minimal|full".into())
                })?;
                let (id, mode) = value.split_once('=').ok_or_else(|| {
                    Error::Invalid("--row-image requires SOURCE_ID=minimal|full".into())
                })?;
                let mode = mode.parse()?;
                if !seen.insert(id) {
                    return Err(Error::Invalid(format!(
                        "duplicate row-image override for source {id}"
                    )));
                }
                let source = config
                    .sources
                    .iter_mut()
                    .find(|entry| entry.id == id)
                    .ok_or_else(|| Error::Invalid(format!("unknown source {id} in --row-image")))?;
                source.config.source.row_image = mode;
            }
            _ => return Err(Error::Invalid(format!("unexpected option {flag}"))),
        }
    }
    Ok(once)
}
