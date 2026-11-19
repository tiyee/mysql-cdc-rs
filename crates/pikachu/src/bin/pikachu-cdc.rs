#![forbid(unsafe_code)]
mod config;
mod print_handle;

use mysql_canal::{
    Canal, Error, Result,
    config::{AppConfig, RowImageMode},
    engine::{Replay, checkpoint::State},
    protocol::mysql::version::VersionRegistry,
};
use std::{collections::HashSet, path::Path};

fn main() {
    if let Err(error) = run() {
        eprintln!("pikachu-cdc: {error}");
        std::process::exit(1);
    }
}
fn run() -> Result<()> {
    let args: Vec<_> = std::env::args().collect();
    let usage = "usage: pikachu-cdc <check|init|run> CONFIG.toml [--row-image SOURCE_ID=minimal|full]... [--track-ddl SOURCE_ID]... [--once]\n       pikachu-cdc replay STATE.json BINLOG_FILE [--row-image minimal|full] [--track-ddl] [--from-position N]\n--once is only valid for run. Each source defaults to row_image = 'minimal'; see README.md";
    if args.len() == 1 || args.get(1).is_some_and(|s| s == "--help" || s == "-h") {
        println!("{usage}");
        return Ok(());
    }
    match args[1].as_str() {
        "replay" if args.len() >= 4 => {
            let mut row_image = None;
            let mut track_ddl = false;
            let mut position = None;
            let mut flags = args[4..].iter();
            while let Some(flag) = flags.next() {
                match flag.as_str() {
                    "--row-image" if row_image.is_none() => {
                        row_image = Some(
                            flags
                                .next()
                                .ok_or_else(|| Error::Invalid(usage.into()))?
                                .parse()?,
                        )
                    }
                    "--track-ddl" if !track_ddl => track_ddl = true,
                    "--from-position" if position.is_none() => {
                        position = Some(
                            flags
                                .next()
                                .and_then(|s| s.parse::<u64>().ok())
                                .ok_or_else(|| Error::Invalid(usage.into()))?,
                        )
                    }
                    _ => return Err(Error::Invalid(usage.into())),
                }
            }
            let path = Path::new(&args[3]);
            let checkpoint = Path::new(&args[2]);
            let state = if let Some(start) = position {
                let file = path
                    .file_name()
                    .and_then(|s| s.to_str())
                    .ok_or_else(|| Error::Invalid("invalid binlog filename".into()))?;
                State::read_at(checkpoint, file, start)?
            } else {
                State::read(checkpoint)?
            };
            let sink = print_handle::PrintHandle::new();
            let mut replay = Replay::new(
                state,
                64 * 1024 * 1024,
                VersionRegistry::default(),
                None,
                row_image.unwrap_or(RowImageMode::Minimal),
            )?;
            if track_ddl {
                replay = replay.with_ddl_tracking();
            }
            replay.run(path, &sink)?;
        }
        command @ ("check" | "init" | "run") if args.len() >= 3 => {
            let mut config = config::Config::read(Path::new(&args[2]))?;
            let options = online_options(&mut config.canal, &args[3..], command == "run")?;
            let mut canal = Canal::new(config.canal)?;
            for id in options.track_ddl {
                canal.track_ddl(id)?;
            }
            match command {
                "check" => canal.check()?,
                "init" => canal.initialize()?,
                _ => {
                    canal.add_handle(print_handle::PrintHandle::new());
                    if options.once {
                        canal.run_once()?;
                    } else {
                        canal.run()?;
                    }
                }
            }
        }
        _ => return Err(Error::Invalid(usage.into())),
    }
    Ok(())
}

#[derive(Default)]
struct OnlineOptions<'a> {
    once: bool,
    track_ddl: HashSet<&'a str>,
}

fn online_options<'a>(
    config: &mut AppConfig,
    args: &'a [String],
    is_run: bool,
) -> Result<OnlineOptions<'a>> {
    let mut options = OnlineOptions::default();
    let mut seen = HashSet::new();
    let mut args = args.iter();
    while let Some(flag) = args.next() {
        match flag.as_str() {
            "--once" if is_run && !options.once => options.once = true,
            "--track-ddl" if is_run => {
                let source = args
                    .next()
                    .ok_or_else(|| Error::Invalid("--track-ddl requires SOURCE_ID".into()))?;
                if !options.track_ddl.insert(source) {
                    return Err(Error::Invalid(format!(
                        "duplicate --track-ddl source {source}"
                    )));
                }
            }
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
                source.config.replication.row_image = mode;
            }
            _ => return Err(Error::Invalid(format!("unexpected option {flag}"))),
        }
    }
    Ok(options)
}
