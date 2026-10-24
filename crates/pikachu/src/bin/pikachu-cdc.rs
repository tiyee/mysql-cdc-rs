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
            let sink = print_handle::PrintHandle::new();
            Replay::new(
                state,
                64 * 1024 * 1024,
                VersionRegistry::default(),
                None,
                row_image,
            )?
            .run(path, &sink)?;
        }
        command @ ("check" | "init" | "run") if args.len() >= 3 => {
            let mut config = config::Config::read(Path::new(&args[2]))?;
            let once = online_options(&mut config.canal, &args[3..], command == "run")?;
            let mut canal = Canal::new(config.canal)?;
            match command {
                "check" => canal.check()?,
                "init" => canal.initialize()?,
                _ => {
                    canal.add_handle(print_handle::PrintHandle::new());
                    if once {
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
                source.config.replication.row_image = mode;
            }
            _ => return Err(Error::Invalid(format!("unexpected option {flag}"))),
        }
    }
    Ok(once)
}
