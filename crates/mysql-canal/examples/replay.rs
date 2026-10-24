//! Replay one historical binlog file without updating its checkpoint.
use mysql_canal::{
    Error, Result,
    config::RowImageMode,
    engine::{Replay, checkpoint::State},
    protocol::mysql::version::VersionRegistry,
    sink::JsonLinesSink,
};
use std::{io::BufWriter, path::Path};

fn main() -> Result<()> {
    let args: Vec<_> = std::env::args().collect();
    if !(3..=4).contains(&args.len()) {
        return Err(Error::Invalid(
            "usage: replay STATE.json BINLOG_FILE [minimal|full]".into(),
        ));
    }
    let mode = args
        .get(3)
        .map(|mode| mode.parse())
        .transpose()?
        .unwrap_or(RowImageMode::Minimal);
    let sink = JsonLinesSink::new(BufWriter::new(std::io::stdout()));
    Replay::with_row_image(
        State::read(Path::new(&args[1]))?,
        64 * 1024 * 1024,
        VersionRegistry::default(),
        None,
        mode,
    )?
    .run(Path::new(&args[2]), &sink)
}
