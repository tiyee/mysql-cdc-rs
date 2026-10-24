//! Fix the CDC image policy at construction, with one CLI dispatch at startup.
use mysql_canal::{
    Cdc, CdcMode, CommittedTransaction, Error, Full, Minimal, Result,
    config::AppConfig,
    sink::{JsonLinesSink, TransactionSink},
};
use mysql_replication::ImageMode;
use std::{io::BufWriter, path::Path};

fn listen<M: CdcMode + ImageMode>(path: &Path) -> Result<()> {
    let mut cdc = Cdc::<M>::for_mode(AppConfig::read(path)?)?;
    let sink = JsonLinesSink::new(BufWriter::new(std::io::stdout()));
    cdc.add_handle(move |transaction: CommittedTransaction<'_>| {
        sink.commit(
            transaction.source_id,
            transaction.checkpoint,
            transaction.changes,
        )
    });
    cdc.run()
}

fn main() -> Result<()> {
    let args: Vec<_> = std::env::args().collect();
    match &args[..] {
        [_, path] => listen::<Minimal>(Path::new(path)),
        [_, path, mode] => match mode.as_str() {
            "minimal" => listen::<Minimal>(Path::new(path)),
            "full" => listen::<Full>(Path::new(path)),
            _ => Err(Error::Invalid("row_image must be minimal or full".into())),
        },
        _ => Err(Error::Invalid(
            "usage: listen_typed CONFIG.toml [minimal|full]".into(),
        )),
    }
}
