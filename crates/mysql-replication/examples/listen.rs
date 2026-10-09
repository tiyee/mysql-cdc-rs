//! Register application listeners for normalized, committed changes.
use mysql_replication::{Event, Replication, Result, config::AppConfig};
use std::path::Path;

fn main() -> Result<()> {
    let Some(path) = std::env::args().nth(1) else {
        eprintln!("usage: cargo run -p mysql-replication --example listen -- CONFIG.toml");
        return Ok(());
    };
    let mut replication = Replication::new(AppConfig::read(Path::new(&path))?)?;
    replication.add_handle(|event: Event<'_>| {
        for change in event.changes {
            println!(
                "{} {}.{} {:?} {:?} {:?}",
                event.source_id,
                change.table.schema,
                change.table.table,
                change.operation,
                change.pk,
                change.changed_names().collect::<Vec<_>>()
            );
        }
        Ok(())
    });
    replication.run()
}
