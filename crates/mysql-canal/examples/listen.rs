//! Register application listeners for normalized, committed changes.
use mysql_canal::{Canal, CommittedTransaction, Result, config::AppConfig};
use std::path::Path;

fn main() -> Result<()> {
    let Some(path) = std::env::args().nth(1) else {
        eprintln!("usage: cargo run -p mysql-canal --example listen -- CONFIG.toml");
        return Ok(());
    };
    let mut canal = Canal::new(AppConfig::read(Path::new(&path))?)?;
    canal.add_handle(|event: CommittedTransaction<'_>| {
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
            for (side, image) in [
                ("before", change.fields.before()),
                ("after", change.fields.after()),
            ] {
                if let Some(image) = image {
                    for field in image.fields() {
                        let field = field?;
                        println!(
                            "{} [{}] {} {}: {:?}",
                            side,
                            field.index,
                            field.column.name,
                            field.column.column_type,
                            field.value
                        );
                    }
                }
            }
        }
        Ok(())
    });
    canal.run()
}
