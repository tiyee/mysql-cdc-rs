#[path = "../tests/support/mod.rs"]
mod common;
use common::*;
use mysql_replication::{Decoder, PrimaryKey};
use std::{hint::black_box, time::Instant};

fn main() {
    let mut decoder = Decoder::new(vec![basic()]).unwrap();
    decoder
        .table_map(&table_map(1, &basic(), &[3, 15, 3], &[100, 0]))
        .unwrap();
    for batch in [1i32, 100, 1000] {
        let mut images = Vec::new();
        for id in 0..batch {
            images.push(0);
            images.extend(id.to_le_bytes());
            images.extend([0, 1, b'x']);
        }
        let data = rows(1, UPDATE_ROWS_EVENT, 3, &[1], &[2], &images);
        let iterations = 1_000_000 / batch;
        let start = Instant::now();
        let mut sum = 0i64;
        for _ in 0..iterations {
            decoder
                .rows(UPDATE_ROWS_EVENT, black_box(&data), |c| {
                    if let PrimaryKey::Signed(id) = c.pk {
                        sum = sum.wrapping_add(black_box(id));
                    }
                    black_box(c.changed);
                    Ok(())
                })
                .unwrap();
        }
        let seconds = start.elapsed().as_secs_f64();
        black_box(sum);
        println!(
            "MINIMAL int PK, {batch} rows/event: {:.0} rows/s ({:.1} ns/row); decode only",
            1_000_000.0 / seconds,
            seconds * 1000.0
        );
    }
}
