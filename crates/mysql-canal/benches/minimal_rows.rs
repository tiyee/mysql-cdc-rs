#[path = "../tests/support/mod.rs"]
mod common;
use common::*;
use mysql_canal::{Decoder, PrimaryKey};
use std::{hint::black_box, time::Instant};

fn main() {
    for width in [3, 80] {
        bench_sparse_update(width);
    }
}

fn bench_sparse_update(width: usize) {
    let mut meta = basic();
    for i in meta.columns.len()..width {
        meta.columns.push(column(&format!("c{i}"), "int", false));
    }
    let key_index = if width == 3 { 0 } else { width - 1 };
    meta.columns.swap(0, key_index);
    let mut types = vec![3; width];
    types[1] = 15;
    let mut before = vec![0; width.div_ceil(8)];
    before[key_index / 8] |= 1 << (key_index % 8);
    let mut after = vec![0; width.div_ceil(8)];
    after[0] = 2;
    let mut decoder = Decoder::<mysql_canal::Minimal>::new(vec![meta.clone()]).unwrap();
    decode_table(&mut decoder, &table_map(1, &meta, &types, &[100, 0])).unwrap();
    for batch in [1i32, 100, 1000] {
        let mut images = Vec::new();
        for id in 0..batch {
            images.push(0);
            images.extend(id.to_le_bytes());
            images.extend([0, 1, b'x']);
        }
        let data = rows(1, UPDATE_ROWS_EVENT, width, &before, &after, &images);
        let iterations = 1_000_000 / batch;
        let start = Instant::now();
        let mut sum = 0i64;
        for _ in 0..iterations {
            decode_rows(&mut decoder, UPDATE_ROWS_EVENT, black_box(&data), |c| {
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
            "MINIMAL {width} columns, int PK at {key_index}, {batch} rows/event: {:.0} rows/s ({:.1} ns/row); decode only",
            1_000_000.0 / seconds,
            seconds * 1000.0
        );
    }
}
