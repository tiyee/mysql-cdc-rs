#[path = "../tests/support/mod.rs"]
mod common;
use common::*;
use mysql_canal::{
    PrimaryKey,
    protocol::mysql::{
        events::ParsedEvent,
        file::EventView,
        version::{FormatDescription, VersionRegistry},
    },
};
use std::{hint::black_box, time::Instant};

fn main() {
    let fde = format();
    let format = FormatDescription::parse(&fde).unwrap();
    let adapter = VersionRegistry::default()
        .resolve(&format.server, None)
        .unwrap();
    let parser = adapter.create_event_parser(&format).unwrap();
    let mut decoder = adapter
        .create_decoder(
            vec![basic()],
            &format,
            mysql_canal::config::RowImageMode::Minimal,
        )
        .unwrap();
    let map = table_map(1, &basic(), &[3, 15, 3], &[100, 0]);
    let ParsedEvent::TableMap(table) = parser
        .parse(EventView {
            event_type: TABLE_MAP_EVENT,
            flags: 0,
            log_pos: 0,
            data: &map,
        })
        .unwrap()
    else {
        panic!("TABLE_MAP")
    };
    decoder.table_map_event(table).unwrap();
    for batch in [1i32, 100, 1000] {
        let mut images = Vec::new();
        for id in 0..batch {
            images.push(0);
            images.extend(id.to_le_bytes());
            images.extend([0, 1, b'x']);
        }
        let data = rows(1, UPDATE_ROWS_EVENT, 3, &[1], &[2], &images);
        let iterations = 1_000_000 / batch;
        let mut sum = 0i64;
        let start = Instant::now();
        for _ in 0..iterations {
            let event = black_box(EventView {
                event_type: UPDATE_ROWS_EVENT,
                flags: 0,
                log_pos: 0,
                data: &data,
            });
            let ParsedEvent::Rows(row) = black_box(parser.as_ref()).parse(event).unwrap() else {
                panic!("ROWS")
            };
            black_box(decoder.as_mut())
                .rows_event(row, &mut |change| {
                    if let PrimaryKey::Signed(id) = change.pk {
                        sum = sum.wrapping_add(black_box(id));
                    }
                    black_box(change.changed);
                    Ok(())
                })
                .unwrap();
        }
        let seconds = start.elapsed().as_secs_f64();
        black_box(sum);
        println!(
            "EventParser + dyn RowDecoder, {batch} rows/event: {:.0} rows/s ({:.1} ns/row); decode only",
            1_000_000.0 / seconds,
            seconds * 1000.0
        );
    }
}
