//! End-to-end tests for the example's parameter dispatch, file input and output.
use super::*;
use mysql_replication::event_type::*;
use std::{
    path::PathBuf,
    sync::atomic::{AtomicU64, Ordering},
};

static NEXT_FILE: AtomicU64 = AtomicU64::new(0);

fn frame(kind: u8, body: &[u8]) -> Vec<u8> {
    let mut bytes = vec![0; 19];
    bytes[4] = kind;
    bytes[5..9].copy_from_slice(&1u32.to_le_bytes());
    bytes.extend(body);
    if kind == FORMAT_DESCRIPTION_EVENT {
        // NONE still carries the algorithm byte and four reserved trailer bytes.
        bytes.extend([0; 5]);
    }
    let size = bytes.len() as u32;
    bytes[9..13].copy_from_slice(&size.to_le_bytes());
    bytes
}

fn format() -> Vec<u8> {
    let mut body = vec![0; 100];
    body[..2].copy_from_slice(&4u16.to_le_bytes());
    body[2..7].copy_from_slice(b"8.4.6");
    body[56] = 19;
    for (kind, size) in [
        (QUERY_EVENT, 13),
        (ROTATE_EVENT, 8),
        (TABLE_MAP_EVENT, 8),
        (WRITE_ROWS_EVENT, 10),
        (UPDATE_ROWS_EVENT, 10),
        (DELETE_ROWS_EVENT, 10),
    ] {
        body[57 + usize::from(kind) - 1] = size;
    }
    frame(FORMAT_DESCRIPTION_EVENT, &body)
}

fn table(types: &[u8], metadata: &[u8], nullable: u8) -> Vec<u8> {
    let mut body = vec![1, 0, 0, 0, 0, 0, 0, 0, 3];
    body.extend(b"app\0");
    body.push(1);
    body.extend(b"t\0");
    body.push(types.len() as u8);
    body.extend(types);
    body.push(metadata.len() as u8);
    body.extend(metadata);
    body.push(nullable);
    frame(TABLE_MAP_EVENT, &body)
}

fn rows(kind: u8, flags: u16, count: u8, bitmaps: &[u8], values: &[u8]) -> Vec<u8> {
    let mut body = vec![1, 0, 0, 0, 0, 0];
    body.extend(flags.to_le_bytes());
    body.extend([2, 0, count]);
    body.extend(bitmaps);
    body.extend(values);
    frame(kind, &body)
}

struct Fixture(PathBuf);
impl Fixture {
    fn new(events: &[Vec<u8>]) -> Self {
        let path = std::env::temp_dir().join(format!(
            "replication-typed-example-{}-{}.binlog",
            std::process::id(),
            NEXT_FILE.fetch_add(1, Ordering::Relaxed)
        ));
        let mut bytes = b"\xfebin".to_vec();
        bytes.extend(format());
        for event in events {
            bytes.extend(event);
        }
        // Create exclusively, so a stale file can never be overwritten.
        let mut file = File::create_new(&path).unwrap();
        file.write_all(&bytes).unwrap();
        Self(path)
    }
    fn run(&self, mode: Option<&str>) -> (Result<()>, Vec<u8>) {
        let mut args = vec!["decode_typed_rows".into(), self.0.to_str().unwrap().into()];
        if let Some(mode) = mode {
            args.extend(["--row-image".into(), mode.into()]);
        }
        let mut output = Vec::new();
        (execute(&args, &mut output), output)
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.0);
    }
}

#[test]
fn mode_parameters_and_default_preserve_complete_operation_output() {
    let fixture = Fixture::new(&[
        table(&[3, 15], &[10, 0], 2),
        rows(
            WRITE_ROWS_EVENT,
            0,
            2,
            &[3],
            &[0, 7, 0, 0, 0, 3, b'o', b'l', b'd'],
        ),
        rows(
            UPDATE_ROWS_EVENT,
            0,
            2,
            &[3, 3],
            &[0, 7, 0, 0, 0, 3, b'o', b'l', b'd', 2, 8, 0, 0, 0],
        ),
        rows(DELETE_ROWS_EVENT, 1, 2, &[3], &[2, 8, 0, 0, 0]),
    ]);
    let expected = concat!(
        "after[0] = signed:7 unsigned:7\nafter[1] = Bytes([111, 108, 100])\n",
        "before[0] = signed:7 unsigned:7\nbefore[1] = Bytes([111, 108, 100])\n",
        "after[0] = signed:8 unsigned:8\nafter[1] = Null\n",
        "before[0] = signed:8 unsigned:8\nbefore[1] = Null\n",
    );
    for mode in [None, Some("minimal"), Some("full")] {
        let (result, output) = fixture.run(mode);
        result.unwrap();
        assert_eq!(output, expected.as_bytes());
    }
}

#[test]
fn minimal_accepts_sparse_operations_and_full_rejects_them_without_output() {
    let fixture = Fixture::new(&[
        table(&[3, 15], &[10, 0], 2),
        rows(WRITE_ROWS_EVENT, 0, 2, &[2], &[0, 3, b'o', b'l', b'd']),
        rows(UPDATE_ROWS_EVENT, 0, 2, &[1, 2], &[0, 7, 0, 0, 0, 1]),
        rows(DELETE_ROWS_EVENT, 1, 2, &[1], &[0, 7, 0, 0, 0]),
    ]);
    for mode in [None, Some("minimal")] {
        let (result, output) = fixture.run(mode);
        result.unwrap();
        assert_eq!(
            output,
            concat!(
                "after[1] = Bytes([111, 108, 100])\n",
                "before[0] = signed:7 unsigned:7\nafter[1] = Null\n",
                "before[0] = signed:7 unsigned:7\n",
            )
            .as_bytes()
        );
    }
    let (result, output) = fixture.run(Some("full"));
    assert!(matches!(result, Err(Error::Unsupported(message))
        if message == "FULL row images must contain every table column"));
    assert!(output.is_empty());
    let (result, output) = fixture.run(Some("invalid"));
    assert!(matches!(result, Err(Error::Invalid(message))
        if message == "row image must be minimal or full"));
    assert!(output.is_empty());
}

#[test]
fn table_remapping_replaces_the_layout_and_boundaries_clear_it() {
    let map = table(&[3], &[], 0);
    let row = rows(WRITE_ROWS_EVENT, 0, 1, &[1], &[0, 7, 0, 0, 0]);
    let fixture = Fixture::new(&[
        map.clone(),
        row.clone(),
        table(&[15], &[10, 0], 0),
        rows(WRITE_ROWS_EVENT, 0, 1, &[1], &[0, 3, b'n', b'e', b'w']),
    ]);
    let mut rotate = 4u64.to_le_bytes().to_vec();
    rotate.extend(b"binlog.000002");
    for mode in ["minimal", "full"] {
        let (result, output) = fixture.run(Some(mode));
        result.unwrap();
        assert_eq!(
            output,
            b"after[0] = signed:7 unsigned:7\nafter[0] = Bytes([110, 101, 119])\n"
        );
        for boundary in [
            rows(WRITE_ROWS_EVENT, 1, 1, &[1], &[0, 7, 0, 0, 0]),
            frame(ROTATE_EVENT, &rotate),
            format(),
        ] {
            let fixture = Fixture::new(&[map.clone(), boundary, row.clone()]);
            let (result, _) = fixture.run(Some(mode));
            assert!(
                matches!(result, Err(Error::Invalid(message)) if message == "missing TABLE_MAP for 1")
            );
        }
    }
}

#[test]
fn truncated_update_is_not_printed_and_output_errors_are_propagated() {
    let fixture = Fixture::new(&[
        table(&[3], &[], 0),
        rows(UPDATE_ROWS_EVENT, 0, 1, &[1, 1], &[0, 7, 0, 0, 0, 0, 8]),
    ]);
    for mode in ["minimal", "full"] {
        let (result, output) = fixture.run(Some(mode));
        assert!(matches!(result, Err(Error::Truncated { need: 4, have: 1 })));
        assert!(output.is_empty());
    }
    struct RejectOutput;
    impl Write for RejectOutput {
        fn write(&mut self, _: &[u8]) -> std::io::Result<usize> {
            Err(std::io::Error::from(std::io::ErrorKind::BrokenPipe))
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    let fixture = Fixture::new(&[
        table(&[3], &[], 0),
        rows(WRITE_ROWS_EVENT, 0, 1, &[1], &[0, 7, 0, 0, 0]),
    ]);
    assert!(
        matches!(run::<Full>(fixture.0.to_str().unwrap(), &mut RejectOutput),
        Err(Error::Io(error)) if error.kind() == std::io::ErrorKind::BrokenPipe)
    );
}
