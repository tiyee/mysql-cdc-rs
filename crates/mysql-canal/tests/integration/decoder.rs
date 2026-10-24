#[path = "../support/mod.rs"]
mod common;
use common::*;
use mysql_canal::{Decoder, Error, Operation, PrimaryKey, protocol::mysql::version::RowDecoder};
use std::{borrow::Cow, sync::Arc};

fn collect(ty: u8, data: &[u8]) -> Vec<mysql_canal::OwnedChange> {
    let mut decoder = Decoder::<mysql_canal::Minimal>::new(vec![basic()]).unwrap();
    decode_table(
        &mut decoder,
        &table_map(7, &basic(), &[3, 15, 3], &[100, 0]),
    )
    .unwrap();
    let mut out = Vec::new();
    decode_rows(&mut decoder, ty, data, |c| {
        out.push(c.into_owned());
        Ok(())
    })
    .unwrap();
    out
}

#[test]
fn minimal_update_and_null_are_sparse() {
    // Before: only id. After: name=NULL, age=30. NULL bits are indexed
    // by present-column ordinal, not absolute table ordinal.
    let mut data = vec![0];
    data.extend((-123i32).to_le_bytes());
    data.push(1);
    data.extend(30i32.to_le_bytes());
    let out = collect(
        UPDATE_ROWS_EVENT,
        &rows(7, UPDATE_ROWS_EVENT, 3, &[1], &[6], &data),
    );
    assert_eq!(out[0].pk, PrimaryKey::Signed(-123));
    assert_eq!(out[0].changed_names().collect::<Vec<_>>(), ["name", "age"]);
    assert_eq!(out[0].operation, Operation::Update);
}

#[test]
fn not_null_columns_reject_null_in_sparse_insert_and_update_images() {
    let meta = mysql_canal::TableMeta {
        schema: "app".into(),
        table: "wide".into(),
        columns: (0..80)
            .map(|i| {
                let mut col = column(&format!("c{i}"), "int", i == 69);
                if i == 79 {
                    col.nullable = false;
                }
                col
            })
            .collect(),
    };
    let mut key_columns = [0; 10];
    key_columns[8] = 1 << 5;
    for ty in [
        WRITE_ROWS_EVENT_V1,
        WRITE_ROWS_EVENT,
        UPDATE_ROWS_EVENT_V1,
        UPDATE_ROWS_EVENT,
    ] {
        let insert = matches!(ty, WRITE_ROWS_EVENT_V1 | WRITE_ROWS_EVENT);
        let mut present = [0; 10];
        present[0] = 1;
        present[9] = 1 << 7;
        if insert {
            present[8] = key_columns[8];
        }
        let mut decoder = Decoder::<mysql_canal::Minimal>::new(vec![meta.clone()]).unwrap();
        decode_table(&mut decoder, &table_map(1, &meta, &[3; 80], &[])).unwrap();
        for invalid in [true, false] {
            let mut data = Vec::new();
            if !insert {
                data.push(0);
                data.extend(42i32.to_le_bytes());
            }
            // Sparse NULL ordinals: c0 is nullable, c79 is NOT NULL.
            data.push(if invalid {
                if insert { 0b101 } else { 0b11 }
            } else {
                1
            });
            if insert {
                data.extend(42i32.to_le_bytes());
            }
            if !invalid {
                data.extend(7i32.to_le_bytes());
            }
            let body = rows(
                1,
                ty,
                80,
                if insert { &present } else { &key_columns },
                &present,
                &data,
            );
            let mut changes = Vec::new();
            let result = decode_rows(&mut decoder, ty, &body, |change| {
                changes.push(change.into_owned());
                Ok(())
            });
            if invalid {
                assert!(
                    matches!(result, Err(Error::Schema(message)) if message.contains("NOT NULL"))
                );
                assert!(changes.is_empty(), "invalid row emitted for event {ty}");
            } else {
                result.unwrap();
                assert_eq!(changes.len(), 1);
                assert_eq!(changes[0].pk, PrimaryKey::Signed(42));
                assert_eq!(changes[0].previous_pk, None);
                assert_eq!(
                    changes[0].operation,
                    if insert {
                        Operation::Insert
                    } else {
                        Operation::Update
                    }
                );
                assert_eq!(
                    changes[0].changed_names().collect::<Vec<_>>(),
                    if insert {
                        vec!["c0", "c69", "c79"]
                    } else {
                        vec!["c0", "c79"]
                    }
                );
            }
        }
    }
}

#[test]
fn multiple_rows_share_changed_indices_and_metadata() {
    let mut data = Vec::new();
    for id in [1i32, 2, 3] {
        data.push(0);
        data.extend(id.to_le_bytes());
        data.extend([0, 1, b'x']);
    }
    let out = collect(
        UPDATE_ROWS_EVENT,
        &rows(7, UPDATE_ROWS_EVENT, 3, &[1], &[2], &data),
    );
    assert_eq!(out.len(), 3);
    assert!(Arc::ptr_eq(&out[0].changed, &out[2].changed));
    assert!(Arc::ptr_eq(&out[0].table, &out[2].table));
    for side in [
        mysql_canal::RowOperation::before,
        mysql_canal::RowOperation::after,
    ] {
        let first = side(&out[0].fields).unwrap();
        let last = side(&out[2].fields).unwrap();
        assert_eq!(
            first.column_indices().as_ptr(),
            last.column_indices().as_ptr()
        );
        assert!(std::ptr::eq(first.columns(), last.columns()));
    }
    assert_eq!(out[2].pk, PrimaryKey::Signed(3));
}

#[test]
fn pk_change_retains_both_keys() {
    let mut data = vec![0];
    data.extend(1i32.to_le_bytes());
    data.push(0);
    data.extend(2i32.to_le_bytes());
    let out = collect(
        UPDATE_ROWS_EVENT,
        &rows(7, UPDATE_ROWS_EVENT, 3, &[1], &[1], &data),
    );
    assert_eq!(out[0].pk, PrimaryKey::Signed(2));
    assert_eq!(out[0].previous_pk, Some(PrimaryKey::Signed(1)));
    assert_eq!(out[0].changed_names().collect::<Vec<_>>(), ["id"]);
}

#[test]
fn insert_delete_v1_v2() {
    for ty in [WRITE_ROWS_EVENT_V1, WRITE_ROWS_EVENT] {
        let mut data = vec![0];
        data.extend(1i32.to_le_bytes());
        data.extend([1, b'a']);
        data.extend(18i32.to_le_bytes());
        let out = collect(ty, &rows(7, ty, 3, &[7], &[], &data));
        assert_eq!(
            out[0].changed_names().collect::<Vec<_>>(),
            ["id", "name", "age"]
        );
        assert_eq!(out[0].operation, Operation::Insert);
    }
    for ty in [DELETE_ROWS_EVENT_V1, DELETE_ROWS_EVENT] {
        let mut data = vec![0xfe];
        data.extend(1i32.to_le_bytes());
        let out = collect(ty, &rows(7, ty, 3, &[1], &[], &data));
        assert!(out[0].changed.is_empty());
        assert_eq!(out[0].operation, Operation::Delete);
    }
    let mut data = vec![0];
    data.extend(1i32.to_le_bytes());
    data.extend([0, 1, b'b']);
    assert_eq!(
        collect(
            UPDATE_ROWS_EVENT_V1,
            &rows(7, UPDATE_ROWS_EVENT_V1, 3, &[1], &[2], &data)
        )[0]
        .pk,
        PrimaryKey::Signed(1)
    );
}

#[test]
fn sparse_insert_after_column_64_uses_present_columns_and_compact_null_bits() {
    let meta = mysql_canal::TableMeta {
        schema: "app".into(),
        table: "wide".into(),
        columns: (0..80)
            .map(|i| {
                if i == 69 {
                    column("id", "bigint", true)
                } else {
                    column(&format!("c{i}"), "int", false)
                }
            })
            .collect(),
    };
    let mut types = vec![3; 80];
    types[69] = 8;
    let mut decoder = Decoder::<mysql_canal::Minimal>::new(vec![meta.clone()]).unwrap();
    decode_table(&mut decoder, &table_map(1, &meta, &types, &[])).unwrap();
    let mut present = [0; 10];
    for index in [0, 69, 79] {
        present[index / 8] |= 1 << (index % 8);
    }
    let mut data = vec![0];
    data.extend(11i32.to_le_bytes());
    data.extend((-42i64).to_le_bytes());
    data.extend(79i32.to_le_bytes());
    // Only three columns are present: bits 0 and 2 mark c0 and c79 NULL.
    // The primary key is the second present column, despite its table ordinal.
    data.push(0b101);
    data.extend((-43i64).to_le_bytes());
    let mut out = Vec::new();
    decode_rows(
        &mut decoder,
        WRITE_ROWS_EVENT,
        &rows(1, WRITE_ROWS_EVENT, 80, &present, &[], &data),
        |change| {
            out.push(change.into_owned());
            Ok(())
        },
    )
    .unwrap();
    assert_eq!(out.len(), 2);
    for (change, pk) in out.iter().zip([-42, -43]) {
        assert_eq!(change.operation, Operation::Insert);
        assert_eq!(change.pk, PrimaryKey::Signed(pk));
        assert_eq!(
            change.changed_names().collect::<Vec<_>>(),
            ["c0", "id", "c79"]
        );
    }
    assert!(Arc::ptr_eq(&out[0].changed, &out[1].changed));
}

#[test]
fn every_signed_and_unsigned_integer_width() {
    for (sql, ty, n) in [
        ("tinyint", 1, 1),
        ("smallint", 2, 2),
        ("mediumint", 9, 3),
        ("int", 3, 4),
        ("bigint", 8, 8),
    ] {
        for unsigned in [false, true] {
            let mut col = column("id", sql, true);
            if unsigned {
                col.column_type.push_str(" unsigned");
            }
            let meta = mysql_canal::TableMeta {
                schema: "app".into(),
                table: "keys".into(),
                columns: vec![col],
            };
            let mut decoder = Decoder::<mysql_canal::Minimal>::new(vec![meta.clone()]).unwrap();
            decode_table(&mut decoder, &table_map(1, &meta, &[ty], &[])).unwrap();
            for bytes in [vec![0xff; n], {
                let mut b = vec![0; n];
                b[n - 1] = 0x80;
                b
            }] {
                let mut data = vec![0];
                data.extend(&bytes);
                decode_rows(
                    &mut decoder,
                    WRITE_ROWS_EVENT,
                    &rows(1, WRITE_ROWS_EVENT, 1, &[1], &[], &data),
                    |c| {
                        let mut raw = [0; 8];
                        raw[..n].copy_from_slice(&bytes);
                        let raw = u64::from_le_bytes(raw);
                        let expected = if unsigned {
                            PrimaryKey::Unsigned(raw)
                        } else {
                            let shift = (8 - n) * 8;
                            PrimaryKey::Signed(((raw << shift) as i64) >> shift)
                        };
                        assert_eq!(c.pk, expected);
                        Ok(())
                    },
                )
                .unwrap();
            }
        }
    }
}

#[test]
fn utf8_pk_borrows_payload_and_invalid_utf8_fails() {
    let mut meta = basic();
    meta.columns[0].primary_key = false;
    meta.columns[0].nullable = true;
    meta.columns[1].primary_key = true;
    meta.columns[1].nullable = false;
    let mut decoder = Decoder::<mysql_canal::Minimal>::new(vec![meta.clone()]).unwrap();
    decode_table(&mut decoder, &table_map(1, &meta, &[3, 15, 3], &[100, 0])).unwrap();
    decode_rows(
        &mut decoder,
        DELETE_ROWS_EVENT,
        &rows(
            1,
            DELETE_ROWS_EVENT,
            3,
            &[2],
            &[],
            &[0, 3, b'a', b'b', b'c'],
        ),
        |c| {
            assert!(matches!(c.pk, PrimaryKey::Text(Cow::Borrowed("abc"))));
            Ok(())
        },
    )
    .unwrap();
    assert!(matches!(
        decode_rows(
            &mut decoder,
            DELETE_ROWS_EVENT,
            &rows(1, DELETE_ROWS_EVENT, 3, &[2], &[], &[0, 1, 0xff]),
            |_| Ok(())
        ),
        Err(Error::Utf8(_))
    ));
}

#[test]
fn decimal_key_is_exact() {
    let mut col = column("id", "decimal", true);
    col.numeric_precision = Some(10);
    col.numeric_scale = Some(2);
    let meta = mysql_canal::TableMeta {
        schema: "app".into(),
        table: "money".into(),
        columns: vec![col],
    };
    let mut decoder = Decoder::<mysql_canal::Minimal>::new(vec![meta.clone()]).unwrap();
    decode_table(&mut decoder, &table_map(1, &meta, &[246], &[10, 2])).unwrap();
    // DECIMAL(10,2): one 4-byte integral group + one 1-byte fractional group.
    for (bytes, expected) in [
        ([0x80, 0xbc, 0x61, 0x4e, 0x5a], "12345678.90"),
        ([0x7f, 0x43, 0x9e, 0xb1, 0xa5], "-12345678.90"),
    ] {
        let mut data = vec![0];
        data.extend(bytes);
        decode_rows(
            &mut decoder,
            DELETE_ROWS_EVENT,
            &rows(1, DELETE_ROWS_EVENT, 1, &[1], &[], &data),
            |c| {
                assert_eq!(c.pk, PrimaryKey::Decimal(expected.into()));
                Ok(())
            },
        )
        .unwrap();
    }
}

#[test]
fn more_than_64_columns_and_pk_after_nulls() {
    let columns = (0..300)
        .map(|i| column(&format!("c{i}"), "int", i == 299))
        .collect();
    let meta = mysql_canal::TableMeta {
        schema: "app".into(),
        table: "wide".into(),
        columns,
    };
    let mut decoder = Decoder::<mysql_canal::Minimal>::new(vec![meta.clone()]).unwrap();
    decode_table(&mut decoder, &table_map(1, &meta, &vec![3; 300], &[])).unwrap();
    let mut present = vec![0xff; 38];
    present[37] = 0x0f;
    let mut nulls = vec![0xff; 38];
    nulls[37] &= !(1 << 3);
    nulls.extend((-8i32).to_le_bytes());
    decode_rows(
        &mut decoder,
        WRITE_ROWS_EVENT,
        &rows(1, WRITE_ROWS_EVENT, 300, &present, &[], &nulls),
        |c| {
            assert_eq!(c.pk, PrimaryKey::Signed(-8));
            assert_eq!(c.changed.len(), 300);
            Ok(())
        },
    )
    .unwrap();
}

#[test]
fn mixed_fields_before_key_preserve_wire_values() {
    let mut cols = Vec::new();
    let mut types = Vec::new();
    let mut metadata = Vec::new();
    let mut values = Vec::new();
    for (sql, ty, meta, value) in [
        ("float", 4, vec![4], vec![0; 4]),
        ("double", 5, vec![8], vec![0; 8]),
        ("datetime", 18, vec![6], vec![0; 8]),
        ("time", 19, vec![1], vec![0; 4]),
        ("timestamp", 17, vec![2], vec![0; 5]),
        ("bit", 16, vec![1, 1], vec![0; 2]),
        ("decimal", 246, vec![12, 4], vec![0x80, 0, 0, 0, 0, 0]),
        ("enum", 254, vec![247, 2], vec![0; 2]),
        ("set", 254, vec![248, 3], vec![0; 3]),
        ("blob", 252, vec![2], vec![3, 0, 1, 2, 3]),
        ("json", 245, vec![4], vec![2, 0, 0, 0, 0xff, 0xff]),
        ("char", 254, vec![0xee, 0x2c], {
            let mut b = vec![4, 0];
            b.extend(b"abcd");
            b
        }),
    ] {
        let mut col = column(&format!("c{}", cols.len()), sql, false);
        match sql {
            "datetime" => col.datetime_precision = Some(6),
            "time" => col.datetime_precision = Some(1),
            "timestamp" => col.datetime_precision = Some(2),
            "bit" => col.numeric_precision = Some(9),
            "decimal" => {
                col.numeric_precision = Some(12);
                col.numeric_scale = Some(4);
            }
            "char" => col.octet_length = Some(300),
            _ => {}
        }
        cols.push(col);
        types.push(ty);
        metadata.extend(meta);
        values.extend(value);
    }
    cols.push(column("id", "bigint", true));
    types.push(8);
    values.extend(i64::MIN.to_le_bytes());
    let meta = mysql_canal::TableMeta {
        schema: "app".into(),
        table: "mixed".into(),
        columns: cols,
    };
    let mut decoder = Decoder::<mysql_canal::Minimal>::new(vec![meta.clone()]).unwrap();
    decode_table(&mut decoder, &table_map(1, &meta, &types, &metadata)).unwrap();
    let mut data = vec![0; 2];
    data.extend(values);
    decode_rows(
        &mut decoder,
        WRITE_ROWS_EVENT,
        &rows(1, WRITE_ROWS_EVENT, 13, &[0xff, 0x1f], &[], &data),
        |c| {
            assert_eq!(c.pk, PrimaryKey::Signed(i64::MIN));
            Ok(())
        },
    )
    .unwrap();
}

#[test]
fn excluded_table_remap_does_not_reuse_whitelist_handler() {
    let mut decoder = Decoder::<mysql_canal::Minimal>::new(vec![basic()]).unwrap();
    decode_table(
        &mut decoder,
        &table_map(1, &basic(), &[3, 15, 3], &[100, 0]),
    )
    .unwrap();
    let mut excluded = basic();
    excluded.schema = "other".into();
    decode_table(
        &mut decoder,
        &table_map(1, &excluded, &[255, 255, 255], &[]),
    )
    .unwrap();
    decode_rows(
        &mut decoder,
        WRITE_ROWS_EVENT,
        &rows(1, WRITE_ROWS_EVENT, 0, &[], &[], &[0xff]),
        |_| panic!("excluded table decoded"),
    )
    .unwrap();
    decoder.clear_table_maps();
    assert!(matches!(
        decode_rows(
            &mut decoder,
            WRITE_ROWS_EVENT,
            &rows(1, WRITE_ROWS_EVENT, 0, &[], &[], &[]),
            |_| Ok(())
        ),
        Err(Error::MissingTable(1))
    ));
}

#[test]
fn statement_end_clears_maps_and_sink_errors_propagate() {
    let mut decoder = Decoder::<mysql_canal::Minimal>::new(vec![basic()]).unwrap();
    let map = table_map(1, &basic(), &[3, 15, 3], &[100, 0]);
    decode_table(&mut decoder, &map).unwrap();
    let data = rows(1, DELETE_ROWS_EVENT, 3, &[1], &[], &[0, 1, 0, 0, 0]);
    assert!(matches!(
        decode_rows(&mut decoder, DELETE_ROWS_EVENT, &data, |_| Err(
            Error::MissingPrimaryKey
        )),
        Err(Error::MissingPrimaryKey)
    ));
    let mut ended = data.clone();
    ended[6] = 1;
    decode_rows(&mut decoder, DELETE_ROWS_EVENT, &ended, |_| Ok(())).unwrap();
    assert!(matches!(
        decode_rows(&mut decoder, DELETE_ROWS_EVENT, &data, |_| Ok(())),
        Err(Error::MissingTable(1))
    ));
}

#[test]
fn missing_null_pk_and_full_images_fail() {
    assert!(Decoder::<mysql_canal::Minimal>::new(vec![]).is_err());
    let mut composite = basic();
    composite.columns[2].primary_key = true;
    assert!(Decoder::<mysql_canal::Minimal>::new(vec![composite]).is_err());
    let mut decoder = Decoder::<mysql_canal::Minimal>::new(vec![basic()]).unwrap();
    decode_table(
        &mut decoder,
        &table_map(1, &basic(), &[3, 15, 3], &[100, 0]),
    )
    .unwrap();
    assert!(matches!(
        decode_rows(
            &mut decoder,
            DELETE_ROWS_EVENT,
            &rows(1, DELETE_ROWS_EVENT, 3, &[1], &[], &[1]),
            |_| Ok(())
        ),
        Err(Error::MissingPrimaryKey)
    ));
    assert!(
        decode_rows(
            &mut decoder,
            DELETE_ROWS_EVENT,
            &rows(1, DELETE_ROWS_EVENT, 3, &[2], &[], &[0, 0]),
            |_| Ok(())
        )
        .is_err()
    );
    assert!(
        decode_rows(
            &mut decoder,
            UPDATE_ROWS_EVENT,
            &rows(1, UPDATE_ROWS_EVENT, 3, &[7], &[7], &[0]),
            |_| Ok(())
        )
        .is_err()
    );
}

#[test]
fn truncated_data_and_schema_mismatch_are_errors() {
    let map = table_map(1, &basic(), &[3, 15, 3], &[100, 0]);
    for len in 0..map.len() {
        let mut decoder = Decoder::<mysql_canal::Minimal>::new(vec![basic()]).unwrap();
        assert!(decode_table(&mut decoder, &map[..len]).is_err());
    }
    let mut decoder = Decoder::<mysql_canal::Minimal>::new(vec![basic()]).unwrap();
    decode_table(&mut decoder, &map).unwrap();
    let data = rows(1, DELETE_ROWS_EVENT, 3, &[1], &[], &[0, 1, 0, 0, 0]);
    for len in 0..data.len() {
        assert!(decode_rows(&mut decoder, DELETE_ROWS_EVENT, &data[..len], |_| Ok(())).is_err());
    }
    assert!(
        decode_table(
            &mut decoder,
            &table_map(1, &basic(), &[8, 15, 3], &[100, 0])
        )
        .is_err()
    );
    assert!(
        decode_table(
            &mut decoder,
            &table_map(1, &basic(), &[3, 15, 3], &[101, 0])
        )
        .is_err()
    );
}

#[test]
fn malformed_inputs_do_not_panic() {
    let mut seed = 7u64;
    for length in 0..150 {
        for _ in 0..30 {
            let data: Vec<_> = (0..length)
                .map(|_| {
                    seed ^= seed << 13;
                    seed ^= seed >> 7;
                    seed ^= seed << 17;
                    seed as u8
                })
                .collect();
            let mut decoder = Decoder::<mysql_canal::Minimal>::new(vec![basic()]).unwrap();
            let _ = decode_table(&mut decoder, &data);
            let _ = decode_rows(&mut decoder, UPDATE_ROWS_EVENT, &data, |_| Ok(()));
        }
    }
}

#[test]
fn char_latin1_ascii_and_two_byte_string_prefix() {
    for (ty, charset, bytes, expected) in [
        ("char", "utf8mb4", b"abc   ".as_slice(), "abc"),
        ("varchar", "latin1", [0x80].as_slice(), "€"),
        ("varchar", "ascii", b"abc".as_slice(), "abc"),
    ] {
        let mut key = column("id", ty, true);
        key.charset = Some(charset.into());
        key.octet_length = Some(300);
        let meta = mysql_canal::TableMeta {
            schema: "app".into(),
            table: "keys".into(),
            columns: vec![key],
        };
        let mut decoder = Decoder::<mysql_canal::Minimal>::new(vec![meta.clone()]).unwrap();
        let (wire, metadata) = if ty == "char" {
            (254, [0xee, 0x2c])
        } else {
            (15, [0x2c, 1])
        };
        decode_table(&mut decoder, &table_map(1, &meta, &[wire], &metadata)).unwrap();
        let mut data = vec![0];
        data.extend((bytes.len() as u16).to_le_bytes());
        data.extend(bytes);
        decode_rows(
            &mut decoder,
            DELETE_ROWS_EVENT,
            &rows(1, DELETE_ROWS_EVENT, 1, &[1], &[], &data),
            |c| {
                assert_eq!(c.pk, PrimaryKey::Text(expected.into()));
                Ok(())
            },
        )
        .unwrap();
    }
}

#[test]
fn decimal_groups_fractional_only_large_values_and_invalid_groups() {
    for text in [
        "0.00001",
        "-0.00001",
        "0.00000",
        "99999999999999999999999999999999999.999999999999999999999999999999",
        "1000000000.000000001",
        "-1000000000.000000001",
    ] {
        let magnitude = text.trim_start_matches('-');
        let (int, frac) = magnitude.split_once('.').unwrap();
        let mut key = column("id", "decimal", true);
        key.numeric_precision = Some((int.len() + frac.len()) as u32);
        key.numeric_scale = Some(frac.len() as u32);
        let meta = mysql_canal::TableMeta {
            schema: "app".into(),
            table: "keys".into(),
            columns: vec![key],
        };
        let mut packed = vec![0];
        text.parse::<mysql_common::binlog::decimal::Decimal>()
            .unwrap()
            .write_bin(&mut packed)
            .unwrap();
        let mut decoder = Decoder::<mysql_canal::Minimal>::new(vec![meta.clone()]).unwrap();
        decode_table(
            &mut decoder,
            &table_map(
                1,
                &meta,
                &[246],
                &[(int.len() + frac.len()) as u8, frac.len() as u8],
            ),
        )
        .unwrap();
        decode_rows(
            &mut decoder,
            DELETE_ROWS_EVENT,
            &rows(1, DELETE_ROWS_EVENT, 1, &[1], &[], &packed),
            |c| {
                assert_eq!(c.pk, PrimaryKey::Decimal(text.into()));
                Ok(())
            },
        )
        .unwrap();
        let mut invalid = vec![0xff; packed.len()];
        invalid[0] = 0;
        assert!(
            decode_rows(
                &mut decoder,
                DELETE_ROWS_EVENT,
                &rows(1, DELETE_ROWS_EVENT, 1, &[1], &[], &invalid),
                |_| Ok(())
            )
            .is_err()
        );
    }
}

fn full_image(id: i32, name: Option<&str>, age: i32) -> Vec<u8> {
    let mut data = vec![if name.is_none() { 2 } else { 0 }];
    data.extend(id.to_le_bytes());
    if let Some(name) = name {
        data.push(name.len() as u8);
        data.extend(name.as_bytes());
    }
    data.extend(age.to_le_bytes());
    data
}

#[test]
fn full_v1_v2_images_preserve_operations_keys_nulls_and_shared_changed_fields() {
    for ty in [
        WRITE_ROWS_EVENT_V1,
        WRITE_ROWS_EVENT,
        UPDATE_ROWS_EVENT_V1,
        UPDATE_ROWS_EVENT,
        DELETE_ROWS_EVENT_V1,
        DELETE_ROWS_EVENT,
    ] {
        let update = matches!(ty, UPDATE_ROWS_EVENT_V1 | UPDATE_ROWS_EVENT);
        let delete = matches!(ty, DELETE_ROWS_EVENT_V1 | DELETE_ROWS_EVENT);
        let mut data = Vec::new();
        for id in [1, 2] {
            data.extend(full_image(id, Some("old"), 20));
            if update {
                data.extend(full_image(if id == 1 { 10 } else { id }, None, 20));
            }
        }
        let mut decoder = Decoder::<mysql_canal::Full>::new(vec![basic()]).unwrap();
        decode_table(
            &mut decoder,
            &table_map(7, &basic(), &[3, 15, 3], &[100, 0]),
        )
        .unwrap();
        let mut out = Vec::new();
        decode_rows(
            &mut decoder,
            ty,
            &rows(7, ty, 3, &[0xff], &[0xff], &data),
            |c| {
                out.push(c.into_owned());
                Ok(())
            },
        )
        .unwrap();
        assert_eq!(out.len(), 2);
        assert_eq!(out[0].pk, PrimaryKey::Signed(if update { 10 } else { 1 }));
        assert_eq!(out[0].previous_pk, update.then_some(PrimaryKey::Signed(1)));
        assert_eq!(out[1].pk, PrimaryKey::Signed(2));
        assert_eq!(out[1].previous_pk, None);
        for change in &out {
            assert_eq!(
                change.operation,
                if update {
                    Operation::Update
                } else if delete {
                    Operation::Delete
                } else {
                    Operation::Insert
                }
            );
            assert_eq!(
                change.changed_names().collect::<Vec<_>>(),
                if delete {
                    vec![]
                } else {
                    vec!["id", "name", "age"]
                }
            );
        }
        assert!(Arc::ptr_eq(&out[0].changed, &out[1].changed));
        for (index, change) in out.iter().enumerate() {
            let original_id = index as i32 + 1;
            assert_eq!(change.fields.before().is_some(), update || delete);
            assert_eq!(change.fields.after().is_some(), !delete);
            for image in [change.fields.before(), change.fields.after()]
                .into_iter()
                .flatten()
            {
                assert_eq!(image.column_indices(), [0, 1, 2]);
                assert_eq!(image.columns(), basic().columns);
                assert_eq!(image.fields().count(), 3);
            }
            let serialized = serde_json::to_value(change).unwrap();
            if update || delete {
                assert_eq!(serialized["fields"]["before"][0]["value"], original_id);
                assert_eq!(serialized["fields"]["before"][1]["value"], "old");
                assert_eq!(serialized["fields"]["before"][2]["value"], 20);
            }
            if !delete {
                assert_eq!(
                    serialized["fields"]["after"][0]["value"],
                    if update && index == 0 {
                        10
                    } else {
                        original_id
                    }
                );
                assert_eq!(
                    serialized["fields"]["after"][1]["value"],
                    if update {
                        serde_json::Value::Null
                    } else {
                        serde_json::json!("old")
                    }
                );
                assert_eq!(serialized["fields"]["after"][2]["value"], 20);
            }
        }
    }
}

#[test]
fn image_policies_reject_mismatched_bitmaps_before_emitting() {
    use mysql_canal::config::RowImageMode;
    for (mode, ty, first, second) in [
        (RowImageMode::Minimal, UPDATE_ROWS_EVENT, 7, 7),
        (RowImageMode::Minimal, DELETE_ROWS_EVENT, 7, 7),
        (RowImageMode::Full, WRITE_ROWS_EVENT, 1, 1),
        (RowImageMode::Full, DELETE_ROWS_EVENT, 1, 1),
        (RowImageMode::Full, UPDATE_ROWS_EVENT, 1, 7),
        (RowImageMode::Full, UPDATE_ROWS_EVENT, 7, 2),
    ] {
        let mut decoder = match mode {
            RowImageMode::Minimal => {
                Box::new(Decoder::<mysql_canal::Minimal>::new(vec![basic()]).unwrap())
                    as Box<dyn RowDecoder>
            }
            RowImageMode::Full => {
                Box::new(Decoder::<mysql_canal::Full>::new(vec![basic()]).unwrap())
                    as Box<dyn RowDecoder>
            }
        };
        decode_table(
            decoder.as_mut(),
            &table_map(7, &basic(), &[3, 15, 3], &[100, 0]),
        )
        .unwrap();
        let mut emitted = 0;
        let result = decode_rows(
            decoder.as_mut(),
            ty,
            &rows(7, ty, 3, &[first], &[second], &full_image(1, None, 20)),
            |_| {
                emitted += 1;
                Ok(())
            },
        );
        assert!(
            matches!(result, Err(Error::Unsupported(_))),
            "{mode} / {ty}: {result:?}"
        );
        assert_eq!(emitted, 0);
    }
}

#[test]
fn full_images_validate_null_constraints_in_both_images_and_truncated_values() {
    let mut meta = basic();
    meta.columns[2].nullable = false;
    for (before, after, missing_pk) in [
        (true, false, false),
        (false, true, false),
        (true, false, true),
        (false, true, true),
    ] {
        let valid = full_image(1, Some("x"), 20);
        let mut invalid = vec![if missing_pk { 1 } else { 4 }];
        if !missing_pk {
            invalid.extend(1i32.to_le_bytes());
        }
        invalid.extend([1, b'x']);
        if missing_pk {
            invalid.extend(20i32.to_le_bytes());
        }
        let mut values = if before {
            invalid.clone()
        } else {
            valid.clone()
        };
        values.extend(if after { invalid } else { valid });
        let mut decoder = Decoder::<mysql_canal::Full>::new(vec![meta.clone()]).unwrap();
        decode_table(&mut decoder, &table_map(7, &meta, &[3, 15, 3], &[100, 0])).unwrap();
        let result = decode_rows(
            &mut decoder,
            UPDATE_ROWS_EVENT,
            &rows(7, UPDATE_ROWS_EVENT, 3, &[7], &[7], &values),
            |_| panic!("invalid row emitted"),
        );
        if missing_pk {
            assert!(matches!(result, Err(Error::MissingPrimaryKey)));
        } else {
            assert!(matches!(result, Err(Error::Schema(message)) if message.contains("NOT NULL")));
        }
    }
    let values = full_image(1, Some("abc"), 20);
    for len in 0..values.len() {
        let mut decoder = Decoder::<mysql_canal::Full>::new(vec![meta.clone()]).unwrap();
        decode_table(&mut decoder, &table_map(7, &meta, &[3, 15, 3], &[100, 0])).unwrap();
        let result = decode_rows(
            &mut decoder,
            WRITE_ROWS_EVENT,
            &rows(7, WRITE_ROWS_EVENT, 3, &[7], &[], &values[..len]),
            |_| panic!("truncated row emitted"),
        );
        if len == 0 {
            assert!(matches!(result, Err(Error::Invalid(_))));
        } else {
            assert!(matches!(result, Err(Error::Truncated { .. })));
        }
    }
}

#[test]
fn full_wide_images_keep_late_primary_keys_and_null_bitmap_ordinals() {
    let meta = mysql_canal::TableMeta {
        schema: "app".into(),
        table: "wide".into(),
        columns: (0..80)
            .map(|i| column(&format!("c{i}"), "int", i == 69))
            .collect(),
    };
    let mut data = Vec::new();
    for pk in [42i32, 43] {
        let mut nulls = [0xff; 10];
        nulls[8] &= !(1 << 5);
        data.extend(nulls);
        data.extend(pk.to_le_bytes());
    }
    let mut decoder = Decoder::<mysql_canal::Full>::new(vec![meta.clone()]).unwrap();
    decode_table(&mut decoder, &table_map(7, &meta, &[3; 80], &[])).unwrap();
    decode_rows(
        &mut decoder,
        UPDATE_ROWS_EVENT,
        &rows(7, UPDATE_ROWS_EVENT, 80, &[0xff; 10], &[0xff; 10], &data),
        |change| {
            assert_eq!(change.pk, PrimaryKey::Signed(43));
            assert_eq!(change.previous_pk, Some(PrimaryKey::Signed(42)));
            assert_eq!(change.changed.as_ref(), &(0..80).collect::<Vec<u16>>());
            Ok(())
        },
    )
    .unwrap();
}

#[test]
fn statically_selected_decoders_validate_mode_before_emitting() {
    use mysql_canal::{Full, Minimal};
    let mapping = table_map(7, &basic(), &[3, 15, 3], &[100, 0]);
    let mut full = Decoder::<Full>::new(vec![basic()]).unwrap();
    let mut minimal = Decoder::<Minimal>::new(vec![basic()]).unwrap();
    decode_table(&mut full, &mapping).unwrap();
    decode_table(&mut minimal, &mapping).unwrap();
    let sparse = rows(
        7,
        UPDATE_ROWS_EVENT,
        3,
        &[1],
        &[4],
        &[0, 42, 0, 0, 0, 0, 30, 0, 0, 0],
    );
    let mut emitted = Vec::new();
    assert!(
        matches!(decode_rows(&mut full, UPDATE_ROWS_EVENT, &sparse, |_| panic!("invalid FULL cannot emit")),
        Err(Error::Unsupported(message)) if message == "expected FULL row images containing every column")
    );
    decode_rows(&mut minimal, UPDATE_ROWS_EVENT, &sparse, |change| {
        emitted.push(change.into_owned());
        Ok(())
    })
    .unwrap();
    assert_eq!(emitted.len(), 1);
    assert_eq!(emitted[0].pk, PrimaryKey::Signed(42));
    assert_eq!(emitted[0].changed_names().collect::<Vec<_>>(), ["age"]);
    let mut values = vec![0];
    values.extend(42i32.to_le_bytes());
    values.extend([1, b'x']);
    values.extend(20i32.to_le_bytes());
    values.push(0);
    values.extend(43i32.to_le_bytes());
    values.extend([1, b'y']);
    values.extend(30i32.to_le_bytes());
    let complete = rows(7, UPDATE_ROWS_EVENT, 3, &[7], &[7], &values);
    assert!(
        matches!(decode_rows(&mut minimal, UPDATE_ROWS_EVENT, &complete, |_| panic!("invalid MINIMAL cannot emit")),
        Err(Error::Unsupported(message)) if message == "expected MINIMAL before image containing only the primary key")
    );
    emitted.clear();
    decode_rows(&mut full, UPDATE_ROWS_EVENT, &complete, |change| {
        emitted.push(change.into_owned());
        Ok(())
    })
    .unwrap();
    assert_eq!(emitted.len(), 1);
    assert_eq!(emitted[0].pk, PrimaryKey::Signed(43));
    assert_eq!(emitted[0].previous_pk, Some(PrimaryKey::Signed(42)));
    assert_eq!(
        emitted[0].changed_names().collect::<Vec<_>>(),
        ["id", "name", "age"]
    );
}

#[test]
fn field_images_borrow_values_and_owned_changes_survive_event_reuse() {
    fn run<M: mysql_canal::CdcImageMode>() {
        use mysql_canal::{RowValue, config::RowImageMode};
        let mut decoder = Decoder::<M>::new(vec![basic()]).unwrap();
        decode_table(
            &mut decoder,
            &table_map(7, &basic(), &[3, 15, 3], &[100, 0]),
        )
        .unwrap();
        let full = M::ROW_IMAGE == RowImageMode::Full;
        let mut data = if full {
            full_image(7, Some("old"), 20)
        } else {
            let mut bytes = vec![0];
            bytes.extend(7i32.to_le_bytes());
            bytes
        };
        data.push(if full { 4 } else { 2 });
        if full {
            data.extend(7i32.to_le_bytes());
        }
        data.extend([3, b'B', b'o', b'b']);
        let mut body = rows(
            7,
            UPDATE_ROWS_EVENT,
            3,
            if full { &[7] } else { &[1] },
            if full { &[7] } else { &[6] },
            &data,
        );
        let mut captured = None;
        let mut borrowed_json = None;
        decode_rows(&mut decoder, UPDATE_ROWS_EVENT, &body, |change| {
            let before = change.fields.before().unwrap();
            let after = change.fields.after().unwrap();
            assert_eq!(
                before.column_indices(),
                if full { &[0, 1, 2][..] } else { &[0][..] }
            );
            assert_eq!(
                after.column_indices(),
                if full { &[0, 1, 2][..] } else { &[1, 2][..] }
            );
            assert_eq!(after.columns().len(), 3);
            assert_eq!(after.field(0)?.is_some(), full);
            let name = after.field(1)?.unwrap();
            assert_eq!(name.index, 1);
            assert!(std::ptr::eq(name.column, &change.table.columns[1]));
            let RowValue::Bytes(value) = name.value else {
                panic!("expected borrowed name")
            };
            assert_eq!(value, b"Bob");
            assert!(
                (body.as_ptr() as usize..body.as_ptr() as usize + body.len())
                    .contains(&(value.as_ptr() as usize))
            );
            assert_eq!(after.field(2)?.unwrap().value, RowValue::Null);
            assert!(matches!(after.field(3), Err(Error::Invalid(_))));
            borrowed_json = Some(serde_json::to_value(&change).unwrap());
            captured = Some(change.into_owned());
            Ok(())
        })
        .unwrap();
        body.fill(0xff);
        drop(body);
        drop(decoder);
        let change = captured.unwrap();
        assert_eq!(
            serde_json::to_value(&change).unwrap(),
            borrowed_json.unwrap()
        );
        let after = change.fields.after().unwrap();
        assert_eq!(
            after.field(1).unwrap().unwrap().value,
            RowValue::Bytes(b"Bob")
        );
        assert_eq!(after.field(2).unwrap().unwrap().value, RowValue::Null);
        assert!(std::ptr::eq(
            after.field(1).unwrap().unwrap().column,
            &change.table.columns[1]
        ));
    }
    run::<mysql_canal::Minimal>();
    run::<mysql_canal::Full>();
}

#[test]
fn field_json_preserves_exact_numbers_text_binary_and_encoded_values() {
    let mut id = column("id", "bigint", true);
    id.column_type = "bigint unsigned".into();
    let mut amount = column("amount", "decimal", false);
    amount.numeric_precision = Some(10);
    amount.numeric_scale = Some(2);
    let mut text = column("label", "varchar", false);
    text.charset = Some("utf8mb4".into());
    text.octet_length = Some(100);
    let mut binary = column("binary", "varbinary", false);
    binary.octet_length = Some(8);
    let mut latin = column("latin", "varchar", false);
    latin.charset = Some("latin1".into());
    latin.octet_length = Some(100);
    let mut bits = column("bits", "bit", false);
    bits.numeric_precision = Some(9);
    let meta = mysql_canal::TableMeta {
        schema: "app".into(),
        table: "values".into(),
        columns: vec![
            id,
            amount,
            text,
            binary,
            column("document", "json", false),
            latin,
            column("real", "double", false),
            bits,
            column("choice", "enum", false),
            column("tags", "set", false),
            column("year", "year", false),
        ],
    };
    let mut decoder = Decoder::<mysql_canal::Minimal>::new(vec![meta.clone()]).unwrap();
    decode_table(
        &mut decoder,
        &table_map(
            1,
            &meta,
            &[8, 246, 15, 15, 245, 15, 5, 16, 254, 254, 13],
            &[10, 2, 100, 0, 8, 0, 4, 100, 0, 8, 1, 1, 247, 1, 248, 1],
        ),
    )
    .unwrap();
    let mut data = vec![0, 0];
    data.extend(u64::MAX.to_le_bytes());
    data.extend([0x80, 0xbc, 0x61, 0x4e, 0x5a]);
    data.push(3);
    data.extend("文".as_bytes());
    data.extend([2, 0, 255]);
    data.extend(2u32.to_le_bytes());
    data.extend([255, 0]);
    data.extend([1, 0xe9]);
    data.extend(1.25f64.to_le_bytes());
    data.extend([1, 255, 2, 5, 126]);
    let mut emitted = 0;
    decode_rows(
        &mut decoder,
        WRITE_ROWS_EVENT,
        &rows(1, WRITE_ROWS_EVENT, 11, &[255, 7], &[], &data),
        |change| {
            let json = serde_json::to_value(&change).unwrap();
            assert_eq!(json["pk"]["value"], serde_json::json!(u64::MAX));
            let fields = json["fields"]["after"].as_array().unwrap();
            assert_eq!(
                fields
                    .iter()
                    .map(|f| f["value"].clone())
                    .collect::<Vec<_>>(),
                vec![
                    serde_json::json!(u64::MAX),
                    serde_json::json!("12345678.90"),
                    serde_json::json!("文"),
                    serde_json::json!([0, 255]),
                    serde_json::json!([255, 0]),
                    serde_json::json!("é"),
                    serde_json::json!(1.25),
                    serde_json::json!(511),
                    serde_json::json!(2),
                    serde_json::json!(5),
                    serde_json::json!(2026)
                ]
            );
            for (index, field) in fields.iter().enumerate() {
                assert_eq!(field["index"], index);
                assert_eq!(
                    field["column"],
                    serde_json::to_value(&meta.columns[index]).unwrap()
                );
            }
            assert!(json["fields"].get("before").is_none());
            assert_eq!(serde_json::to_value(change.into_owned()).unwrap(), json);
            emitted += 1;
            Ok(())
        },
    )
    .unwrap();
    assert_eq!(emitted, 1);
}

#[test]
fn malformed_non_key_decimal_is_rejected_before_delivery() {
    let mut amount = column("amount", "decimal", false);
    amount.numeric_precision = Some(10);
    amount.numeric_scale = Some(2);
    let meta = mysql_canal::TableMeta {
        schema: "app".into(),
        table: "money".into(),
        columns: vec![column("id", "int", true), amount],
    };
    let mut decoder = Decoder::<mysql_canal::Minimal>::new(vec![meta.clone()]).unwrap();
    decode_table(&mut decoder, &table_map(1, &meta, &[3, 246], &[10, 2])).unwrap();
    let mut bytes = vec![0];
    bytes.extend(42i32.to_le_bytes());
    bytes.extend([255; 5]);
    let mut delivered = 0;
    let result = decode_rows(
        &mut decoder,
        WRITE_ROWS_EVENT,
        &rows(1, WRITE_ROWS_EVENT, 2, &[3], &[], &bytes),
        |_| {
            delivered += 1;
            Ok(())
        },
    );
    assert!(
        matches!(result, Err(Error::Invalid(message)) if message == "invalid packed DECIMAL digit group")
    );
    assert_eq!(delivered, 0);
}
