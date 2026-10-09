#[path = "../support/mod.rs"]
mod common;
use common::*;
use mysql_canal::{Decoder, Error, Operation, PrimaryKey};
use std::{borrow::Cow, sync::Arc};

fn collect(ty: u8, data: &[u8]) -> Vec<mysql_canal::OwnedChange> {
    let mut decoder = Decoder::new(vec![basic()]).unwrap();
    decoder
        .table_map(&table_map(7, &basic(), &[3, 15, 3], &[100, 0]))
        .unwrap();
    let mut out = Vec::new();
    decoder
        .rows(ty, data, |c| {
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
    let mut decoder = Decoder::new(vec![meta.clone()]).unwrap();
    decoder
        .table_map(&table_map(1, &meta, &types, &[]))
        .unwrap();
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
    decoder
        .rows(
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
            let mut decoder = Decoder::new(vec![meta.clone()]).unwrap();
            decoder.table_map(&table_map(1, &meta, &[ty], &[])).unwrap();
            for bytes in [vec![0xff; n], {
                let mut b = vec![0; n];
                b[n - 1] = 0x80;
                b
            }] {
                let mut data = vec![0];
                data.extend(&bytes);
                decoder
                    .rows(
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
    let mut decoder = Decoder::new(vec![meta.clone()]).unwrap();
    decoder
        .table_map(&table_map(1, &meta, &[3, 15, 3], &[100, 0]))
        .unwrap();
    decoder
        .rows(
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
        decoder.rows(
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
    let mut decoder = Decoder::new(vec![meta.clone()]).unwrap();
    decoder
        .table_map(&table_map(1, &meta, &[246], &[10, 2]))
        .unwrap();
    // DECIMAL(10,2): one 4-byte integral group + one 1-byte fractional group.
    for (bytes, expected) in [
        ([0x80, 0xbc, 0x61, 0x4e, 0x5a], "12345678.90"),
        ([0x7f, 0x43, 0x9e, 0xb1, 0xa5], "-12345678.90"),
    ] {
        let mut data = vec![0];
        data.extend(bytes);
        decoder
            .rows(
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
    let mut decoder = Decoder::new(vec![meta.clone()]).unwrap();
    decoder
        .table_map(&table_map(1, &meta, &vec![3; 300], &[]))
        .unwrap();
    let mut present = vec![0xff; 38];
    present[37] = 0x0f;
    let mut nulls = vec![0xff; 38];
    nulls[37] &= !(1 << 3);
    nulls.extend((-8i32).to_le_bytes());
    decoder
        .rows(
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
fn skip_mixed_types_before_key_without_decoding_values() {
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
        ("decimal", 246, vec![12, 4], vec![0; 6]),
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
    let mut decoder = Decoder::new(vec![meta.clone()]).unwrap();
    decoder
        .table_map(&table_map(1, &meta, &types, &metadata))
        .unwrap();
    let mut data = vec![0; 2];
    data.extend(values);
    decoder
        .rows(
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
    let mut decoder = Decoder::new(vec![basic()]).unwrap();
    decoder
        .table_map(&table_map(1, &basic(), &[3, 15, 3], &[100, 0]))
        .unwrap();
    let mut excluded = basic();
    excluded.schema = "other".into();
    decoder
        .table_map(&table_map(1, &excluded, &[255, 255, 255], &[]))
        .unwrap();
    decoder
        .rows(
            WRITE_ROWS_EVENT,
            &rows(1, WRITE_ROWS_EVENT, 0, &[], &[], &[0xff]),
            |_| panic!("excluded table decoded"),
        )
        .unwrap();
    decoder.clear_table_maps();
    assert!(matches!(
        decoder.rows(
            WRITE_ROWS_EVENT,
            &rows(1, WRITE_ROWS_EVENT, 0, &[], &[], &[]),
            |_| Ok(())
        ),
        Err(Error::MissingTable(1))
    ));
}

#[test]
fn statement_end_clears_maps_and_sink_errors_propagate() {
    let mut decoder = Decoder::new(vec![basic()]).unwrap();
    let map = table_map(1, &basic(), &[3, 15, 3], &[100, 0]);
    decoder.table_map(&map).unwrap();
    let data = rows(1, DELETE_ROWS_EVENT, 3, &[1], &[], &[0, 1, 0, 0, 0]);
    assert!(matches!(
        decoder.rows(DELETE_ROWS_EVENT, &data, |_| Err(Error::MissingPrimaryKey)),
        Err(Error::MissingPrimaryKey)
    ));
    let mut ended = data.clone();
    ended[6] = 1;
    decoder.rows(DELETE_ROWS_EVENT, &ended, |_| Ok(())).unwrap();
    assert!(matches!(
        decoder.rows(DELETE_ROWS_EVENT, &data, |_| Ok(())),
        Err(Error::MissingTable(1))
    ));
}

#[test]
fn missing_null_pk_and_full_images_fail() {
    assert!(Decoder::new(vec![]).is_err());
    let mut composite = basic();
    composite.columns[2].primary_key = true;
    assert!(Decoder::new(vec![composite]).is_err());
    let mut decoder = Decoder::new(vec![basic()]).unwrap();
    decoder
        .table_map(&table_map(1, &basic(), &[3, 15, 3], &[100, 0]))
        .unwrap();
    assert!(matches!(
        decoder.rows(
            DELETE_ROWS_EVENT,
            &rows(1, DELETE_ROWS_EVENT, 3, &[1], &[], &[1]),
            |_| Ok(())
        ),
        Err(Error::MissingPrimaryKey)
    ));
    assert!(
        decoder
            .rows(
                DELETE_ROWS_EVENT,
                &rows(1, DELETE_ROWS_EVENT, 3, &[2], &[], &[0, 0]),
                |_| Ok(())
            )
            .is_err()
    );
    assert!(
        decoder
            .rows(
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
        let mut decoder = Decoder::new(vec![basic()]).unwrap();
        assert!(decoder.table_map(&map[..len]).is_err());
    }
    let mut decoder = Decoder::new(vec![basic()]).unwrap();
    decoder.table_map(&map).unwrap();
    let data = rows(1, DELETE_ROWS_EVENT, 3, &[1], &[], &[0, 1, 0, 0, 0]);
    for len in 0..data.len() {
        assert!(
            decoder
                .rows(DELETE_ROWS_EVENT, &data[..len], |_| Ok(()))
                .is_err()
        );
    }
    assert!(
        decoder
            .table_map(&table_map(1, &basic(), &[8, 15, 3], &[100, 0]))
            .is_err()
    );
    assert!(
        decoder
            .table_map(&table_map(1, &basic(), &[3, 15, 3], &[101, 0]))
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
            let mut decoder = Decoder::new(vec![basic()]).unwrap();
            let _ = decoder.table_map(&data);
            let _ = decoder.rows(UPDATE_ROWS_EVENT, &data, |_| Ok(()));
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
        let mut decoder = Decoder::new(vec![meta.clone()]).unwrap();
        let (wire, metadata) = if ty == "char" {
            (254, [0xee, 0x2c])
        } else {
            (15, [0x2c, 1])
        };
        decoder
            .table_map(&table_map(1, &meta, &[wire], &metadata))
            .unwrap();
        let mut data = vec![0];
        data.extend((bytes.len() as u16).to_le_bytes());
        data.extend(bytes);
        decoder
            .rows(
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
        let mut decoder = Decoder::new(vec![meta.clone()]).unwrap();
        decoder
            .table_map(&table_map(
                1,
                &meta,
                &[246],
                &[(int.len() + frac.len()) as u8, frac.len() as u8],
            ))
            .unwrap();
        decoder
            .rows(
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
            decoder
                .rows(
                    DELETE_ROWS_EVENT,
                    &rows(1, DELETE_ROWS_EVENT, 1, &[1], &[], &invalid),
                    |_| Ok(())
                )
                .is_err()
        );
    }
}
