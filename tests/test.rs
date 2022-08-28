extern crate core;

use bit_set::BitSet;
use bytes::{Buf, BufMut, Bytes};
use rustcdc::*;
use rustcdc::mysql::{ChecksumType, ColValues, Event, FormatDescriptionEventData, RowEvent, RowsEvent, TableMap, TableMapEventData, XidEventData};
use rustcdc::io::buf::BufExt;
use rustcdc::mysql::ColTypes::{Long, VarChar};
use rustcdc::mysql::RowEvent::NewRow;

#[test]
fn test_format_desc() {
    let input = include_bytes!("events/15_format_desc/binlog.000002").to_vec();
    let mut buf = Bytes::from(input);
    buf.get_bytes(4);
    let mut table_map = TableMap::default();
    let (event, _) = Event::decode(buf, &mut table_map).unwrap();
    match event.data.as_any().downcast_ref::<FormatDescriptionEventData>() {
        Some(fde) => {
            assert_eq!(fde.binlog_version, 4);
            assert_eq!(fde.sever_version, "8.0.21");
            assert_eq!(fde.create_timestamp, 0);
            assert_eq!(fde.checksum, ChecksumType::CRC32)
        },

        _ => panic!("should be format desc"),
    }
}

#[test]
fn test_xid() {
    let input = include_bytes!("events/16_xid/binlog.000002").to_vec();
    let mut buf = Bytes::from(input);
    println!("{}", buf.len());
    buf.get_bytes(1094); // xid Event pos start
    let mut table_map = TableMap::default();
    loop {
        let (event, op_buf) = Event::decode(buf, &mut table_map).unwrap();
        if let Some(xid) = event.data.as_any().downcast_ref::<XidEventData>() {
            assert_eq!(xid.0, 852);
            break
        } else {
            match op_buf {
                Some(mut bytes) => {
                    bytes.get_bytes(4);
                    buf = bytes
                },
                None => panic!("should be format xid"),
            }
        }
    }

}

#[test]
fn test_table_map() {
    // TODO need to test more column types
    let input = include_bytes!("events/19_table_map/mysql-bin.000002").to_vec();
    let mut buf = Bytes::from(input);
    println!("total: {}", buf.remaining());
    buf.get_bytes(687);

    let mut table_map = TableMap::default();
    let vec = vec![0 as u8];
    let set = BitSet::from_bytes(vec.as_slice());

    loop {
        let (event, op_buf) = Event::decode(buf, &mut table_map).unwrap();
        if let Some(tmed) = event.data.as_any().downcast_ref::<TableMapEventData>() {
            assert_eq!(tmed.table_id, 71);
            assert_eq!(tmed.table, "rustcdc");
            assert_eq!(tmed.columns, vec![Long, VarChar(160)]);
            assert_eq!(tmed.nullable_bitmap, set);
            break
        } else {
            match op_buf {
                Some(mut bytes) => {
                    bytes.get_bytes(4);
                    buf = bytes
                },
                None => panic!("should be format table map"),
            }
        }
    }
}

#[test]
fn test_write_rows_v2() {
    let input = include_bytes!("events/30_write_rows_v2/mysql-bin.000002").to_vec();
    let mut buf = Bytes::from(input);
    buf.get_bytes(687);
    let mut table_map = TableMap::default();

    let cols:Vec<ColValues> = vec![ColValues::Long(vec![1, 0, 0, 0]),
                                   ColValues::VarChar(vec![99, 100, 99, 45, 49])];
    let re = NewRow { cols };
    loop {
        let (event, op_buf) = Event::decode(buf, &mut table_map).unwrap();
        if let Some(_) = event.data.as_any().downcast_ref::<TableMapEventData>() {
            assert!(table_map.get(71).is_some());
            break
        } else if let Some(row_event) = event.data.as_any().downcast_ref::<RowsEvent>() {
            assert_eq!(row_event.table_id, 71);
            assert_eq!(row_event.rows, vec![re]);
            break
        } else {
            match op_buf {
                Some(mut bytes) => {
                    bytes.get_bytes(4);
                    buf = bytes
                },
                None => panic!("should write_rows_v2"),
            }
        }
    }
}

#[test]
fn test_update_rows_v2() {
    let input = include_bytes!("events/31_update_rows_v2/mysql-bin.000001").to_vec();
    let mut buf = Bytes::from(input);
    buf.get_bytes(1377);
    let abc = vec![97, 98, 99];
    let xd = vec![120, 100];

    let before = vec![
        ColValues::Long(vec![1, 0, 0, 0]),
        ColValues::VarChar(abc.clone()),
        ColValues::VarChar(abc.clone()),
        ColValues::Blob(abc.clone()),
        ColValues::Blob(abc.clone()),
        ColValues::Blob(abc.clone()),
        ColValues::Float(1.0),
        ColValues::Double(2.0),
        ColValues::NewDecimal(vec![128, 0, 3, 0, 0]),
    ];

    let after = vec![
        ColValues::Long(vec![1, 0, 0, 0]),
        ColValues::VarChar(xd.clone()),
        ColValues::VarChar(xd.clone()),
        ColValues::Blob(xd.clone()),
        ColValues::Blob(xd.clone()),
        ColValues::Blob(xd.clone()),
        ColValues::Float(4.0),
        ColValues::Double(4.0),
        ColValues::NewDecimal(vec![128, 0, 4, 0, 0]),
    ];

    let expect_row = RowEvent::UpdatedRow {
        before_cols: before,
        after_cols: after,
    };

    let mut table_map = TableMap::default();

    loop {
        let (event, op_buf) = Event::decode(buf, &mut table_map).unwrap();
        if let Some(_) = event.data.as_any().downcast_ref::<TableMapEventData>() {
            assert!(table_map.get(72).is_some());
            break
        } else if let Some(row_event) = event.data.as_any().downcast_ref::<RowsEvent>() {
            assert_eq!(row_event.rows, vec![expect_row]);
            break
        } else {
            match op_buf {
                Some(mut bytes) => {
                    bytes.get_bytes(4);
                    buf = bytes
                },
                None => panic!("should be update_row_v2"),
            }
        }
    }

}

#[test]
fn test_delete_rows_v2() {
    let input = include_bytes!("events/32_delete_rows_v2/mysql-bin.000001").to_vec();
    let mut buf = Bytes::from(input);
    buf.get_bytes(901);
    let mut table_map = TableMap::default();


    loop {
        let (event, op_buf) = Event::decode(buf, &mut table_map).unwrap();
        if let Some(_) = event.data.as_any().downcast_ref::<TableMapEventData>() {
            assert!(table_map.get(76).is_some());
            break
        } else if let Some(row_event) = event.data.as_any().downcast_ref::<RowsEvent>() {
            assert_eq!(row_event.table_id, 76);
            // assert_eq!(row_event.column_count, 2);
            assert_eq!(
                row_event.rows,
                vec![RowEvent::DeletedRow {
                    cols: vec![
                        ColValues::Long(vec![1, 0, 0, 0]),
                        ColValues::VarChar(vec![97, 98, 99, 100, 101])
                    ]
                }]
            );
            break
        } else {
            match op_buf {
                Some(mut bytes) => {
                    bytes.get_bytes(4);
                    buf = bytes
                },
                None => panic!("should be delete rows v2"),
            }
        }
    }
}
