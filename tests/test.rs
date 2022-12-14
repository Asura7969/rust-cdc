extern crate core;

use bit_set::BitSet;
use bytes::{Buf, BufMut, Bytes, BytesMut};
use regex::Regex;
use rustcdc::*;
use rustcdc::error::Error;
use rustcdc::mysql::{ChecksumType, ColValues, Event, Listener, MysqlEvent, MySqlOption, MysqlPayload, RowsEvent, RowType, TableMap};
use rustcdc::io::buf::BufExt;
use rustcdc::mysql::ColTypes::{Long, VarChar};
use rustcdc::mysql::RowType::NewRow;

#[test]
fn regex_test() {
    let all_database_regex = Regex::new(r"\\*").unwrap();
    assert!(all_database_regex.is_match("rustcdc.table"));
    assert!(all_database_regex.is_match("rustcdc11.table"));

    let all_table_regex = Regex::new(r"rustcdc\.*").unwrap();
    assert!(all_table_regex.is_match("rustcdc.table"));

    let prefix_table_regex = Regex::new(r"rustcdc\.test*").unwrap();
    assert!(!prefix_table_regex.is_match("rustcdc.table"));
    assert!(prefix_table_regex.is_match("rustcdc.test1"));

    let regex = Regex::new(r"database(.)*\.test(.)*").unwrap();
    assert!(regex.is_match("database.test"));
    assert!(regex.is_match("database1.test2"));
    assert!(!regex.is_match("database1.tt"));
    assert!(!regex.is_match("data.test2"));

    let all_regex = Regex::new("[\\s\\S]*\\.[\\s\\S]*").unwrap();
    assert!(all_regex.is_match("database.test"));
    assert!(all_regex.is_match("data.test2"));
}

#[test]
fn test_format_desc() {
    let input = include_bytes!("events/15_format_desc/binlog.000002").to_vec();
    let mut buf = Bytes::from(input);
    buf.get_bytes(4);
    let mut table_map = TableMap::default();
    let (event, _) = Event::decode(buf, &mut table_map).unwrap();
    match event.body {
        MysqlPayload::FormatDescriptionEvent {
            binlog_version,
            sever_version,
            create_timestamp,
            header_len,
            checksum,
        } => {
            assert_eq!(binlog_version, 4);
            assert_eq!(sever_version, "8.0.21");
            assert_eq!(create_timestamp, 0);
            assert_eq!(checksum, ChecksumType::CRC32)
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
        match event.body {
            MysqlPayload::XidEvent {
                xid
            } => {
                assert_eq!(xid, 852);
                break
            },
            _ => {
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
        match event.body {
            MysqlPayload::TableMapEvent {
                table_id,
                schema_name,
                table,
                columns,
                nullable_bitmap,
            } => {
                assert_eq!(table_id, 71);
                assert_eq!(schema_name, "rustcdc");
                assert_eq!(table, "rustcdc");
                assert_eq!(columns, vec![Long, VarChar(160)]);
                assert_eq!(nullable_bitmap, set);
                break
            },
            _ => {
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
}

#[test]
fn test_write_rows_v2() {
    let input = include_bytes!("events/30_write_rows_v2/mysql-bin.000002").to_vec();
    let mut buf = Bytes::from(input);
    buf.get_bytes(687);
    let mut table_map = TableMap::default();

    let v = String::from_utf8(vec![99, 100, 99, 45, 49]).unwrap();
    let cols:Vec<ColValues> = vec![ColValues::Long(vec![1, 0, 0, 0]),
                                   ColValues::VarChar(v)];
    let re = NewRow { cols };
    loop {
        let (event, op_buf) = Event::decode(buf, &mut table_map).unwrap();
        match event.body {
            MysqlPayload::TableMapEvent {
                ..
            } => {
                assert!(table_map.get(71).is_some());
                buf = op_buf.unwrap();
                buf.get_bytes(4);
            },
            MysqlPayload::WriteEvent {
                rows,
            } => {
                assert_eq!(rows.table_id, 71);
                assert_eq!(rows.rows, vec![re]);
                break
            },
            _ => {
                match op_buf {
                    Some(mut bytes) => {
                        bytes.get_bytes(4);
                        buf = bytes
                    },
                    None => panic!("should write rows v2"),
                }
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

    let v = String::from_utf8(abc.clone()).unwrap();
    let v1 = String::from_utf8(xd.clone()).unwrap();
    let before = vec![
        ColValues::Long(vec![1, 0, 0, 0]),
        ColValues::VarChar(v.clone()),
        ColValues::VarChar(v.clone()),
        ColValues::Blob(abc.clone()),
        ColValues::Blob(abc.clone()),
        ColValues::Blob(abc.clone()),
        ColValues::Float(1.0),
        ColValues::Double(2.0),
        ColValues::NewDecimal(vec![128, 0, 3, 0, 0]),
    ];

    let after = vec![
        ColValues::Long(vec![1, 0, 0, 0]),
        ColValues::VarChar(v1.clone()),
        ColValues::VarChar(v1.clone()),
        ColValues::Blob(xd.clone()),
        ColValues::Blob(xd.clone()),
        ColValues::Blob(xd.clone()),
        ColValues::Float(4.0),
        ColValues::Double(4.0),
        ColValues::NewDecimal(vec![128, 0, 4, 0, 0]),
    ];

    let expect_row = RowType::UpdatedRow {
        before_cols: before,
        after_cols: after,
    };

    let mut table_map = TableMap::default();

    loop {
        let (event, op_buf) = Event::decode(buf, &mut table_map).unwrap();
        match event.body {
            MysqlPayload::TableMapEvent {
                ..
            } => {
                assert!(table_map.get(72).is_some());
                buf = op_buf.unwrap();
                buf.get_bytes(4);
            },
            MysqlPayload::UpdateEvent {
                rows,
            } => {
                assert_eq!(rows.rows, vec![expect_row]);
                break
            },
            _ => {
                match op_buf {
                    Some(mut bytes) => {
                        bytes.get_bytes(4);
                        buf = bytes
                    },
                    None => panic!("should be update row v2"),
                }
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
        match event.body {
            MysqlPayload::TableMapEvent {
                ..
            } => {
                assert!(table_map.get(76).is_some());
                buf = op_buf.unwrap();
                buf.get_bytes(4);
            },
            MysqlPayload::DeleteEvent {
                rows,
            } => {
                assert_eq!(rows.table_id, 76);
                // assert_eq!(row_event.column_count, 2);
                let v = String::from_utf8(vec![97, 98, 99, 100, 101]).unwrap();
                assert_eq!(
                    rows.rows,
                    vec![RowType::DeletedRow {
                        cols: vec![
                            ColValues::Long(vec![1, 0, 0, 0]),
                            ColValues::VarChar(v)
                        ]
                    }]
                );
                break
            },
            _ => {
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
}


#[test]
fn test_query() {
    let input = include_bytes!("events/query/mysql-bin.000001").to_vec();
    let mut buf = Bytes::from(input);
    buf.get_bytes(256);
    let mut table_map = TableMap::default();
    let (event, op_buf) = Event::decode(buf, &mut table_map).unwrap();
    match event.body {
        MysqlPayload::QueryEvent {
            thread_id,
            exec_time,
            error_code,
            schema,
            query,
        } => {
            assert_eq!(thread_id, 8);
            assert_eq!(exec_time, 0);
            assert_eq!(error_code, 0);
            assert_eq!(schema, "rustcdc");
            assert_eq!(query, "CREATE TABLE `rustcdc` (\r\n                             `id` INT UNSIGNED AUTO_INCREMENT,\r\n                             `title` VARCHAR(40) NOT NULL,\r\n                             PRIMARY KEY (`id`)\r\n)ENGINE=InnoDB DEFAULT CHARSET=utf8mb4");
        },
        _ => panic!("should be query"),
    }
}


#[test]
fn test_gtid_prev_gtid() {
    let input = include_bytes!("events/gtid_prev_gtid/mysql-bin.000001").to_vec();
    let mut buf = Bytes::from(input);
    buf.get_bytes(120);
    let mut table_map = TableMap::default();
    let (event, op_buf) = Event::decode(buf, &mut table_map).unwrap();
    match event.body {
        MysqlPayload::PreviousGtidsEvent {
            gtid_sets,
            buf_size,
            checksum,
        } => {
            assert_eq!(gtid_sets, vec![0,0,0,0]);
            assert_eq!(buf_size, 0);
            assert_eq!(checksum, 3421036636);
        },
        _ => panic!("should be gtid prev gtid"),
    }
}

#[test]
fn test_gtid() {
    let input = include_bytes!("events/gtid_prev_gtid/mysql-bin.000001").to_vec();
    let mut buf = Bytes::from(input);
    buf.get_bytes(151);
    let mut table_map = TableMap::default();
    let (event, op_buf) = Event::decode(buf, &mut table_map).unwrap();
    match event.body {
        MysqlPayload::GtidEvent {
            flags,
            uuid,
            gno,
            checksum,
        } => {
            assert_eq!(flags, 1 as u8);
            assert_eq!(&uuid.to_string(), "95b11928-268e-11ed-b39c-04d4c4eb9817");
            assert_eq!(gno, 1 as u64);
        },
        _ => panic!("should be gtid"),
    }
}

#[test]
fn test_rotate() {
    let input = include_bytes!("events/gtid_prev_gtid/mysql-bin.000001").to_vec();
    let mut buf = Bytes::from(input);
    buf.get_bytes(999);
    let mut table_map = TableMap::default();
    let (event, op_buf) = Event::decode(buf, &mut table_map).unwrap();
    match event.body {
        MysqlPayload::RotateEvent {
            position,
            next_binlog,
            checksum,
        } => {
            assert_eq!(position, 4 as u64);
            assert_eq!(next_binlog, "mysql-bin.000002");
            assert_eq!(checksum, 2314217416 as u32);
        },
        _ => panic!("should be gtid"),
    }
}
