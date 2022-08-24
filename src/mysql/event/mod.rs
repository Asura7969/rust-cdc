mod xid_event;
mod query_event;
mod format_des_event;
mod table_map_event;
mod gtid_event;


use format_des_event::FormatDescriptionEventData;
use gtid_event::GtidEventData;
use xid_event::XidEventData;
use query_event::QueryEventData;
use table_map_event::TableMapEventData;
use serde::Serialize;
use std::any::Any;
use std::borrow::Borrow;
use std::marker::PhantomData;
use std::sync::Arc;
use bit_set::BitSet;
use bytes::{Buf, Bytes, BytesMut};
use crate::{err_parse, err_protocol};
use crate::error::Error;
use crate::io::{BufExt, Decode};
use crate::mysql::connection::{SingleTableMap, TableMap};
use crate::mysql::io::MySqlBufExt;
use crate::mysql::value::MySQLValue;

pub(crate) struct Event {
    header: EventHeaderV4,
    data: Box<dyn EventData>,
}

impl Event {

    pub(crate) fn decode(mut buf: Bytes, table_map: &mut TableMap) -> Result<Self, Error> {
        /// [header size is 19]
        ///
        /// [header size is 19]: https://dev.mysql.com/doc/internals/en/binlog-event-header.html
        let mut header_bytes = buf.split_off(20);
        let header = EventHeaderV4::decode(header_bytes)?;
        let event_type = header.event_type;
        let data: Box<dyn EventData> = match header.event_type {
            EventType::FormatDescriptionEvent => {
                /// [Replication event checksums]
                ///
                /// [Replication event checksums]: https://dev.mysql.com/worklog/task/?id=2540#tabs-2540-4
                ///
                /// ---
                ///
                /// +-----------+------------+-----------+------------------------+----------+
                /// | Header    | Payload (dataLength)   | Checksum Type (1 byte) | Checksum |
                /// +-----------+------------+-----------+------------------------+----------+
                ///             |                    (eventBodyLength)                       |
                ///             +------------------------------------------------------------+
                Box::new(FormatDescriptionEventData::decode_with(buf)?)
            },
            EventType::TableMapEvent => {
                let data = TableMapEventData::decode_with(buf)?;
                let t_id = data.table_id;
                let schema = data.schema_name.clone();
                let table = data.table.clone();
                let columns = data.columns.to_vec();
                table_map.handle(t_id, schema, table, columns);
                Box::new(data)
            },
            EventType::GtidEvent => {
                Box::new(GtidEventData::decode_with(buf)?)
            },
            EventType::QueryEvent => {
                Box::new(QueryEventData::decode_with(buf)?)
            },
            EventType::XidEvent => {
                Box::new(XidEventData::decode_with(buf)?)
            },
            EventType::WriteRowsEventV1 | EventType::WriteRowsEventV2 => {

                unimplemented!()
            },
            EventType::UpdateRowsEventV1 | EventType::UpdateRowsEventV2 => {
                unimplemented!()
            },
            EventType::DeleteRowsEventV1 | EventType::DeleteRowsEventV2 => {
                unimplemented!()
            },
            _ => {
                unimplemented!()
            },
        };
        Ok(Self{header, data})
    }
}

fn parse_rows_event(
    event_type: EventType,
    data_len: usize,
    mut buf: Bytes,
    table_map: Option<&TableMap>,
) -> Result<RowsEvent, Error> {

    let table_id = buf.get_u64_le();
    buf.advance(2);
    match event_type {
        EventType::WriteRowsEventV2 | EventType::UpdateRowsEventV2 | EventType::DeleteRowsEventV2 => {
            let _ = buf.get_i16_le();
        }
        _ => {}
    }
    let num_columns = buf.get_uint_lenenc() as usize;
    let bitmask_size = (num_columns + 7) >> 3;
    let vec = buf.get_bytes(bitmask_size).to_vec();
    let x = vec.as_slice();
    let before_column_bitmask = BitSet::from_bytes(x);
    let after_column_bitmask = match event_type {
        EventType::UpdateRowsEventV1 | EventType::UpdateRowsEventV2 => {
            let vec = buf.get_bytes(bitmask_size).to_vec();
            let x = vec.as_slice();
            Some(BitSet::from_bytes(x))
        }
        _ => None,
    };
    // let mut _rows = Vec::with_capacity(1);
    if let Some(table_map) = table_map {
        if let Some(this_table_map) = table_map.get(table_id) {
            match event_type {
                EventType::WriteRowsEventV1 | EventType::WriteRowsEventV2 => {
                    unimplemented!()
                },
                EventType::UpdateRowsEventV1 | EventType::UpdateRowsEventV2 => {
                    unimplemented!()
                },
                EventType::DeleteRowsEventV1 | EventType::DeleteRowsEventV2 => {
                    unimplemented!()
                },
                _ => {}
            }
        }
    }


    unimplemented!()
}

fn parse_one_row(
    buf: &mut Bytes,
    this_table_map: &SingleTableMap,
    present_bitmask: &BitSet,
) -> Result<RowData, Error> {
    let num_set_columns = present_bitmask.len();
    let null_bitmask_size = (num_set_columns + 7) >> 3;
    let vec = buf.get_bytes(null_bitmask_size).to_vec();
    let x = vec.as_slice();
    let null_bitmask = BitSet::from_bytes(x);
    let mut row = Vec::with_capacity(this_table_map.columns.len());
    let mut null_index = 0;
    for (i, column_definition) in this_table_map.columns.iter().enumerate() {

        if !present_bitmask.contains(i) {
            row.push(None);
            continue;
        }
        let is_null = null_bitmask.contains(null_index);
        let val = if is_null {
            MySQLValue::Null
        } else {
            //println!("parsing column {} ({:?})", i, column_definition);
            column_definition.read_value(buf)?
        };
        row.push(Some(val));
        null_index += 1;
    }
    //println!("finished row: {:?}", row);
    Ok(row)
}

pub type RowData = Vec<Option<MySQLValue>>;

#[derive(Debug, Serialize)]
#[serde(untagged)]
pub enum RowEvent {
    NewRow {
        cols: RowData,
    },
    DeletedRow {
        cols: RowData,
    },
    UpdatedRow {
        before_cols: RowData,
        after_cols: RowData,
    },
}

struct RowsEvent {
    table_id: u64,
    rows: Vec<RowEvent>,
}

// pub(crate) trait EventHeader<'a>: Decode<'a> {
//     fn get_timestamp(&self) -> u32;
//     fn get_event_type(&self) -> &EventType;
//     fn get_server_id(&self) -> u32;
//     fn get_event_size(&self) -> u32;
//     fn get_log_pos(&self) -> u32;
//     fn get_flags(&self) -> u16;
// }

pub(crate) struct EventHeaderV4 {
    pub(crate) timestamp: u32,
    pub(crate) event_type: EventType,
    pub(crate) server_id: u32,
    pub(crate) event_size: u32,
    pub(crate) log_pos: u32,
    pub(crate) flags: u16,
}

impl EventHeaderV4 {
    pub(crate) fn decode(mut buf: Bytes) -> Result<Self, Error> {
        Ok(Self {
            timestamp: buf.get_u32_le(),
            event_type: EventType::try_from_u8(buf.get_u8())?,
            server_id: buf.get_u32_le(),
            event_size: buf.get_u32_le(),
            log_pos: buf.get_u32_le(),
            flags: buf.get_u16_le(),
        })
    }
}

// impl EventHeader<'_> for EventHeaderV4 {
//     fn get_timestamp(&self) -> u32 {
//         self.timestamp
//     }
//
//     fn get_event_type(&self) -> &EventType {
//         self.event_type.borrow()
//     }
//
//     fn get_server_id(&self) -> u32 {
//         self.server_id
//     }
//
//     fn get_event_size(&self) -> u32 {
//         self.event_size
//     }
//
//     fn get_log_pos(&self) -> u32 {
//         self.log_pos
//     }
//
//     fn get_flags(&self) -> u16 {
//         self.flags
//     }
// }

pub(crate) trait EventData {

}

// https://dev.mysql.com/doc/internals/en/event-classes-and-types.html
// https://dev.mysql.com/doc/internals/en/event-data-for-specific-event-types.html
// https://dev.mysql.com/doc/internals/en/binlog-event-type.html
#[derive(Debug, Copy, Clone, PartialEq)]
#[repr(u8)]
pub(crate) enum EventType {
    UnknownEvent = 0x00,
    StartEventV3 = 0x01,
    QueryEvent = 0x02,
    StopEvent = 0x03,
    RotateEvent = 0x04,
    IntvarEvent = 0x05,
    LoadEvent = 0x06,
    SlaveEvent = 0x07,
    CreateFileEvent = 0x08,
    AppendBlockEvent = 0x09,
    ExecLoadEvent = 0x0a,
    DeleteFileEvent = 0x0b,
    NewLoadEvent = 0x0c,
    RanDEvent = 0x0d,
    UserVarEvent = 0x0e,
    FormatDescriptionEvent = 0x0f,
    XidEvent = 0x10,
    BeginLoadQueryEvent = 0x11,
    ExecuteLoadQueryEvent = 0x12,
    TableMapEvent = 0x13,
    WriteRowsEventV0 = 0x14,
    UpdateRowsEventV0 = 0x15,
    DeleteRowsEventV0 = 0x16,
    WriteRowsEventV1 = 0x17,
    UpdateRowsEventV1 = 0x18,
    DeleteRowsEventV1 = 0x19,
    IncidentEvent = 0x1a,
    HeartbeatEvent = 0x1b,
    IgnorableEvent = 0x1c,
    RowsQueryEvent = 0x1d,
    WriteRowsEventV2 = 0x1e,
    UpdateRowsEventV2 = 0x1f,
    DeleteRowsEventV2 = 0x20,
    GtidEvent = 0x21,
    AnonymousGtidEvent = 0x22,
    PreviousGtidsEvent = 0x23,
}

impl EventType {
    pub(crate) fn try_from_u8(id: u8) -> Result<Self, Error> {
        Ok(match id {
            0x00 => EventType::UnknownEvent,
            0x01 => EventType::StartEventV3,
            0x02 => EventType::QueryEvent,
            0x03 => EventType::StopEvent,
            0x04 => EventType::RotateEvent,
            0x05 => EventType::IntvarEvent,
            0x06 => EventType::LoadEvent,
            0x07 => EventType::SlaveEvent,
            0x08 => EventType::CreateFileEvent,
            0x09 => EventType::AppendBlockEvent,
            0x0a => EventType::ExecLoadEvent,
            0x0b => EventType::DeleteFileEvent,
            0x0c => EventType::NewLoadEvent,
            0x0d => EventType::RanDEvent,
            0x0e => EventType::UserVarEvent,
            0x0f => EventType::FormatDescriptionEvent,
            0x10 => EventType::XidEvent,
            0x11 => EventType::BeginLoadQueryEvent,
            0x12 => EventType::ExecuteLoadQueryEvent,
            0x13 => EventType::TableMapEvent,
            0x14 => EventType::WriteRowsEventV0,
            0x15 => EventType::UpdateRowsEventV0,
            0x16 => EventType::DeleteRowsEventV0,
            0x17 => EventType::WriteRowsEventV1,
            0x18 => EventType::UpdateRowsEventV1,
            0x19 => EventType::DeleteRowsEventV1,
            0x1a => EventType::IncidentEvent,
            0x1b => EventType::HeartbeatEvent,
            0x1c => EventType::IgnorableEvent,
            0x1d => EventType::RowsQueryEvent,
            0x1e => EventType::WriteRowsEventV2,
            0x1f => EventType::UpdateRowsEventV2,
            0x20 => EventType::DeleteRowsEventV2,
            0x21 => EventType::GtidEvent,
            0x22 => EventType::AnonymousGtidEvent,
            0x23 => EventType::PreviousGtidsEvent,

            _ => {
                return Err(err_protocol!("unknown event type 0x{:02x}", id));
            }
        })
    }

    pub(crate) fn is_row_mutation(event_type: EventType) -> bool {
        EventType::is_write(event_type) ||
            EventType::is_update(event_type) ||
            EventType::is_delete(event_type)
    }

    pub(crate) fn is_write(event_type: EventType) -> bool {
        event_type.eq(&EventType::WriteRowsEventV0) ||
            event_type.eq(&EventType::WriteRowsEventV1) ||
            event_type.eq(&EventType::WriteRowsEventV2)
    }

    pub(crate) fn is_update(event_type: EventType) -> bool {
        event_type.eq(&EventType::UpdateRowsEventV0) ||
            event_type.eq(&EventType::UpdateRowsEventV1) ||
            event_type.eq(&EventType::UpdateRowsEventV2)
    }

    pub(crate) fn is_delete(event_type: EventType) -> bool {
        event_type.eq(&EventType::DeleteRowsEventV0) ||
            event_type.eq(&EventType::DeleteRowsEventV1) ||
            event_type.eq(&EventType::DeleteRowsEventV2)
    }
}

#[derive(Debug, Eq, PartialEq, Clone)]
pub enum ColumnType {
    Decimal,
    Tiny,
    Short,
    Long,
    Float(u8),
    Double(u8),
    Null,
    Timestamp,
    LongLong,
    Int24,
    Date,
    Time,
    DateTime,
    Year,
    NewDate,
    Timestamp2(u8),
    DateTime2(u8),
    Time2(u8),
    VarChar(u16),
    Bit(u8, u8),
    NewDecimal(u8, u8),
    Enum(u16),
    Set(u16),
    TinyBlob,
    MediumBlob,
    LongBlob,
    Blob(u8),
    VarString,
    MyString,
    Geometry(u8),
    Json(u8),
    UNKNOWN(u8),
}

impl ColumnType {
    fn by_code(i: u8) -> ColumnType {
        match i {
            0 => ColumnType::Decimal,
            1 => ColumnType::Tiny,
            2 => ColumnType::Short,
            3 => ColumnType::Long,
            4 => ColumnType::Float(0),
            5 => ColumnType::Double(0),
            6 => ColumnType::Null,
            7 => ColumnType::Timestamp,
            8 => ColumnType::LongLong,
            9 => ColumnType::Int24,
            10 => ColumnType::Date,
            11 => ColumnType::Time,
            12 => ColumnType::DateTime,
            13 => ColumnType::Year,
            14 => ColumnType::NewDate, // not implemented (or documented)
            15 => ColumnType::VarChar(0),
            16 => ColumnType::Bit(0, 0), // not implemented
            17 => ColumnType::Timestamp2(0),
            18 => ColumnType::DateTime2(0),
            19 => ColumnType::Time2(0),
            245 => ColumnType::Json(0), // need to implement JsonB
            246 => ColumnType::NewDecimal(0, 0),
            247 => ColumnType::Enum(0),
            248 => ColumnType::Set(0),
            249 => ColumnType::TinyBlob,   // docs say this can't occur
            250 => ColumnType::MediumBlob, // docs say this can't occur
            251 => ColumnType::LongBlob,   // docs say this can't occur
            252 => ColumnType::Blob(0),
            253 => ColumnType::VarString, // not implemented
            254 => ColumnType::MyString,
            255 => ColumnType::Geometry(0), // not implemented
            b => ColumnType::UNKNOWN(b)
        }
    }

    fn read_metadata(self, buf: &mut Bytes)  -> Result<Self, Error> {
        Ok(match self {
            ColumnType::Float(_) => {
                let pack_length = buf.get_u8();
                ColumnType::Float(pack_length)
            }
            ColumnType::Double(_) => {
                let pack_length = buf.get_u8();
                ColumnType::Double(pack_length)
            }
            ColumnType::Blob(_) => {
                let pack_length = buf.get_u8();
                ColumnType::Blob(pack_length)
            }
            ColumnType::Geometry(_) => {
                let pack_length = buf.get_u8();
                ColumnType::Geometry(pack_length)
            }
            ColumnType::VarString | ColumnType::VarChar(_) => {
                let max_length = buf.get_u16_le();
                assert_ne!(max_length, 0);
                ColumnType::VarChar(max_length)
            }
            ColumnType::Bit(..) => unimplemented!(),
            ColumnType::NewDecimal(_, _) => {
                let precision = buf.get_u8();
                let num_decimals = buf.get_u8();
                ColumnType::NewDecimal(precision, num_decimals)
            }
            ColumnType::MyString => {
                // In Table_map_event, column type MYSQL_TYPE_STRING
                // can have the following real_type:
                // * MYSQL_TYPE_STRING (used for CHAR(n) and BINARY(n) SQL types with n <=255)
                // * MYSQL_TYPE_ENUM
                // * MYSQL_TYPE_SET
                let f1 = buf.get_u8();
                let f2 = buf.get_u8();
                let (real_type, max_length) = if f1 == 0 {
                    // not sure which version of mysql emits this,
                    // but log_event.cc checks this case
                    (ColumnType::MyString, f2 as u16)
                } else {
                    // The max length is in 0-1023,
                    // (since CHAR(255) CHARACTER SET utf8mb4 turns into max_length=1020)
                    // and the upper 4 bits of real_type are always set
                    // (in real_type = MYSQL_TYPE_ENUM, MYSQL_TYPE_SET, MYSQL_TYPE_STRING)
                    // So MySQL packs the upper bits of the length
                    // in the 0x30 bits of the type, inverted
                    let real_type = f1 | 0x30;
                    let max_length = (!f1 as u16) << 4 & 0x300 | f2 as u16;
                    (ColumnType::by_code(real_type), max_length)
                };
                match real_type {
                    ColumnType::MyString => ColumnType::VarChar(max_length),
                    ColumnType::Set(_) => ColumnType::Set(max_length),
                    ColumnType::Enum(_) => ColumnType::Enum(max_length),
                    i => unimplemented!("unimplemented stringy type {:?}", i),
                }
            }
            ColumnType::Enum(_) => {
                let pack_length = buf.get_u16_le();
                ColumnType::Enum(pack_length)
            }
            ColumnType::DateTime2(..) => ColumnType::DateTime2(buf.get_u8()),
            ColumnType::Time2(..) => ColumnType::Time2(buf.get_u8()),
            ColumnType::Timestamp2(..) => ColumnType::Timestamp2(buf.get_u8()),
            ColumnType::Json(..) => ColumnType::Json(buf.get_u8()),
            c => c,
        })
    }

    // https://dev.mysql.com/doc/internals/en/binary-protocol-value.html#packet-ProtocolBinary::MYSQL_TYPE_VARCHAR
    pub fn read_value(&self, r: &mut Bytes) -> Result<MySQLValue, Error> {
        match self {
            &ColumnType::Tiny => Ok(MySQLValue::SignedInteger(i64::from(r.get_i8()))),
            &ColumnType::Short => Ok(MySQLValue::SignedInteger(i64::from(
                r.get_i16_le(),
            ))),
            &ColumnType::Long => Ok(MySQLValue::SignedInteger(i64::from(
                r.get_i32_le(),
            ))),
            &ColumnType::Timestamp => Ok(MySQLValue::Timestamp {
                unix_time: r.get_i32_le(),
                subsecond: 0,
            }),
            &ColumnType::LongLong => Ok(MySQLValue::SignedInteger(r.get_i64_le())),
            &ColumnType::Int24 => {
                // let val = i64::from(read_i24_le(r.get_bytes(32)));
                // Ok(MySQLValue::SignedInteger(val))
                unimplemented!()
            }
            &ColumnType::Null => Ok(MySQLValue::Null),
            &ColumnType::VarChar(max_len) => {
                // TODO: don't decode to String,
                // since type=real_type=MYSQL_TYPE_STRING is used for BINARY(n)
                // and type=MYSQL_TYPE_VARCHAR is used for VARBINARY(n)
                // and also the CHAR(n) and VARCHAR(n) encoding is not always utf-8
                let value = r.get_str_lenenc()?;
                Ok(MySQLValue::String(value))
            }
            &ColumnType::Year => Ok(MySQLValue::Year(u32::from(r.get_u8()) + 1900)),
            &ColumnType::Date => {
                // let val = read_i24_le(r.get_bytes(32)) as u32;
                // if val == 0 {
                //     Ok(MySQLValue::Null)
                // } else {
                //     let year = (val & ((1 << 15) - 1) << 9) >> 9;
                //     let month = (val & ((1 << 4) - 1) << 5) >> 5;
                //     let day = val & ((1 << 5) - 1);
                //     if year == 0 || month == 0 || day == 0 {
                //         Ok(MySQLValue::Null)
                //     } else {
                //         Ok(MySQLValue::Date { year, month, day })
                //     }
                // }
                unimplemented!()
            }
            &ColumnType::Time => {
                // let val = read_i24_le(r.get_bytes(32)) as u32;
                // let hours = val / 10000;
                // let minutes = (val % 10000) / 100;
                // let seconds = val % 100;
                // Ok(MySQLValue::Time {
                //     hours,
                //     minutes,
                //     seconds,
                //     subseconds: 0,
                // })
                unimplemented!()
            }
            &ColumnType::DateTime => {
                let value = r.get_u64_le();
                if value == 0 {
                    Ok(MySQLValue::Null)
                } else {
                    let date = value / 1000000;
                    let time = value % 1000000;
                    let year = (date / 10000) as u32;
                    let month = ((date % 10000) / 100) as u32;
                    let day = (date % 100) as u32;
                    let hour = (time / 10000) as u32;
                    let minute = ((time % 10000) / 100) as u32;
                    let second = (time % 100) as u32;
                    if year == 0 || month == 0 || day == 0 {
                        Ok(MySQLValue::Null)
                    } else {
                        Ok(MySQLValue::DateTime {
                            year,
                            month,
                            day,
                            hour,
                            minute,
                            second,
                            subsecond: 0,
                        })
                    }
                }
            }
            // the *2 functions are new in MySQL 5.6
            // docs are at
            // https://dev.mysql.com/doc/internals/en/date-and-time-data-type-representation.html
            &ColumnType::DateTime2(pack_length) => {
                // let mut buf = [0u8; 5];
                // r.read_exact(&mut buf)?;
                // let subsecond = read_datetime_subsecond_part(r, pack_length)?;
                // // one bit unused (sign, but always positive
                // buf[0] &= 0x7f;
                // // 17 bits of yearmonth (all of buf[0] and buf[1] and the top 2 bits of buf[2]
                // let year_month: u32 =
                //     ((buf[2] as u32) >> 6) + ((buf[1] as u32) << 2) + ((buf[0] as u32) << 10);
                // let year = year_month / 13;
                // let month = year_month % 13;
                // // 5 bits day (bits 3-7 of buf[2])
                // let day = ((buf[2] & 0x3e) as u32) >> 1;
                // // 5 bits hour (the last bit of buf[2] and the top 4 bits of buf[3]
                // let hour = (((buf[3] & 0xf0) as u32) >> 4) + (((buf[2] & 0x01) as u32) << 4);
                // // 6 bits minute (the bottom 4 bits of buf[3] and the top 2 bits of buf[4]
                // let minute = (buf[4] >> 6) as u32 + (((buf[3] & 0x0f) as u32) << 2);
                // // 6 bits second (the rest of buf[4])
                // let second = (buf[4] & 0x3f) as u32;
                // Ok(MySQLValue::DateTime {
                //     year,
                //     month,
                //     day,
                //     hour,
                //     minute,
                //     second,
                //     subsecond,
                // })
                unimplemented!()
            }
            &ColumnType::Timestamp2(pack_length) => {
                // let whole_part = r.get_i32();
                // let frac_part = read_datetime_subsecond_part(r, pack_length)?;
                // Ok(MySQLValue::Timestamp {
                //     unix_time: whole_part,
                //     subsecond: frac_part,
                // })
                unimplemented!()
            }
            &ColumnType::Time2(pack_length) => {
                // one bit sign
                // one bit unused
                // 10 bits hour
                // 6 bits minute
                // 6 bits second

                // let mut buf = [0u8; 3];
                // r.read_exact(&mut buf)?;
                // let hours = (((buf[0] & 0x3f) as u32) << 4) | (((buf[1] & 0xf0) as u32) >> 4);
                // let minutes = (((buf[1] & 0x0f) as u32) << 2) | (((buf[2] & 0xb0) as u32) >> 6);
                // let seconds = (buf[2] & 0x3f) as u32;
                // let frac_part = read_datetime_subsecond_part(r, pack_length)?;
                // Ok(MySQLValue::Time {
                //     hours,
                //     minutes,
                //     seconds,
                //     subseconds: frac_part,
                // })
                unimplemented!()
            }
            &ColumnType::Blob(length_bytes) => {
                // let val = read_var_byte_length_prefixed_bytes(r, length_bytes)?;
                // Ok(MySQLValue::Blob(val.into()))
                unimplemented!()
            }
            &ColumnType::Float(length) | &ColumnType::Double(length) => {
                if length == 4 {
                    Ok(MySQLValue::Float(r.get_f32_le()))
                } else if length == 8 {
                    Ok(MySQLValue::Double(r.get_f64_le()))
                } else {
                    unimplemented!("wtf is a {}-byte float?", length)
                }
            }
            &ColumnType::NewDecimal(precision, decimal_places) => {
                // let body = read_new_decimal(r, precision, decimal_places)?;
                // Ok(MySQLValue::Decimal(body))
                unimplemented!()
            }
            &ColumnType::Enum(length_bytes) => {
                let enum_value = match (length_bytes & 0xff) as u8 {
                    0x01 => i16::from(r.get_i8()),
                    0x02 => r.get_i16_le(),
                    i => unimplemented!("unhandled Enum pack_length {:?}", i),
                };
                Ok(MySQLValue::Enum(enum_value))
            }
            &ColumnType::Json(size) => {
                // let body = read_var_byte_length_prefixed_bytes(r, size)?;
                // Ok(MySQLValue::Json(jsonb::parse(body)?))
                unimplemented!()
            }
            &ColumnType::TinyBlob
            | &ColumnType::MediumBlob
            | &ColumnType::LongBlob
            | &ColumnType::VarString
            | &ColumnType::MyString => {
                // the manual promises that these are never present in binlogs and are
                // not implemented by MySQL
                // Err(ColumnParseError::UnimplementedTypeError {
                //     column_type: self.clone(),
                // })
                unimplemented!()
            }
            &ColumnType::Decimal
            | &ColumnType::NewDate
            | &ColumnType::Bit(..)
            | &ColumnType::Set(..)
            | &ColumnType::Geometry(..) => {
                unimplemented!("unhandled value type: {:?}", self);
            }
            _ => unimplemented!("unknown type: {:?}", self)
        }
    }
}
