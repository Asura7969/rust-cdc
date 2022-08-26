mod xid_event;
mod query_event;
mod format_des_event;
mod table_map_event;
mod gtid_event;
mod row;


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
use byteorder::{ByteOrder, LittleEndian};
use bytes::{Buf, Bytes, BytesMut};
use crate::{err_parse, err_protocol};
use crate::error::Error;
use crate::io::{BufExt, Decode};
use crate::mysql::connection::{SingleTableMap, TableMap};
use crate::mysql::event::row::{RowsData, RowType};
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
                let ev = parse_rows_event(event_type, buf, Some(table_map))?;
                Box::new(RowsData {
                    tp: RowType::Write,
                    table_id: ev.table_id,
                    rows: ev.rows,
                })
            },
            EventType::UpdateRowsEventV1 | EventType::UpdateRowsEventV2 => {
                let ev = parse_rows_event(event_type, buf, Some(table_map))?;
                Box::new(RowsData {
                    tp: RowType::Update,
                    table_id: ev.table_id,
                    rows: ev.rows,
                })
            },
            EventType::DeleteRowsEventV1 | EventType::DeleteRowsEventV2 => {
                let ev = parse_rows_event(event_type, buf, Some(table_map))?;
                Box::new(RowsData {
                    tp: RowType::Delete,
                    table_id: ev.table_id,
                    rows: ev.rows,
                })
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
    mut buf: Bytes,
    table_map: Option<&mut TableMap>,
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
    let mut rows = Vec::with_capacity(1);
    if let Some(table_map) = table_map {
        if let Some(this_table_map) = table_map.get(table_id) {
            match event_type {
                EventType::WriteRowsEventV1 | EventType::WriteRowsEventV2 => {
                    rows.push(RowEvent::NewRow {
                        cols: parse_one_row(
                            &mut buf,
                            this_table_map,
                            &before_column_bitmask,
                        )?,
                    });
                },
                EventType::UpdateRowsEventV1 | EventType::UpdateRowsEventV2 => {
                    rows.push(RowEvent::UpdatedRow {
                        before_cols: parse_one_row(
                            &mut buf,
                            this_table_map,
                            &before_column_bitmask,
                        )?,
                        after_cols: parse_one_row(
                            &mut buf,
                            this_table_map,
                            after_column_bitmask.as_ref().unwrap(),
                        )?,
                    })
                },
                EventType::DeleteRowsEventV1 | EventType::DeleteRowsEventV2 => {
                    rows.push(RowEvent::DeletedRow {
                        cols: parse_one_row(
                            &mut buf,
                            this_table_map,
                            &before_column_bitmask,
                        )?,
                    });
                },
                _ => unimplemented!(),
            }
        }
    }
    Ok(RowsEvent { table_id, rows })
}

fn parse_one_row(
    buf: &mut Bytes,
    this_table_map: &SingleTableMap,
    present_bitmask: &BitSet,
) -> Result<RowData, Error> {
    let num_set_columns = present_bitmask.len();
    let null_bitmap_len = (num_set_columns + 7) >> 3;
    let vec = buf.get_bytes(null_bitmap_len).to_vec();
    let x = vec.as_slice();
    let null_bitmap = BitSet::from_bytes(x);
    let mut row = Vec::with_capacity(this_table_map.columns.len());
    let mut null_index = 0;
    for (column_idx, column) in this_table_map.columns.iter().enumerate() {

        if !present_bitmask.contains(column_idx) {
            row.push(None);
            continue;
        }
        let _is_null = null_bitmap.contains(null_index);
        let (offset, col_val) = column.read_value(buf)?;
        row.push(Some(col_val));
        null_index += 1;
    }
    //println!("finished row: {:?}", row);
    Ok(row)
}

pub type RowData = Vec<Option<ColValues>>;

#[derive(Debug, Serialize, PartialEq, Clone)]
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

#[derive(Debug, Serialize, PartialEq, Eq, Clone, Copy)]
pub enum ColTypes {
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
    NewDate, // internal used
    VarChar(u16),
    Bit(u8, u8),
    Timestamp2(u8), // this field is suck!!! don't know how to parse
    DateTime2(u8),  // this field is suck!!! don't know how to parse
    Time2(u8),      // this field is suck!!! don't know how to parse
    NewDecimal(u8, u8),
    Enum,       // internal used
    Set,        // internal used
    TinyBlob,   // internal used
    MediumBlob, // internal used
    LongBlob,   // internal used
    Blob(u8),
    VarString(u8, u8),
    String(u8, u8),
    Geometry(u8),
}

impl ColTypes {
    fn by_code(i: u8) -> ColTypes {
        match i {
            0 => ColTypes::Decimal,
            1 => ColTypes::Tiny,
            2 => ColTypes::Short,
            3 => ColTypes::Long,
            4 => ColTypes::Float(4),
            5 => ColTypes::Double(8),
            6 => ColTypes::Null,
            7 => ColTypes::Timestamp,
            8 => ColTypes::LongLong,
            9 => ColTypes::Int24,
            10 => ColTypes::Date,
            11 => ColTypes::Time,
            12 => ColTypes::DateTime,
            13 => ColTypes::Year,
            14 => ColTypes::NewDate,
            15 => ColTypes::VarChar(0),
            16 => ColTypes::Bit(0, 0),
            17 => ColTypes::Timestamp2(0),
            18 => ColTypes::DateTime2(0),
            19 => ColTypes::Time2(0),
            246 => ColTypes::NewDecimal(10, 0),
            247 => ColTypes::Enum,
            248 => ColTypes::Set,
            249 => ColTypes::TinyBlob,
            250 => ColTypes::MediumBlob,
            251 => ColTypes::LongBlob,
            252 => ColTypes::Blob(1),
            253 => ColTypes::VarString(1, 0),
            254 => ColTypes::String(253, 0),
            255 => ColTypes::Geometry(1),
            _ => {
                unreachable!()
            }
        }
    }

    fn read_metadata(self, buf: &mut Bytes)  -> Result<Self, Error> {
        Ok(match self {
            ColTypes::Float(_) => ColTypes::Float(buf.get_u8()),
            ColTypes::Double(_) => ColTypes::Double(buf.get_u8()),
            ColTypes::VarChar(_) => ColTypes::VarChar(buf.get_u16_le()),
            ColTypes::NewDecimal(_, _) => ColTypes::NewDecimal(buf.get_u8(), buf.get_u8()),
            ColTypes::Blob(_) => ColTypes::Blob(buf.get_u8()),
            ColTypes::VarString(_, _) => ColTypes::VarString(buf.get_u8(), buf.get_u8()),
            ColTypes::String(_, _) => ColTypes::String(buf.get_u8(), buf.get_u8()),
            ColTypes::Bit(_, _) => ColTypes::Bit(buf.get_u8(), buf.get_u8()),
            ColTypes::Geometry(_) => ColTypes::Geometry(buf.get_u8()),
            ColTypes::Timestamp2(_) => ColTypes::Timestamp2(buf.get_u8()),
            ColTypes::DateTime2(_) => ColTypes::DateTime2(buf.get_u8()),
            ColTypes::Time2(_) => ColTypes::Timestamp2(buf.get_u8()),
            _ => self.clone(),
        })
    }

    // https://dev.mysql.com/doc/internals/en/binary-protocol-value.html#packet-ProtocolBinary::MYSQL_TYPE_VARCHAR
    pub fn read_value(&self, buf: &mut Bytes) -> Result<(usize, ColValues), Error> {
        Ok(match *self {
            ColTypes::Decimal => {
                (4, ColValues::Decimal(buf.get_bytes(4).to_vec()))
            },
            ColTypes::Tiny => (1, ColValues::Tiny(buf.get_bytes(1).to_vec())),
            ColTypes::Short => {
                (2, ColValues::Short(buf.get_bytes(2).to_vec()))
            }
            ColTypes::Long => (4, ColValues::Long(buf.get_bytes(4).to_vec())),
            ColTypes::Float(_) => {
                let mut f: [u8; 4] = Default::default();
                f.copy_from_slice(buf.get_bytes(4).to_vec().as_slice());
                (4, ColValues::Float(f32::from_le_bytes(f)))
            },
            ColTypes::Double(_) => {
                let mut d: [u8; 8] = Default::default();
                d.copy_from_slice(buf.get_bytes(8).to_vec().as_slice());
                (8, ColValues::Double(f64::from_le_bytes(d)))
            },
            ColTypes::Null => (0, ColValues::Null),
            ColTypes::LongLong => {
                (8, ColValues::LongLong(buf.get_bytes(8).to_vec()))
            },
            ColTypes::Int24 => (4, ColValues::Int24(buf.get_bytes(4).to_vec())),
            ColTypes::Timestamp => {
                let (len, v) = parse_packed(buf)?;
                (len, ColValues::Timestamp(v))
            },
            ColTypes::Date => {
                let (len, v) = parse_packed(buf)?;
                (len, ColValues::Date(v))
            },
            ColTypes::Time => {
                let (len, v) = parse_packed(buf)?;
                (len, ColValues::Time(v))
            },
            ColTypes::DateTime => {
                let (len, v) = parse_packed(buf)?;
                (len, ColValues::DateTime(v))
            },
            ColTypes::Year => (2, ColValues::Year(buf.get_bytes(2).to_vec())),
            ColTypes::NewDate => (0, ColValues::NewDate),
            // ref: https://dev.mysql.com/doc/refman/5.7/en/char.html
            ColTypes::VarChar(max_len) => {
                if max_len > 255 {
                    let len = buf.get_u16_le();
                    let vec = buf.get_bytes(len as usize).to_vec();
                    (len as usize + 2, ColValues::VarChar(vec))
                } else {
                    let len = buf.get_u8();
                    let vec = buf.get_bytes(len as usize).to_vec();
                    (len as usize + 1, ColValues::VarChar(vec))
                }
            },
            ColTypes::Bit(b1, b2) => {
                let len = ((b1 + 7) / 8 + (b2 + 7) / 8) as usize;

                (len, ColValues::Bit(buf.get_bytes(len).to_vec()))
            },
            ColTypes::Timestamp2(_) => {
                (4, ColValues::Timestamp2(buf.get_bytes(4).to_vec()))
            },
            ColTypes::DateTime2(_) => {
                (4, ColValues::DateTime2(buf.get_bytes(4).to_vec()))
            },
            ColTypes::Time2(_) => {
                (4, ColValues::Time2(buf.get_bytes(4).to_vec()))
            },
            ColTypes::NewDecimal(precision, scale) => {
                // copy from https://github.com/mysql/mysql-server/blob/a394a7e17744a70509be5d3f1fd73f8779a31424/libbinlogevents/src/binary_log_funcs.cpp#L204-L214
                let dig2bytes: [u8; 10] = [0, 1, 1, 2, 2, 3, 3, 4, 4, 4];
                let intg = precision - scale;
                let intg0 = intg / 9;
                let frac0 = scale / 9;
                let intg0x = intg - intg0 * 9;
                let frac0x = scale - frac0 * 9;
                let len =
                    intg0 * 4 + dig2bytes[intg0x as usize] + frac0 * 4 + dig2bytes[frac0x as usize];

                (len as usize, ColValues::NewDecimal(buf.get_bytes(len as usize).to_vec()))
            },
            ColTypes::Enum => (0, ColValues::Enum),
            ColTypes::Set => (0, ColValues::Set),
            ColTypes::TinyBlob => (0, ColValues::TinyBlob),
            ColTypes::MediumBlob => (0, ColValues::MediumBlob),
            ColTypes::LongBlob => (0, ColValues::LongBlob),
            ColTypes::Blob(len_bytes) => {
                let len = match len_bytes {
                    1 => buf.get_u8() as usize,
                    2 => buf.get_u16_le() as usize,
                    3 => {
                        let mut bytes = &buf.get_bytes(4)[0..3];
                        byteorder::LittleEndian::read_u32(&mut bytes) as usize
                    }
                    4 => buf.get_u32_le() as usize,
                    8 => buf.get_u64_le() as usize,
                    l => unreachable!("got unexpected length {0:?}", l),
                };

                (len_bytes as usize + len,
                    ColValues::Blob(buf.get_bytes(len).to_vec()))
            }
            ColTypes::VarString(_, _) => {
                // TODO should check string max_len ?
                let len = buf.get_u8();
                (len as usize, ColValues::VarString(buf.get_bytes(len as usize).to_vec()))
            }
            ColTypes::String(_, _) => {
                // TODO should check string max_len ?
                let len = buf.get_u8();
                (len as usize, ColValues::VarChar(buf.get_bytes(len as usize).to_vec()))
            }
            // TODO fix do not use len in def ?
            ColTypes::Geometry(len) => {
                (len as usize, ColValues::Geometry(buf.get_bytes(len as usize).to_vec()))
            },
        })
    }
}

fn parse_packed(buf: &mut Bytes) -> Result<(usize, Vec<u8>), Error> {
    let len = buf.get_u8();
    Ok((len as usize + 1, buf.get_bytes(len as usize).to_vec()))
}

#[derive(Debug, Serialize, PartialEq, Clone)]
pub enum ColValues {
    Decimal(Vec<u8>),
    Tiny(Vec<u8>),
    Short(Vec<u8>),
    Long(Vec<u8>),
    Float(f32),
    Double(f64),
    Null,
    Timestamp(Vec<u8>),
    LongLong(Vec<u8>),
    Int24(Vec<u8>),
    Date(Vec<u8>),
    Time(Vec<u8>),
    DateTime(Vec<u8>),
    Year(Vec<u8>),
    NewDate, // internal used
    VarChar(Vec<u8>),
    Bit(Vec<u8>),
    Timestamp2(Vec<u8>),
    DateTime2(Vec<u8>),
    Time2(Vec<u8>),
    NewDecimal(Vec<u8>),
    Enum,       // internal used
    Set,        // internal used
    TinyBlob,   // internal used
    MediumBlob, // internal used
    LongBlob,   // internal used
    Blob(Vec<u8>),
    VarString(Vec<u8>),
    String(Vec<u8>),
    Geometry(Vec<u8>),
}
