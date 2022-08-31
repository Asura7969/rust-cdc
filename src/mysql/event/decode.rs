use std::any::Any;
use bit_set::BitSet;
use bytes::{Buf, Bytes, BytesMut};
use uuid::Uuid;
use serde::Serialize;
use crate::err_parse;
use crate::error::Error;
use crate::io::{BufExt, Decode};
use crate::mysql::event::{EventType, ColTypes};
use crate::mysql::{
    ColValues,
    EventHeaderV4,
    has_buf,
    MysqlEvent,
    RowType,
    RowsEvent,
    SingleTableMap,
    TableMap,
    replace_note};
use crate::mysql::io::MySqlBufExt;

pub(crate) fn decode_delete_row(mut buf: Bytes,
                                header: EventHeaderV4,
                                event_type: EventType,
                                table_map: Option<&mut TableMap>) -> Result<(MysqlEvent, Option<Bytes>), Error> {
    let (rows, op_buf) = parse_rows_event(event_type, buf, table_map)?;
    Ok((MysqlEvent::DeleteEvent {
        header,
        rows,
    }, op_buf))
}

pub(crate) fn decode_update_row(mut buf: Bytes,
                               header: EventHeaderV4,
                               event_type: EventType,
                               table_map: Option<&mut TableMap>) -> Result<(MysqlEvent, Option<Bytes>), Error> {
    let (rows, op_buf) = parse_rows_event(event_type, buf, table_map)?;
    Ok((MysqlEvent::UpdateEvent {
        header,
        rows,
    }, op_buf))
}

pub(crate) fn decode_write_row(mut buf: Bytes,
                                  header: EventHeaderV4,
                                  event_type: EventType,
                                  table_map: Option<&mut TableMap>) -> Result<(MysqlEvent, Option<Bytes>), Error> {
    let (rows, op_buf) = parse_rows_event(event_type, buf, table_map)?;
    Ok((MysqlEvent::WriteEvent {
        header,
        rows,
    }, op_buf))
}

pub(crate) fn decode_xid(mut buf: Bytes,
                         header: EventHeaderV4) -> Result<(MysqlEvent, Option<Bytes>), Error> {
    Ok((MysqlEvent::XidEvent { header, xid: buf.get_u64_le() }, has_buf(buf)))
}

pub(crate) fn decode_query(mut buf: Bytes,
                           header: EventHeaderV4) -> Result<(MysqlEvent, Option<Bytes>), Error> {
    let thread_id = buf.get_u32_le();
    let exec_time = buf.get_u32_le();
    let schema_length = buf.get_u8() as usize;
    let error_code = buf.get_u16_le();
    // if binlog-version ≥ 4:
    let status_vars = buf.get_u16_le() as usize;
    buf.advance(status_vars);

    let schema = buf.get_str(schema_length)?;
    buf.advance(1);
    let query_len = header.event_size - 19 - 4 - 4 - 1 - 2 - 2 - status_vars as u32 - schema_length as u32 - 1 - 4;
    let query_bytes = buf.get_bytes(query_len as usize).to_vec();
    let input = query_bytes.as_slice();
    let null_end = input
        .iter()
        .position(|&c| c == b'\0')
        .unwrap_or(input.len());
    let query = String::from_utf8_lossy(&input[0..null_end]).to_string();
    Ok((MysqlEvent::QueryEvent {
        header,
        thread_id,
        exec_time,
        error_code,
        schema,
        query: replace_note(query),
    }, has_buf(buf)))
}

pub(crate) fn decode_rotate(mut buf: Bytes,
                            header: EventHeaderV4) -> Result<(MysqlEvent, Option<Bytes>), Error> {
    let position = buf.get_u64_le();
    let str_len = header.event_size - 19 - 8 - 4;
    let next_binlog = buf.get_str(str_len as usize)?;
    let checksum = buf.get_u32_le();
    Ok((MysqlEvent::RotateEvent {
        header,
        position,
        next_binlog,
        checksum,
    }, has_buf(buf)))
}


pub(crate) fn decode_gtid(mut buf: Bytes,
                          header: EventHeaderV4) -> Result<(MysqlEvent, Option<Bytes>), Error> {
    let (flags, uuid, gno, _, _, checksum) = gtid(&mut buf, false)?;
    Ok((MysqlEvent::GtidEvent {
        header,
        flags,
        uuid,
        gno,
        // last_committed,
        // sequence_number,
        checksum
    }, has_buf(buf)))
}

pub(crate) fn decode_anonymous_gtid(mut buf: Bytes,
                                    header: EventHeaderV4) -> Result<(MysqlEvent, Option<Bytes>), Error> {
    let (flags, uuid, gno, last_committed, sequence_number, checksum) = gtid(&mut buf, true)?;
    Ok((MysqlEvent::AnonymousGtidEvent {
        header,
        flags,
        uuid,
        gno,
        last_committed,
        sequence_number,
        checksum
    }, has_buf(buf)))
}

fn gtid(buf: &mut Bytes, flag: bool) -> Result<(u8, Uuid, u64, Option<u64>, Option<u64>, u32), Error>{
    let flags = buf.get_u8();
    let sid = buf.get_bytes(16).to_vec();
    let uuid = Uuid::from_slice(sid.as_slice())?;
    let gno = buf.get_bytes(8).get_uint_lenenc();
    let (last_committed, sequence_number) = if flag {
        match buf.get_u8() {
            0x02 => {
                let last_committed = buf.get_u64_le();
                let sequence_number = buf.get_u64_le();
                (Some(last_committed), Some(sequence_number))
            }
            _ => (None, None),
        }
    } else {
        (None, None)
    };
    let checksum = buf.get_u32_le();
    Ok((flags, uuid, gno, last_committed, sequence_number, checksum))
}

pub(crate) fn decode_unknown(mut buf: Bytes,
                             header: EventHeaderV4) -> Result<(MysqlEvent, Option<Bytes>), Error> {
    let checksum = buf.get_u32_le();
    Ok((MysqlEvent::UnknownEvent {
        header,
        checksum
        }, has_buf(buf)))
}

pub(crate) fn decode_previous_gtids(mut buf: Bytes,
                                    header: EventHeaderV4) -> Result<(MysqlEvent, Option<Bytes>), Error> {
    let num = header.event_size - 19 - 4 - 4;
    let gtid_sets = buf.get_bytes(num as usize).to_vec();
    let buf_size = buf.get_u32_le();
    let checksum = buf.get_u32_le();

    Ok((MysqlEvent::PreviousGtidsEvent {
        header,
        gtid_sets,
        buf_size,
        checksum,
    }, has_buf(buf)))
}

pub(crate) fn decode_table_map(mut buf: Bytes,
                               header: EventHeaderV4) -> Result<(MysqlEvent, Option<Bytes>), Error> {
    let mut table_bytes = buf.get_bytes(6);
    let table_id = table_bytes.get_uint_lenenc();
    buf.advance(2); // flags
    let schema_name = buf.get_str_lenenc()?;
    // nul byte
    buf.advance(1);
    // let table_name_length = buf.get_u8() as usize;
    let table = buf.get_str_lenenc()?;
    // nul byte
    buf.advance(1);
    let column_count  = buf.get_uint_lenenc() as usize;
    let mut columns = Vec::with_capacity(column_count);
    for _ in 0..column_count {
        let column_type = ColTypes::by_code(buf.get_u8());
        columns.push(column_type);
    }
    let _metadata_length = buf.get_uint_lenenc() as usize;
    let final_columns = columns
        .into_iter()
        .map(|c| c.read_metadata(&mut buf))
        .collect::<Result<Vec<_>, _>>()?;
    let num_columns = final_columns.len();
    let null_bitmask_size = (num_columns + 7) >> 3;
    let vec = buf.get_bytes(null_bitmask_size).to_vec();
    let x = vec.as_slice();
    let nullable_bitmap = BitSet::from_bytes(x);

    Ok((MysqlEvent::TableMapEvent {
        header,
        table_id,
        schema_name,
        table,
        columns: final_columns,
        nullable_bitmap
    }, has_buf(buf)))
}

pub(crate) fn decode_format_desc(mut buf: Bytes,
                                 header: EventHeaderV4) -> Result<(MysqlEvent, Option<Bytes>), Error> {
    let binlog_version = buf.get_u16_le();
    if binlog_version != 4 {
        return Err(err_parse!("can only parse a version 4 binary log"));
    }
    let sever_version = buf.get_bytes(50).get_str_until(0x00)?;
    let create_timestamp = buf.get_u32_le();
    let header_len = buf.get_u8();
    let event_types = header.event_size - 19 - (2 + 50 + 4 + 1) - 1 - 4;
    buf.get_bytes(event_types as usize);
    let checksum = if binlog_version != 4 {
        let _checksum_alg = buf.get_u8();
        let code = buf.get_u32_le();
        ChecksumType::from(code as u8)
    } else {
        ChecksumType::from(buf.get_u8())
    };

    Ok((MysqlEvent::FormatDescriptionEvent {
        header,
        binlog_version,
        sever_version,
        create_timestamp,
        header_len,
        checksum
    }, has_buf(buf)))
}

fn parse_rows_event(
    event_type: EventType,
    mut buf: Bytes,
    table_map: Option<&mut TableMap>,
) -> Result<(RowsEvent, Option<Bytes>), Error> {

    let mut table_bytes = buf.get_bytes(6);
    let table_id = table_bytes.get_uint_lenenc();
    buf.advance(2); // flags
    match event_type {
        EventType::WriteRowsEventV2 | EventType::UpdateRowsEventV2 | EventType::DeleteRowsEventV2 => {
            let _extra_data_len = buf.get_u16_le();
            // match extra_data_len {
            //     // https://dev.mysql.com/doc/internals/en/rows-event.html#write-rows-eventv0
            //     2 => unimplemented!(), // nothing
            //     _ => unimplemented!(),
            // }
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
                    rows.push(RowType::NewRow {
                        cols: parse_one_row(
                            &mut buf,
                            this_table_map,
                            &before_column_bitmask,
                        )?,
                    });
                },
                EventType::UpdateRowsEventV1 | EventType::UpdateRowsEventV2 => {
                    rows.push(RowType::UpdatedRow {
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
                    rows.push(RowType::DeletedRow {
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
    Ok((RowsEvent { table_id, rows }, has_buf(buf)))
}

fn parse_one_row(
    buf: &mut Bytes,
    this_table_map: &SingleTableMap,
    present_bitmask: &BitSet,
) -> Result<Vec<ColValues>, Error> {
    let num_set_columns = present_bitmask.len();
    let null_bitmap_len = (num_set_columns + 7) >> 3;
    let vec = buf.get_bytes(null_bitmap_len).to_vec();
    let x = vec.as_slice();
    let null_bitmap = BitSet::from_bytes(x);
    let mut row = Vec::with_capacity(this_table_map.columns.len());
    let mut null_index = 0;
    for (column_idx, column) in this_table_map.columns.iter().enumerate() {

        if !present_bitmask.contains(column_idx) {
            continue;
        }
        let _is_null = null_bitmap.contains(null_index);
        let (_offset, col_val) = column.read_value(buf)?;
        row.push(col_val);
        null_index += 1;
    }
    //println!("finished row: {:?}", row);
    Ok(row)
}

#[derive(Debug, PartialEq, Serialize, Clone)]
pub enum ChecksumType {
    NONE,
    CRC32,
    Other(u8),
}
impl From<u8> for ChecksumType {
    fn from(byte: u8) -> Self {
        match byte {
            0x00 => ChecksumType::NONE,
            0x01 => ChecksumType::CRC32,
            other => ChecksumType::Other(other),
        }
    }
}
