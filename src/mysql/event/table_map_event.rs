use bit_set::BitSet;
use bytes::{Buf, BufMut, Bytes, BytesMut};
use crate::error::Error;
use crate::io::{BufExt, Decode};
use crate::mysql::event::{ColumnType, EventData, EventType};

// https://dev.mysql.com/doc/internals/en/table-map-event.html
pub(crate) struct TableMapEvent {
    table_id: u64,
    database: String,
    table: String,
    column_types: Bytes,
    column_metadata: Vec<i32>,
    column_nullability: BitSet,

}

impl EventData for TableMapEvent {

}

impl TableMapEvent {
    pub(crate) fn decode_with(mut buf: Bytes) -> Result<Self, Error> {
        let table_id = buf.get_u64_le();
        buf.truncate(3);
        let database = buf.get_str_until(0x00)?;
        buf.truncate(1);
        let table = buf.get_str_until(0x00)?;
        let number_of_columns = buf.get_packet_num()?;
        let column_types = buf.get_bytes(number_of_columns as usize);
        buf.get_packet_num()?; // metadata length
        let column_metadata = read_metadata(&mut buf, &column_types)?;
        let column_nullability= buf.read_bitset(number_of_columns as usize, true)?;
        let metadata_len = buf.remaining();
        if metadata_len > 0 {
            // todo
        }

        Ok(Self{
            table_id,
            database,
            table,
            column_types,
            column_metadata,
            column_nullability
        })
    }
}


fn read_metadata(bytes: &mut Bytes, column_types: &Bytes) -> Result<Vec<i32>, Error> {
    let column_type_vec = column_types.to_vec();
    let len = column_type_vec.len();
    let mut vec = Vec::with_capacity(len);

    for i in column_type_vec {
        match ColumnType::by_code((i & 0xFF)) {
            ColumnType::DECIMAL | ColumnType::DOUBLE | ColumnType::BLOB | ColumnType::JSON | ColumnType::GEOMETRY => {
                vec.push(bytes.get_u8() as i32);
            },
            ColumnType::BIT | ColumnType::VARCHAR | ColumnType::NEWDECIMAL => {
                vec.push(bytes.get_u16_le() as i32);
            },
            ColumnType::SET | ColumnType::ENUM | ColumnType::STRING => {
                vec.push(big_endian_int(bytes.get_bytes(2)) as i32);
            },
            ColumnType::TimeV2 | ColumnType::DatetimeV2 | ColumnType::TimestampV2 => {
                vec.push(bytes.get_u8() as i32);
            },
            _ => {
                vec.push(0);
            },
        }
    }
    Ok(vec)
}


fn big_endian_int(bytes: Bytes) -> u8 {
    let mut accum: u8 = 0;
    for b in bytes {
        accum = (accum << 8) | if b >= 0 {
            b
        } else {
            b + u8::MAX + 1
        }
    }
    accum
}
