use bit_set::BitSet;
use bytes::{Buf, BufMut, Bytes, BytesMut};
use crate::error::Error;
use crate::io::{BufExt, Decode};
use crate::mysql::event::{ColTypes, EventData, EventType};
use crate::mysql::io::MySqlBufExt;
use serde::Serialize;

// https://dev.mysql.com/doc/internals/en/table-map-event.html
#[derive(Debug, PartialEq, Clone)]
pub(crate) struct TableMapEventData {
    pub table_id: u64,
    pub schema_name: String,
    pub table: String,
    pub columns: Vec<ColTypes>,
    pub nullable_bitmap: BitSet,

}

impl EventData for TableMapEventData {

}

impl TableMapEventData {
    pub(crate) fn decode_with(mut buf: Bytes) -> Result<Self, Error> {
        let table_id = buf.get_u64_le();
        buf.advance(2);
        let schema_name_length = buf.get_u8() as usize;
        let schema_name = buf.get_str(schema_name_length)?;
        // nul byte
        buf.advance(1);
        let table_name_length = buf.get_u8() as usize;
        let table = buf.get_str(table_name_length)?;
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

        Ok(Self{
            table_id,
            schema_name,
            table,
            columns: final_columns,
            nullable_bitmap
        })
    }
}

