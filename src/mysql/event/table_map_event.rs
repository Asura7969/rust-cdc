use std::any::Any;
use bit_set::BitSet;
use byteorder::{ByteOrder, LittleEndian};
use bytes::{Buf, BufMut, Bytes, BytesMut};
use crate::error::Error;
use crate::io::{BufExt, Decode};
use crate::mysql::event::{ColTypes, EventData, EventType};
use crate::mysql::io::MySqlBufExt;
use serde::Serialize;
use crate::mysql::has_buf;

// https://dev.mysql.com/doc/internals/en/table-map-event.html
#[derive(Debug, PartialEq, Clone)]
pub struct TableMapEventData {
    pub table_id: u64,
    pub schema_name: String,
    pub table: String,
    pub columns: Vec<ColTypes>,
    pub nullable_bitmap: BitSet,

}

impl EventData for TableMapEventData {
    fn as_any(&self) -> &dyn Any {
        self
    }
}

impl TableMapEventData {
    pub fn decode_with(mut buf: Bytes) -> Result<(Box<dyn EventData>, Option<Bytes>), Error> {
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

        Ok((Box::new(Self{
            table_id,
            schema_name,
            table,
            columns: final_columns,
            nullable_bitmap
        }), has_buf(buf)))
    }
}

