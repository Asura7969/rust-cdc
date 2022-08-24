use bytes::{Buf, Bytes};
use crate::error::Error;
use crate::io::{BufExt, Decode};
use crate::mysql::event::EventData;

// https://dev.mysql.com/doc/internals/en/query-event.html
pub(crate) struct QueryEventData {
    thread_id: u32,
    exec_time: u32,
    error_code: u16,
    schema: String,
    query: String,
}


impl QueryEventData {
    pub(crate) fn decode_with(mut buf: Bytes) -> Result<Self, Error> {
        let thread_id = buf.get_u32_le();
        let exec_time = buf.get_u32_le();
        let schema_length = buf.get_u8() as usize;
        let error_code = buf.get_u16_le();

        let status_vars = buf.get_u16_le() as usize;
        buf.advance(status_vars);

        let schema = buf.get_bytes(schema_length).get_str_nul()?;
        buf.advance(1);
        let statement = buf.get_str_nul()?;
        Ok(Self{
            thread_id,
            exec_time,
            error_code,
            schema,
            query: statement,
        })
    }
}

impl EventData for QueryEventData {

}
