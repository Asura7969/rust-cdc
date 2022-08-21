use bytes::{Buf, Bytes};
use crate::error::Error;
use crate::io::Decode;
use crate::mysql::event::EventData;

// https://dev.mysql.com/doc/internals/en/query-event.html
pub(crate) struct QueryEventData {
    slave_proxy_id: u32,
    execution_time: u32,
    schema_length: u32,
    error_code: u16,
    status_vars: u16,
}


impl Decode<'_> for QueryEventData {
    fn decode_with(mut buf: Bytes, _: ()) -> Result<Self, Error> {
        Ok(Self{
            slave_proxy_id: buf.get_u32_le(),
            execution_time: buf.get_u32_le(),
            schema_length: buf.get_u32_le(),
            error_code: buf.get_u16_le(),
            status_vars: buf.get_u16_le(),
        })
    }
}

impl EventData<'_> for QueryEventData {

}
