use bytes::{Buf, Bytes};
use crate::error::Error;
use crate::io::{BufExt, Decode};

#[derive(Debug)]
pub(crate) struct BinlogFilenameAndPosition {
    pub(crate) binlog_filename: String,
    pub(crate) binlog_position: u32,
}
impl Decode<'_> for BinlogFilenameAndPosition {

    fn decode_with(mut buf: Bytes, _: ()) -> Result<Self, Error> {
        println!("buf: {:?}", buf.clone());
        let binlog_position = buf.get_u32_le();
        buf.advance(2);
        let server_id = buf.get_bytes(2).get_str_nul()?;
        println!("server_id: {}", server_id);
        let binlog_filename = buf.get_str_nul()?;
        Ok(Self {binlog_filename, binlog_position})
    }
}
