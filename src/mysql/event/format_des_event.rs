use bytes::{Buf, Bytes, BytesMut};
use crate::err_parse;
use crate::error::Error;
use crate::io::{BufExt, Decode};
use crate::mysql::event::{EventData, EventType};
use crate::mysql::io::MySqlBufExt;

// https://dev.mysql.com/doc/internals/en/format-description-event.html
pub(crate) struct FormatDescriptionEvent {
    binlog_version: u16,
    sever_version: String,
    header_len: u8,
    data_len: u8,
    checksum: ChecksumType,
}
impl EventData for FormatDescriptionEvent {

}

impl FormatDescriptionEvent {
    pub(crate) fn decode_with(mut buf: Bytes) -> Result<Self, Error> {
        let event_body_len = buf.remaining();
        let binlog_version = buf.get_u16_le();
        if binlog_version != 4 {
            return Err(err_parse!("can only parse a version 4 binary log"));
        }
        let sever_version = buf.get_bytes(50).get_str_until(0x00)?;
        let _create_timestamp = buf.get_u32_le();
        let header_len = buf.get_u8();
        // FORMAT_DESCRIPTION = 15
        buf.advance(15 - 1);
        let data_len = buf.get_u8();
        let checksum_block_len = event_body_len - data_len as usize;
        let checksum = if checksum_block_len > 0 {
            let skip = buf.remaining() - checksum_block_len;
            buf.advance(skip);
            ChecksumType::CRC32
        } else {
            ChecksumType::NONE
        };
        Ok(Self{
            binlog_version,
            sever_version,
            header_len,
            data_len,
            checksum
        })
    }
}


pub(crate) enum ChecksumType {
    NONE = 0,
    CRC32 = 4
}
