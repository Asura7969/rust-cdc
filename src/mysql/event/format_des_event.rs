use std::any::Any;
use bytes::{Buf, Bytes, BytesMut};
use crate::err_parse;
use crate::error::Error;
use crate::io::{BufExt, Decode};
use crate::mysql::event::{EventData, EventType};
use crate::mysql::io::MySqlBufExt;

// https://dev.mysql.com/doc/internals/en/format-description-event.html
pub struct FormatDescriptionEventData {
    pub binlog_version: u16,
    pub sever_version: String,
    pub create_timestamp: u32,
    pub header_len: u8,
    checksum: ChecksumType,
}
impl EventData for FormatDescriptionEventData {
    fn as_any(&self) -> &dyn Any {
        self
    }
}

impl FormatDescriptionEventData {
    pub fn decode_with(mut buf: Bytes) -> Result<Self, Error> {
        let data_len = buf.remaining();
        let binlog_version = buf.get_u16_le();
        if binlog_version != 4 {
            return Err(err_parse!("can only parse a version 4 binary log"));
        }
        let sever_version = buf.get_bytes(50).get_str_until(0x00)?;
        let create_timestamp = buf.get_u32_le();
        let header_len = buf.get_u8();
        let event_types = data_len - 2 - 50 - 4 - 1 - 5;
        buf.advance(event_types);
        let checksum = ChecksumType::from(buf.get_u8());
        Ok(Self{
            binlog_version,
            sever_version,
            create_timestamp,
            header_len,
            checksum
        })
    }
}


pub(crate) enum ChecksumType {
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
