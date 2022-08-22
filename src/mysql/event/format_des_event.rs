use bytes::{Buf, Bytes, BytesMut};
use crate::error::Error;
use crate::io::{BufExt, Decode};
use crate::mysql::event::{EventData, EventType};

pub(crate) struct FormatDescriptionEvent {
    binlog_version: u16,
    sever_version: String,
    header_len: u8,
    data_len: u8,
    checksum: ChecksumType,
}

impl FormatDescriptionEvent {
    pub(crate) fn decode_with(mut buf: Bytes) -> Result<Self, Error> {
        let event_body_len = buf.remaining();
        let binlog_version = buf.get_u16_le();
        let sever_version = buf.get_str(50)?;
        buf.truncate(4);
        let header_len = buf.get_u8();
        // FORMAT_DESCRIPTION = 15
        buf.truncate(15 - 1);
        let data_len = buf.get_u8();
        let checksum_block_len = event_body_len - data_len as usize;
        let checksum = if checksum_block_len > 0 {
            let skip = buf.remaining() - checksum_block_len;
            buf.truncate(skip);
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
