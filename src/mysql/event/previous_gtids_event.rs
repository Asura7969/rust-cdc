use std::any::Any;
use bytes::{Buf, Bytes};
use crate::error::Error;
use crate::io::{BufExt, Decode};
use serde::Serialize;
use crate::mysql::has_buf;
use crate::mysql::{EventData, EventHeaderV4};

#[derive(Debug, Serialize, PartialEq, Clone)]
pub struct PreviousGtidsEventData {
    // header: EventHeaderV4,
    // TODO do more specify parse
    gtid_sets: Vec<u8>,
    buf_size: u32,
    checksum: u32,
}

impl EventData for PreviousGtidsEventData {
    fn as_any(&self) -> &dyn Any {
        self
    }
}

impl PreviousGtidsEventData {
    pub(crate) fn decode_with(mut buf: Bytes, header: &EventHeaderV4) -> Result<(Box<dyn EventData>, Option<Bytes>), Error> {
        let num = header.event_size - 19 - 4 - 4;
        let gtid_sets = buf.get_bytes(num as usize).to_vec();
        let buf_size = buf.get_u32_le();
        let checksum = buf.get_u32_le();
        Ok((Box::new(Self{
            gtid_sets,
            buf_size,
            checksum,
        }), has_buf(buf)))
    }
}
