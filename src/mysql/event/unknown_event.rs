use std::any::Any;
use bytes::{Buf, Bytes};
use crate::error::Error;
use crate::io::{BufExt, Decode};
use serde::Serialize;
use crate::mysql::has_buf;
use crate::mysql::{EventData, EventHeaderV4};

#[derive(Debug, Serialize, PartialEq, Clone)]
pub struct UnknownEventData {
    // header: EventHeaderV4,
    checksum: u32,
}

impl EventData for UnknownEventData {
    fn as_any(&self) -> &dyn Any {
        self
    }
}

impl UnknownEventData {
    pub(crate) fn decode_with(mut buf: Bytes, header: &EventHeaderV4) -> Result<(Box<dyn EventData>, Option<Bytes>), Error> {
        let checksum = buf.get_u32_le();
        Ok((Box::new(Self{
            checksum,
        }), has_buf(buf)))
    }
}
