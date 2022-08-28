use std::any::Any;
use bytes::{Buf, Bytes};
use crate::error::Error;
use crate::io::Decode;
use crate::mysql::event::EventData;
use serde::Serialize;
use crate::mysql::has_buf;

// https://dev.mysql.com/doc/internals/en/xid-event.html
#[derive(Debug, Serialize, PartialEq, Clone)]
pub struct XidEventData(pub u64);

impl XidEventData {
    pub(crate) fn decode_with(mut buf: Bytes) -> Result<(Box<dyn EventData>, Option<Bytes>), Error> {
        Ok((Box::new(Self(buf.get_u64_le())), has_buf(buf)))
    }
}

impl EventData for XidEventData {
    fn as_any(&self) -> &dyn Any {
        self
    }
}
