use std::any::Any;
use bytes::{Buf, Bytes};
use crate::error::Error;
use crate::io::Decode;
use crate::mysql::event::EventData;
use serde::Serialize;

// https://dev.mysql.com/doc/internals/en/xid-event.html
#[derive(Debug, Serialize, PartialEq, Clone)]
pub struct XidEventData(u64);

impl XidEventData {
    pub(crate) fn decode_with(mut buf: Bytes) -> Result<Self, Error> {
        Ok(Self(buf.get_u64_le()))
    }
}

impl EventData for XidEventData {
    fn as_any(&self) -> &dyn Any {
        self
    }
}
