use std::any::Any;
use bytes::{Buf, Bytes};
use uuid::Uuid;
use crate::error::Error;
use crate::io::{BufExt, Decode};
use crate::mysql::event::EventData;
use serde::Serialize;
use crate::mysql::has_buf;

#[derive(Debug, PartialEq, Clone)]
pub struct GtidEventData {
    flags: u8,
    uuid: Uuid,
    coordinate: u64,
    last_committed: Option<u64>,
    sequence_number: Option<u64>,
}


impl GtidEventData {
    pub(crate) fn decode_with(mut buf: Bytes) -> Result<(Box<dyn EventData>, Option<Bytes>), Error> {
        let flags = buf.get_u8();
        let uuid_vec = buf.get_bytes(16).to_vec();
        let uuid_slice = uuid_vec.as_slice();
        let uuid = Uuid::from_slice(uuid_slice)?;
        let offset = buf.get_u64_le();
        let (last_committed, sequence_number) = match buf.get_u8() {
            0x02 => {
                let last_committed = buf.get_u64_le();
                let sequence_number = buf.get_u64_le();
                (Some(last_committed), Some(sequence_number))
            }
            _ => (None, None),
        };
        Ok((Box::new(Self {
            flags,
            uuid,
            coordinate: offset,
            last_committed,
            sequence_number,
        }), has_buf(buf)))
    }
}

impl EventData for GtidEventData {
    fn as_any(&self) -> &dyn Any {
        self
    }
}
