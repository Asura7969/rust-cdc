use bytes::{Buf, Bytes};
use uuid::Uuid;
use crate::error::Error;
use crate::io::{BufExt, Decode};
use crate::mysql::event::EventData;
use serde::Serialize;

#[derive(Debug, PartialEq, Clone)]
pub(crate) struct GtidEventData {
    flags: u8,
    uuid: Uuid,
    coordinate: u64,
    last_committed: Option<u64>,
    sequence_number: Option<u64>,
}


impl GtidEventData {
    pub(crate) fn decode_with(mut buf: Bytes) -> Result<Self, Error> {
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
        Ok(Self {
            flags,
            uuid,
            coordinate: offset,
            last_committed,
            sequence_number,
        })
    }
}

impl EventData for GtidEventData {

}
