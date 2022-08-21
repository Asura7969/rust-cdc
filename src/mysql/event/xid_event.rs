use bytes::{Buf, Bytes};
use crate::error::Error;
use crate::io::Decode;
use crate::mysql::event::EventData;

// https://dev.mysql.com/doc/internals/en/xid-event.html
pub(crate) struct XidEventData(u64);

impl Decode<'_> for XidEventData {
    fn decode_with(mut buf: Bytes, _: ()) -> Result<Self, Error> {
        Ok(Self(buf.get_u64_le()))
    }
}

impl EventData<'_> for XidEventData { }
