use bytes::BufMut;
use crate::io::{BufMutExt, Encode};
use crate::mysql::protocol::Capabilities;

#[derive(Debug)]
pub(crate) struct ComBinlogDump {
    // status: u8,
    pub binlog_pos: u32,
    // flags: u16,
    pub server_id: u32,
    pub binlog_filename: String,
}

impl Encode<'_, Capabilities> for ComBinlogDump {
    fn encode_with(&self, buf: &mut Vec<u8>, _: Capabilities) {
        buf.put_u8(0x12);
        buf.put_u32_le(self.binlog_pos);
        buf.put_u16_le(0);
        buf.put_u32_le(self.server_id);
        buf.put(self.binlog_filename.as_bytes())
    }
}
