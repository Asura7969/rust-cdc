use std::ops::Range;

use bytes::{Buf, Bytes};
use crate::error::Error;
use crate::mysql::ColumnDefinition;
use crate::mysql::io::MySqlBufExt;

#[derive(Debug)]
pub(crate) struct Row {
    pub(crate) storage: Bytes,
    pub(crate) values: Vec<Option<Range<usize>>>,
}

impl Row {
    pub(crate) fn get(&self, index: usize) -> Option<&[u8]> {
        self.values[index]
            .as_ref()
            .map(|col| &self.storage[(col.start as usize)..(col.end as usize)])
    }

    pub(crate) fn decode_row<'de>(mut buf: Bytes, columns: &'de Vec<ColumnDefinition>) -> Result<Self, Error> {
        let storage = buf.clone();
        let offset = buf.len();

        let mut values = Vec::with_capacity(columns.len());

        for _ in columns {
            if buf[0] == 0xfb {
                // NULL is sent as 0xfb
                values.push(None);
                buf.advance(1);
            } else {
                let size = buf.get_uint_lenenc() as usize;
                let offset = offset - buf.len();

                values.push(Some(offset..(offset + size)));

                buf.advance(size);
            }
        }

        Ok(Row { values, storage })

    }
}
