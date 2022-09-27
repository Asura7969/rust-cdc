use std::collections::HashMap;
use deltalake::DeltaTableError;
use serde_json::Value;



#[derive(Debug, Default)]
pub(crate) struct ValueBuffers {
    // binlog_file_name -> pos
    buffers: HashMap<String, ValueBuffer>,
    len: usize,
}

impl ValueBuffers {
    /// Adds a value to in-memory buffers and tracks the partition and offset.
    pub(crate) fn add(
        &mut self,
        partition: String,
        offset: DataOffset,
        value: Value,
    ) -> Result<(), DeltaTableError> {
        let buffer = self
            .buffers
            .entry(partition)
            .or_insert_with(ValueBuffer::new);

        buffer.add(value, offset);
        self.len += 1;
        Ok(())
    }

    /// Returns the total number of items stored across each partition specific [`ValueBuffer`].
    pub(crate) fn len(&self) -> usize {
        self.len
    }

    /// Returns values, partition offsets and partition counts currently held in buffer and resets buffers to empty.
    pub(crate) fn consume(&mut self) -> ConsumedBuffers {
        let mut partition_offsets = HashMap::new();
        let mut partition_counts = HashMap::new();

        let values = self
            .buffers
            .iter_mut()
            .filter_map(|(partition, buffer)| match buffer.consume() {
                Some((values, offset)) => {
                    partition_offsets.insert(partition.clone(), offset);
                    partition_counts.insert(partition.clone(), values.len());
                    Some(values)
                }
                None => None,
            })
            .flatten()
            .collect();

        self.len = 0;

        ConsumedBuffers {
            values,
            partition_offsets,
            partition_counts,
        }
    }

    /// Clears all value buffers currently held in memory.
    pub(crate) fn reset(&mut self) {
        self.len = 0;
        self.buffers.clear();
    }
}



pub type DataOffset = i64;

#[derive(Debug)]
struct ValueBuffer {
    /// The offset of the last message stored in the buffer.
    last_offset: DataOffset,
    /// The buffer of [`Value`] instances.
    values: Vec<Value>,
}

impl ValueBuffer {
    /// Creates a new [`ValueBuffer`] to store messages from a mysql binlog offset.
    pub(crate) fn new() -> Self {
        Self {
            // The -1 means that it has no stored offset and anything that is firstly passed
            // will be accepted, since message offsets starts with 0.
            // Hence, if the buffer is "consumed", the values list is emptied, but the last_offset
            // should always holds the value to prevent messages duplicates.
            last_offset: -1,
            values: Vec::new(),
        }
    }

    /// Adds the value to buffer and stores its offset as the `last_offset` of the buffer.
    pub(crate) fn add(&mut self, value: Value, offset: DataOffset) {
        self.last_offset = offset;
        self.values.push(value);
    }

    /// Consumes and returns the buffer and last offset so it may be written to delta and clears internal state.
    pub(crate) fn consume(&mut self) -> Option<(Vec<Value>, DataOffset)> {
        if !self.values.is_empty() {
            assert!(self.last_offset > -1);
            Some((std::mem::take(&mut self.values), self.last_offset))
        } else {
            None
        }
    }
}


pub(crate) struct ConsumedBuffers {
    /// The vector of [`Value`] instances consumed.
    pub(crate) values: Vec<Value>,
    /// A [`HashMap`] from partition to last offset represented by the consumed buffers.
    pub(crate) partition_offsets: HashMap<String, DataOffset>,
    /// A [`HashMap`] from partition to number of messages consumed for each partition.
    pub(crate) partition_counts: HashMap<String, usize>,
}
