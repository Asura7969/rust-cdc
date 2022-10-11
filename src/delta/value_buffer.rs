use std::collections::HashMap;
use std::hash::{Hash, Hasher};
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
        key: UniqueKey,
        value: Value,
    ) -> Result<(), DeltaTableError> {
        let buffer = self
            .buffers
            .entry(partition)
            .or_insert_with(ValueBuffer::new);

        buffer.add(value, key);
        self.len += 1;
        Ok(())
    }

    pub(crate) fn remove(
        &mut self,
        partition: String,
        key: UniqueKey,
    ) -> Result<(), DeltaTableError> {
        match self.buffers.get_mut(&partition) {
            Some(buffer) => {
                buffer.remove(key);
                self.len -= 1;
            },
            _ => {}
        };
        Ok(())
    }

    pub(crate) fn clean(&mut self) {
        self.buffers.clear();
        self.len = 0;
    }

    /// Returns the total number of items stored across each partition specific [`ValueBuffer`].
    pub(crate) fn len(&self) -> usize {
        self.len
    }

    /// Returns values, partition offsets and partition counts currently held in buffer and resets buffers to empty.
    pub(crate) fn consume(&mut self) -> ConsumedBuffers {

        let values = self
            .buffers
            .iter_mut()
            .filter_map(|(partition, buffer)| match buffer.consume() {
                Some(values) => Some(values),
                None => None,
            })
            .flatten()
            .collect();

        self.len = 0;

        ConsumedBuffers {
            values,
        }
    }

    /// Clears all value buffers currently held in memory.
    pub(crate) fn reset(&mut self) {
        self.len = 0;
        self.buffers.clear();
    }
}



pub type DataOffset = i64;

#[derive(Debug, Eq, PartialEq)]
pub enum UniqueKey {
    Number(i64),
    Str(String),
    Empty
}

// impl PartialEq for UniqueKey {
//     fn eq(&self, other: &Self) -> bool {
//         if let &UniqueKey::Number(num) = &self {
//             match other {
//                 UniqueKey::Number(n) => num == n,
//                 _ => false
//             }
//         } else if let &UniqueKey::Str(string) = &self {
//             match other {
//                 UniqueKey::Str(s1) => string.eq(s1),
//                 _ => false
//             }
//         }
//     }
//
//     fn ne(&self, other: &Self) -> bool {
//         !self.eq(other)
//     }
// }

impl Hash for UniqueKey {
    fn hash<H: Hasher>(&self, state: &mut H) {
        match &self {
            &UniqueKey::Number(num) => num.hash(state),
            &UniqueKey::Str(s) => s.hash(state),
            _ => {}
        }
    }

    // fn hash_slice<H: Hasher>(data: &[Self], state: &mut H) where Self: Sized {
    //     todo!()
    // }
}


#[derive(Debug)]
struct ValueBuffer {
    /// The buffer of [`Value`] instances.
    values: HashMap<UniqueKey, Value>,

}

impl ValueBuffer {
    /// Creates a new [`ValueBuffer`] to store messages from a mysql binlog offset.
    pub(crate) fn new() -> Self {
        Self {
            // The -1 means that it has no stored offset and anything that is firstly passed
            // will be accepted, since message offsets starts with 0.
            // Hence, if the buffer is "consumed", the values list is emptied, but the last_offset
            // should always holds the value to prevent messages duplicates.
            values: HashMap::new(),
        }
    }

    /// Adds the value to buffer and stores its offset as the `last_offset` of the buffer.
    pub(crate) fn add(&mut self, value: Value, key: UniqueKey) {
        self.values.insert(key, value);
    }

    pub(crate) fn remove(&mut self, key: UniqueKey) {
        self.values.remove(&key);
    }

    /// Consumes and returns the buffer and last offset so it may be written to delta and clears internal state.
    pub(crate) fn consume(&mut self) -> Option<Vec<Value>> {
        if !self.values.is_empty() {
            let vec = self.values.iter_mut()
                .map(|t| t.1.clone())
                .collect::<Vec<Value>>();
            Some(vec)
        } else {
            None
        }
    }
}


pub(crate) struct ConsumedBuffers {
    /// The vector of [`Value`] instances consumed.
    pub(crate) values: Vec<Value>,
    // /// A [`HashMap`] from partition to last offset represented by the consumed buffers.
    // pub(crate) partition_offsets: HashMap<String, DataOffset>,
    // /// A [`HashMap`] from partition to number of messages consumed for each partition.
    // pub(crate) partition_counts: HashMap<String, usize>,
}
