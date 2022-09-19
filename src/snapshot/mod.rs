mod rocksdb_backend;
mod file_backend;


use std::collections::HashMap;
use serde::{Serialize, Deserialize};
use crate::error::Error;
use crate::mysql::SingleTableMap;

pub enum SnapShotType {
    FILE,
    ROCKSDB,
    OTHER
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct LogEntry {
    pub file_name: String,
    pub log_pos: u64,
    pub tables: HashMap<u64, SingleTableMap>,
}

impl PartialEq for LogEntry {
    fn eq(&self, other: &Self) -> bool {
        self.file_name.eq(&other.file_name) && self.log_pos == other.log_pos
    }
}

impl Default for LogEntry {
    fn default() -> Self {
        LogEntry::new("", 0)
    }
}

impl From<&[u8]> for LogEntry {

    fn from(mut bytes: &[u8]) -> Self {
        serde_json::from_slice(bytes).expect("parse latest record error!")
    }
}

impl LogEntry {

    pub(crate) fn new(file_name: &str, log_pos: u64) -> Self {
        Self { file_name: file_name.to_string(), log_pos, tables: HashMap::new() }
    }

    pub(crate) fn add_table(&mut self, table: SingleTableMap) {
        let _ = self.tables.insert(table.table_id, table);
    }

    pub(crate) fn set_binlog_metadata(&mut self, file_name: String, log_pos: u64) {
        self.file_name = file_name;
        self.log_pos = log_pos;
    }
}

pub trait LogCommitter {

    /// load history records metadata
    fn open(&mut self) -> Result<(), Error>;

    fn get_latest_record(&mut self) -> Result<Option<LogEntry>, Error>;

    /// it will not be persisted
    fn recode_table_metadata(&mut self, table_map: SingleTableMap) -> Result<(), Error>;

    /// it will not be persisted
    fn recode_binlog(&mut self, file_name: String, log_pos: u64) -> Result<(), Error>;
    /// commit processed log offset, If it is the same Log Record, it will not be persisted
    fn commit(&mut self) -> Result<(), Error>;

    /// release resource
    fn close(&mut self) -> Result<(), Error>;
}

pub(crate) const COMMIT_FILE_NAME: &str = "__commit_offset__.json";



pub(crate) fn commit_error(error_msg: &str) -> Error {
    Error::CommitterErr(error_msg.to_string())
}


