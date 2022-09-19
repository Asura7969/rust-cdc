use log::warn;
use rocksdb::{DB, DBPath, DBWithThreadMode, Options, SingleThreaded};
use crate::error::Error;
use crate::mysql::SingleTableMap;
use crate::snapshot::{LogCommitter, LogEntry};

const METADATA_KEY: &str = "";

pub struct RocksDBCommitter {
    path: String,
    db: Option<DBWithThreadMode<SingleThreaded>>,
    newest: LogEntry,
}

impl RocksDBCommitter {
    fn new(path: &str) -> Self {
        Self { path: path.to_string(), db: None, newest: LogEntry::default() }
    }
}


impl LogCommitter for RocksDBCommitter {
    fn open(&mut self) -> Result<(), Error> {
        let db = DB::open_default(self.path.as_str())?;
        self.db = Some(db);
        Ok(())
    }

    fn get_latest_record(&mut self) -> Result<Option<LogEntry>, Error> {
        if let Some(db) = &self.db {
            match db.get(METADATA_KEY) {
                Ok(Some(value)) => {
                    let log_entry: LogEntry = serde_json::from_slice(value.as_slice()).unwrap();
                    self.newest = log_entry.clone();
                    return Ok(Some(log_entry));
                },
                Ok(None) => warn!("metadata value not found"),
                Err(err) => return Err(Error::BackendErr(err.to_string()))
            }
        }
        Ok(None)
    }

    fn recode_table_metadata(&mut self, table_map: SingleTableMap) -> Result<(), Error> {
        self.newest.add_table(table_map);
        Ok(())
    }

    fn recode_binlog(&mut self, file_name: String, log_pos: u64) -> Result<(), Error> {
        self.newest.set_binlog_metadata(file_name, log_pos);
        Ok(())
    }

    fn commit(&mut self) -> Result<(), Error> {
        match &self.db {
            Some(db) => {
                let record = serde_json::to_string(&self.newest).unwrap();
                if let Err(err) = db.put(METADATA_KEY, record) {
                    return Err(Error::BackendErr(err.to_string()));
                }
                Ok(())
            },
            None => Err(Error::BackendErr("rocksDB not open.".to_string()))
        }
    }

    fn close(&mut self) -> Result<(), Error> {
        match DB::destroy(&Options::default(), &self.path) {
            Ok(_) => Ok(()),
            Err(err) => Err(Error::BackendErr(err.to_string()))
        }

    }
}
