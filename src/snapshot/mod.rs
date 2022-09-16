use std::cell::Cell;
use std::fs::{File, OpenOptions};
use std::io::{BufReader, Write};
use std::path::{PathBuf};
use std::io::prelude::*;
use serde::{Serialize, Deserialize};
use crate::error::Error;
use crate::mysql::SingleTableMap;

pub enum SnapShotType {
    FILE,
    OTHER
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct LogRecord {
    pub file_name: String,
    pub log_pos: u64,
}

impl PartialEq for LogRecord {
    fn eq(&self, other: &Self) -> bool {
        self.file_name.eq(&other.file_name) && self.log_pos == other.log_pos
    }
}


impl From<&[u8]> for LogRecord {

    fn from(mut bytes: &[u8]) -> Self {
        serde_json::from_slice(bytes).expect("parse latest record error!")
    }
}

pub trait LogCommitter {

    /// load history records metadata
    fn open(&mut self) -> Result<(), Error>;

    fn get_latest_record(&mut self) -> Result<Option<LogRecord>, Error>;

    fn persist_table_metadata(&mut self, table_map: SingleTableMap) -> Result<(), Error>;

    /// commit processed log offset, If it is the same Log Record, it will not be persisted
    fn commit(&mut self, record: LogRecord) -> Result<(), Error>;

    /// release resource
    fn close(&mut self) -> Result<(), Error>;
}

const COMMIT_FILE_NAME: &str = "__commit_offset__.json";
const NEW_LINE: &str = "\n";

pub struct FileCommitter {
    path: PathBuf,
    writer: Option<File>,
    newest: Option<LogRecord>,
}

impl FileCommitter {
    pub fn new(path: &str) -> Result<Self, Error> {
        let mut path = PathBuf::from(path);
        if path.is_dir() {
            path.push(COMMIT_FILE_NAME);
            if !path.exists() {
                File::create(path.clone()).ok();
            }
        } else if path.is_file() {
            commit_error("file-committer initialization failed, need path, not file");
        };

        Ok(Self { path, writer: None, newest: None })
    }
}


impl Default for FileCommitter {
    fn default() -> Self {
        let mut path = std::env::current_dir().unwrap();
        path.push(COMMIT_FILE_NAME);
        let _ = File::create(path.clone());
        Self { path, writer: None, newest: None }
    }
}

impl LogCommitter for FileCommitter {
    fn open(&mut self) -> Result<(), Error> {
        match self.writer {
            None => {
                let file = OpenOptions::new()
                    .append(true)
                    .read(true)
                    .open(&self.path)?;
                self.writer = Some(file)
            },
            _ => {}
        }
        Ok(())
    }

    fn get_latest_record(&mut self) -> Result<Option<LogRecord>, Error> {
        let mut f = BufReader::new(File::open(&self.path).unwrap());
        let mut latest_line = String::new();

        for line in f.lines() {
            latest_line = line.unwrap();
        }
        if latest_line.is_empty() {
            return Ok(None)
        }
        let latest = latest_line.as_bytes();
        let record = LogRecord::from(latest);
        self.newest = Some(record.clone());
        Ok(Some(record))
    }

    fn persist_table_metadata(&mut self, table_map: SingleTableMap) -> Result<(), Error> {
        todo!()
    }

    fn commit(&mut self, record: LogRecord) -> Result<(), Error> {
        match &self.newest {
            Some(record) if record.eq(&record) => {
                return Ok(())
            },
            _ => {}
        }
        let mut record = serde_json::to_string(&record).unwrap();
        record.push_str(NEW_LINE);
        let _ = self.writer.as_ref().ok_or(commit_error("file not open!"))?
            .write_all(record.as_bytes())?;
        Ok(())
    }

    fn close(&mut self) -> Result<(), Error> {
        match &mut self.writer {
            Some(file) => {
                let _ = file.flush()?;
            },
            None => {}
        }
        Ok(())
    }
}


fn commit_error(error_msg: &str) -> Error {
    Error::CommitterErr(error_msg.to_string())
}


#[cfg(test)]
mod tests {
    use std::fs;
    use std::io::BufReader;
    use crate::err_parse;
    use super::*;

    #[test]
    fn eq_log_record() {
        let record1 = LogRecord { file_name: String::from("binlog.0000001"), log_pos: 1 as u64 };
        let record2 = LogRecord { file_name: String::from("binlog.0000002"), log_pos: 1 as u64 };
        let record3 = LogRecord { file_name: String::from("binlog.0000002"), log_pos: 2 as u64 };
        let record4 = LogRecord { file_name: String::from("binlog.0000002"), log_pos: 2 as u64 };

        assert_ne!(record1, record2);
        assert_ne!(record2, record3);
        assert_eq!(record3, record4);
    }

    fn del_file(path: &PathBuf) {
        let _ = fs::remove_file(path).unwrap();
    }

    #[test]
    fn default_commit_test() {
        let committer = FileCommitter::default();
        println!("{:?}", committer.path);
        del_file(&committer.path);
    }

    #[test]
    fn default_new_test() {
        let path = "E:\\rustProject\\rust-cdc";
        let committer = FileCommitter::new(path).unwrap();
        println!("{:?}", committer.path);
        del_file(&committer.path);
    }

    #[test]
    fn commit_test() {
        let mut committer = FileCommitter::default();

        let record1 = LogRecord { file_name: "binlog.0000001".to_owned(), log_pos: 111 };
        let record2 = LogRecord { file_name: "binlog.0000002".to_owned(), log_pos: 222 };
        committer.open().unwrap();
        committer.commit(record1).unwrap();
        committer.commit(record2).unwrap();
        committer.close().unwrap();
        del_file(&committer.path);
    }

    #[test]
    fn get_latest_record_test() -> Result<(), Error> {
        let mut committer = FileCommitter::default();
        let record1 = LogRecord { file_name: "binlog.0000001".to_owned(), log_pos: 111 };
        let record2 = LogRecord { file_name: "binlog.0000002".to_owned(), log_pos: 222 };
        committer.open()?;
        committer.commit(record1)?;
        committer.commit(record2)?;
        committer.close()?;

        match committer.get_latest_record()? {
            Some(record) => {
                assert_eq!(record.log_pos, 222 as u64);
                assert_eq!(record.file_name, "binlog.0000002".to_string());
                del_file(&committer.path);
                Ok(())
            },
            _ => Err(err_parse!(""))
        }

    }


}
