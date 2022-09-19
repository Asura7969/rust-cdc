use std::path::PathBuf;
use crate::error::Error;
use std::fs::{File, OpenOptions};
use std::io::{BufReader, Write};
use std::io::prelude::*;

use crate::mysql::SingleTableMap;
use crate::snapshot::{commit_error, COMMIT_FILE_NAME, LogCommitter, LogEntry};

pub struct FileCommitter {
    path: PathBuf,
    newest: LogEntry,
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

        Ok(Self { path, newest: LogEntry::default() })
    }
}


impl Default for FileCommitter {
    fn default() -> Self {
        let mut path = std::env::current_dir().unwrap();
        path.push(COMMIT_FILE_NAME);
        let _ = File::create(path.clone());
        Self { path, newest: LogEntry::default() }
    }
}

impl LogCommitter for FileCommitter {
    fn open(&mut self) -> Result<(), Error> {
        Ok(())
    }

    fn get_latest_record(&mut self) -> Result<Option<LogEntry>, Error> {
        let mut f = BufReader::new(File::open(&self.path).unwrap());

        for line in f.lines() {
            let x = line.unwrap();
            let log_entry: LogEntry = serde_json::from_str(&x).unwrap();
            self.newest = log_entry.clone();
            return Ok(Some(log_entry));
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
        let mut record = serde_json::to_string(&self.newest).unwrap();

        let mut file = OpenOptions::new()
            .truncate(true)
            .read(true)
            .open(&self.path)?;

        file.write(record.as_bytes())?;

        Ok(())
    }

    fn close(&mut self) -> Result<(), Error> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::io::BufReader;
    use crate::err_parse;
    use super::*;

    #[test]
    fn eq_log_record() {
        let record1 = LogEntry::new("binlog.0000001", 1 as u64);
        let record2 = LogEntry::new("binlog.0000002", 1 as u64);
        let record3 = LogEntry::new("binlog.0000002" ,2 as u64);
        let record4 = LogEntry::new("binlog.0000002", 2 as u64);

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

        let record1 = LogEntry::new("binlog.0000001", 111);
        let record2 = LogEntry::new("binlog.0000002", 222);
        committer.open().unwrap();

        committer.recode_binlog("binlog.0000001".to_string(), 111).unwrap();
        committer.commit().unwrap();
        committer.recode_binlog("binlog.0000002".to_string(), 222).unwrap();
        committer.commit().unwrap();

        committer.close().unwrap();
        del_file(&committer.path);
    }

    #[test]
    fn get_latest_record_test() -> Result<(), Error> {
        let mut committer = FileCommitter::default();
        committer.open()?;

        committer.recode_binlog("binlog.0000001".to_string(), 111)?;
        committer.commit()?;
        committer.recode_binlog("binlog.0000002".to_string(), 222)?;
        committer.commit()?;

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
