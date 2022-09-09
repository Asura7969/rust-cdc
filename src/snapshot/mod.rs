use std::fs::{File, OpenOptions};
use std::io;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::io::prelude::*;
use serde::Serialize;
use crate::error::Error;


#[derive(Debug, PartialEq, Serialize, Clone)]
pub struct LogRecord<'a> {
    file_name: &'a str,
    log_pos: u32,
}

trait LogCommitter {

    /// load history records metadata
    fn open(&mut self) -> Result<(), Error>;

    fn get_latest_record(&self) -> Option<LogRecord>;

    /// commit processed log offset
    fn commit(&self, record: LogRecord) -> Result<(), Error>;

    /// release resource
    fn close(&mut self) -> Result<(), Error>;
}

const COMMIT_FILE_NAME: String = "__commit_offset__.json".to_string();

struct FileCommitter {
    path: PathBuf,
    writer: Option<File>
}

impl FileCommitter {
    fn new(path: &str) -> Result<Self, Error> {
        let mut path:PathBuf = path.parse()?;
        if path.is_dir() {
            if !path.exists() {
                path.push(COMMIT_FILE_NAME);
                File::create(path.clone())
            }
        } else if path.is_file() {
            // error
        }

        Ok(Self { path, writer: None })
    }
}


impl Default for FileCommitter {
    fn default() -> Self {
        let mut path = std::env::current_dir().unwrap();
        path.push(COMMIT_FILE_NAME);
        let _ = File::create(path.clone());
        Self { path, writer: None }
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

    fn get_latest_record(&self) -> Option<LogRecord> {
        // let deserialized: LogRecord = serde_json::from_str(&serialized).unwrap();
        let result = self.writer.unwrap().read_to_end();
        todo!()
    }

    fn commit(&mut self, mut record: LogRecord) -> Result<(), Error> {
        let mut record = serde_json::to_string(&record).unwrap();
        record.push_str("\n");
        let _ = self.writer.unwrap().write_all(record.as_bytes())?;
        Ok(())
    }

    fn close(&mut self) -> Result<(), Error> {
        match self.writer.unwrap().flush() {
            Err(err) => {

            },
            _ => {},
        }

        Ok(())
    }
}
