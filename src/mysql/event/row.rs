use std::any::Any;
use crate::mysql::event::{EventData, RowEvent};
use serde::Serialize;

#[derive(Debug, Serialize, PartialEq, Clone)]
pub enum RowType {
    Write,
    Update,
    Delete,
}

#[derive(Debug, Serialize, PartialEq, Clone)]
pub struct RowsData {
    pub(crate) tp: RowType,
    pub(crate) table_id: u64,
    pub(crate) rows: Vec<RowEvent>,
}

impl RowsData {
    pub(crate) fn write(table_id: u64, rows: Vec<RowEvent>) -> Box<dyn EventData> {
        Box::new(RowsData {
            tp: RowType::Write,
            table_id,
            rows,
        })
    }

    pub(crate) fn delete(table_id: u64, rows: Vec<RowEvent>) -> Box<dyn EventData> {
        Box::new(RowsData {
            tp: RowType::Delete,
            table_id,
            rows,
        })
    }

    pub(crate) fn update(table_id: u64, rows: Vec<RowEvent>) -> Box<dyn EventData> {
        Box::new(RowsData {
            tp: RowType::Update,
            table_id,
            rows,
        })
    }
}

impl EventData for RowsData {
    fn as_any(&self) -> &dyn Any {
        self
    }
}
