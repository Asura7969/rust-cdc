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

impl EventData for RowsData {
    fn as_any(&self) -> &dyn Any {
        self
    }
}
