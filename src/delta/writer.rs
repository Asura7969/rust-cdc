use std::collections::HashMap;
use std::sync::Arc;
use parquet::arrow::ArrowWriter;

use arrow::{
    datatypes::Schema as ArrowSchema,
    datatypes::*,
    error::ArrowError,
    json::reader::Decoder,
    record_batch::*,
};
use arrow::json::reader::DecoderOptions;
use deltalake::action::ColumnCountStat;
use deltalake::storage::DeltaObjectStore;
use futures_util::io::Cursor;
use parquet::file::properties::WriterProperties;
use serde_json::Value;

type NullCounts = HashMap<String, ColumnCountStat>;

#[derive(Debug)]
pub enum DataWriterError {
    #[error("Arrow RecordBatch schema does not match: RecordBatch schema: {record_batch_schema}, {expected_schema}")]
    SchemaMismatch {
        /// The record batch schema.
        record_batch_schema: SchemaRef,
        /// The schema of the target delta table.
        expected_schema: Arc<arrow::datatypes::Schema>,
    },
    /// An Arrow RecordBatch could not be created from the JSON buffer.
    #[error("Arrow RecordBatch created from JSON buffer is a None value")]
    EmptyRecordBatch,
}

pub struct DataWriter {
    storage: DeltaObjectStore,
    arrow_schema_ref: Arc<ArrowSchema>,
    writer_properties: WriterProperties,
    partition_columns: Vec<String>,
    arrow_writers: HashMap<String, DataArrowWriter>,
}

/// Writes messages to an underlying arrow buffer.
pub(crate) struct DataArrowWriter {
    arrow_schema: Arc<ArrowSchema>,
    writer_properties: WriterProperties,
    cursor: Cursor<Vec<u8>>,
    arrow_writer: ArrowWriter<Cursor<Vec<u8>>>,
    partition_values: HashMap<String, Option<String>>,
    null_counts: NullCounts,
    buffered_record_batch_count: usize,
}

impl DataArrowWriter {
    async fn write_values(
        &mut self,
        partition_columns: &[String],
        arrow_schema: Arc<ArrowSchema>,
        json_buffer: Vec<Value>,
    ) -> Result<(), DataWriterError> {
        let record_batch = record_batch_from_json(arrow_schema.clone(), json_buffer.as_slice())?;

        if record_batch.schema() != arrow_schema {
            return Err(DataWriterError::SchemaMismatch {
                record_batch_schema: record_batch.schema(),
                expected_schema: arrow_schema,
            });
        }

        let result = self
            .write_record_batch(partition_columns, record_batch)
            .await;

        if let Err(DataWriterError::Parquet { source }) = result {
            self.write_partial(partition_columns, arrow_schema, json_buffer, source)
                .await
        } else {
            result
        }
    }

    /// Writes the record batch in-memory and updates internal state accordingly.
    /// This method buffers the write stream internally so it can be invoked for many record batches and flushed after the appropriate number of bytes has been written.
    async fn write_record_batch(
        &mut self,
        partition_columns: &[String],
        record_batch: RecordBatch,
    ) -> Result<(), DataWriterError> {
        if self.partition_values.is_empty() {
            let partition_values = extract_partition_values(partition_columns, &record_batch)?;
            self.partition_values = partition_values;
        }

        // Copy current cursor bytes so we can recover from failures
        let current_cursor_bytes = self.cursor.data();

        let result = self.arrow_writer.write(&record_batch);

        match result {
            Ok(_) => {
                self.buffered_record_batch_count += 1;

                apply_null_counts(
                    partition_columns,
                    &record_batch.into(),
                    &mut self.null_counts,
                    0,
                );
                Ok(())
            }
            // If a write fails we need to reset the state of the DeltaArrowWriter
            Err(e) => {
                let new_cursor = Self::cursor_from_bytes(current_cursor_bytes.as_slice())?;
                let _ = std::mem::replace(&mut self.cursor, new_cursor.clone());
                let arrow_writer = Self::new_underlying_writer(
                    new_cursor,
                    self.arrow_schema.clone(),
                    self.writer_properties.clone(),
                )?;
                let _ = std::mem::replace(&mut self.arrow_writer, arrow_writer);
                self.partition_values.clear();

                Err(e.into())
            }
        }
    }
}

/// Creates an Arrow RecordBatch from the passed JSON buffer.
pub fn record_batch_from_json(
    arrow_schema_ref: Arc<ArrowSchema>,
    json_buffer: &[Value],
) -> Result<RecordBatch, DataWriterError> {
    let row_count = json_buffer.len();
    let mut value_iter = json_buffer.iter().map(|j| Ok(j.to_owned()));
    let options = DecoderOptions::new()
        .with_batch_size(row_count);
    let decoder = Decoder::new(arrow_schema_ref, options);
    decoder
        .next_batch(&mut value_iter)?
        .ok_or(DataWriterError::EmptyRecordBatch)
}


