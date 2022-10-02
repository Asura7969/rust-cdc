use std::collections::HashMap;
use std::io::Cursor;
use std::sync::Arc;
use std::convert::TryFrom;
use std::io::Write;

use parquet::arrow::ArrowWriter;

use arrow::{
    datatypes::Schema as ArrowSchema,
    datatypes::*,
    error::ArrowError,
    json::reader::Decoder,
    record_batch::*,
};
use arrow::array::{Array, as_primitive_array, as_struct_array, StructArray};
use arrow::json::reader::DecoderOptions;
use datafusion::datasource::source_as_provider;
use deltalake::action::ColumnCountStat;
use deltalake::DeltaDataTypeLong;
use deltalake::storage::DeltaObjectStore;
use futures_util::AsyncWriteExt;
use log::{info, warn};
use parquet::errors::ParquetError;
use parquet::file::properties::WriterProperties;
use serde_json::Value;
use crate::error::Error;


type NullCounts = HashMap<String, ColumnCountStat>;

#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum DataWriterError {
    #[error("Arrow RecordBatch schema does not match: RecordBatch schema: {record_batch_schema}, {expected_schema}")]
    SchemaMismatch {
        /// The record batch schema.
        record_batch_schema: SchemaRef,
        /// The schema of the target delta table.
        expected_schema: Arc<ArrowSchema>,
    },
    /// An Arrow RecordBatch could not be created from the JSON buffer.
    #[error("Arrow RecordBatch created from JSON buffer is a None value")]
    EmptyRecordBatch,

    /// Indicates that a partial write was performed and error records were discarded.
    #[error("Failed to write some values to parquet. Sample error: {sample_error}.")]
    PartialParquetWrite {
        /// Vec of tuples where the first element of each tuple is the skipped value and the second element is the [`ParquetError`] associated with it.
        skipped_values: Vec<(Value, ParquetError)>,
        /// A sample [`ParquetError`] representing the overall partial write.
        sample_error: ParquetError,
    },

    /// Parquet write failed.
    #[error("Parquet write failed: {source}")]
    Parquet {
        /// The wrapped [`ParquetError`]
        #[from]
        source: ParquetError,
    },

    /// Arrow returned an error.
    #[error("Arrow interaction failed: {source}")]
    Arrow {
        /// The wrapped [`ArrowError`]
        #[from]
        source: ArrowError,
    },

    /// Error returned from std::io
    #[error("std::io::Error: {source}")]
    Io {
        /// The wrapped [`std::io::Error`]
        #[from]
        source: std::io::Error,
    },

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

    async fn write_partial(
        &mut self,
        partition_columns: &[String],
        arrow_schema: Arc<ArrowSchema>,
        json_buffer: Vec<Value>,
        parquet_error: ParquetError,
    ) -> Result<(), DataWriterError> {
        warn!("Failed with parquet error while writing record batch. Attempting quarantine of bad records.");
        let (good, bad) = quarantine_failed_parquet_rows(arrow_schema.clone(), json_buffer)?;
        let record_batch = record_batch_from_json(arrow_schema, good.as_slice())?;
        self.write_record_batch(partition_columns, record_batch)
            .await?;
        info!(
            "Wrote {} good records to record batch and quarantined {} bad records.",
            good.len(),
            bad.len()
        );
        Err(DataWriterError::PartialParquetWrite {
            skipped_values: bad,
            sample_error: parquet_error,
        })
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
        let current_cursor_bytes = self.cursor.clone().into_inner();

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

                Err(DataWriterError::Parquet { source:e })
            }
        }
    }

    fn cursor_from_bytes(bytes: &[u8]) -> Result<Cursor<Vec<u8>>, std::io::Error> {
        let mut cursor = Cursor::new(Vec::new());
        cursor.write_all(bytes)?;
        Ok(cursor)
    }

    fn new_underlying_writer(
            cursor: Cursor<Vec<u8>>,
            arrow_schema: Arc<ArrowSchema>,
            writer_properties: WriterProperties,
    ) -> Result<ArrowWriter<Cursor<Vec<u8>>>, ParquetError> {
        ArrowWriter::try_new(cursor, arrow_schema, Some(writer_properties))
    }
}

fn apply_null_counts(
    partition_columns: &[String],
    array: &StructArray,
    null_counts: &mut HashMap<String, ColumnCountStat>,
    nest_level: i32,
) {
    let fields = match array.data_type() {
        DataType::Struct(fields) => fields,
        _ => unreachable!(),
    };

    array
        .columns()
        .iter()
        .zip(fields)
        .for_each(|(column, field)| {
            let key = field.name().to_owned();

            // Do not include partition columns in statistics
            if nest_level == 0 && partition_columns.contains(&key) {
                return;
            }

            apply_null_counts_for_column(partition_columns, null_counts, nest_level, column, field);
        });
}

fn apply_null_counts_for_column(
    partition_columns: &[String],
    null_counts: &mut HashMap<String, ColumnCountStat>,
    nest_level: i32,
    column: &&Arc<dyn Array>,
    field: &Field,
) {
    let key = field.name().to_owned();

    match column.data_type() {
        // Recursive case
        DataType::Struct(_) => {
            let col_struct = null_counts
                .entry(key)
                .or_insert_with(|| ColumnCountStat::Column(HashMap::new()));

            match col_struct {
                ColumnCountStat::Column(map) => {
                    apply_null_counts(
                        partition_columns,
                        as_struct_array(column),
                        map,
                        nest_level + 1,
                    );
                }
                _ => unreachable!(),
            }
        }
        // Base case
        _ => {
            let col_struct = null_counts
                .entry(key.clone())
                .or_insert_with(|| ColumnCountStat::Value(0));

            match col_struct {
                ColumnCountStat::Value(n) => {
                    let null_count = column.null_count() as DeltaDataTypeLong;
                    let n = null_count + *n;
                    null_counts.insert(key, ColumnCountStat::Value(n));
                }
                _ => unreachable!(),
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

type BadValue = (Value, ParquetError);

fn quarantine_failed_parquet_rows(
    arrow_schema: Arc<ArrowSchema>,
    values: Vec<Value>,
) -> Result<(Vec<Value>, Vec<BadValue>), DataWriterError> {
    let mut good: Vec<Value> = Vec::new();
    let mut bad: Vec<BadValue> = Vec::new();

    for value in values {
        let record_batch = record_batch_from_json(arrow_schema.clone(), &[value.clone()])?;

        let cursor = Cursor::new(Vec::new());;
        let mut writer = ArrowWriter::try_new(cursor.clone(), arrow_schema.clone(), None)?;

        match writer.write(&record_batch) {
            Ok(_) => good.push(value),
            Err(e) => bad.push((value, e)),
        }
    }

    Ok((good, bad))
}

fn extract_partition_values(
    partition_cols: &[String],
    record_batch: &RecordBatch,
) -> Result<HashMap<String, Option<String>>, DataWriterError> {
    let mut partition_values = HashMap::new();

    for col_name in partition_cols.iter() {
        let arrow_schema = record_batch.schema();

        let i = arrow_schema.index_of(col_name)?;
        let col = record_batch.column(i);

        let partition_string = stringified_partition_value(col)?;

        partition_values.insert(col_name.clone(), partition_string);
    }

    Ok(partition_values)
}

// very naive implementation for plucking the partition value from the first element of a column array.
// ideally, we would do some validation to ensure the record batch containing the passed partition column contains only distinct values.
// if we calculate stats _first_, we can avoid the extra iteration by ensuring max and min match for the column.
// however, stats are optional and can be added later with `dataChange` false log entries, and it may be more appropriate to add stats _later_ to speed up the initial write.
// a happy middle-road might be to compute stats for partition columns only on the initial write since we should validate partition values anyway, and compute additional stats later (at checkpoint time perhaps?).
// also this does not currently support nested partition columns and many other data types.
fn stringified_partition_value(arr: &Arc<dyn Array>) -> Result<Option<String>, DataWriterError> {
    let data_type = arr.data_type();

    if arr.is_null(0) {
        return Ok(None);
    }

    let s = match data_type {
        DataType::Int8 => as_primitive_array::<Int8Type>(arr).value(0).to_string(),
        DataType::Int16 => as_primitive_array::<Int16Type>(arr).value(0).to_string(),
        DataType::Int32 => as_primitive_array::<Int32Type>(arr).value(0).to_string(),
        DataType::Int64 => as_primitive_array::<Int64Type>(arr).value(0).to_string(),
        DataType::UInt8 => as_primitive_array::<UInt8Type>(arr).value(0).to_string(),
        DataType::UInt16 => as_primitive_array::<UInt16Type>(arr).value(0).to_string(),
        DataType::UInt32 => as_primitive_array::<UInt32Type>(arr).value(0).to_string(),
        DataType::UInt64 => as_primitive_array::<UInt64Type>(arr).value(0).to_string(),
        DataType::Utf8 => {
            let data = arrow::array::as_string_array(arr);

            data.value(0).to_string()
        }
        // TODO: handle more types
        _ => {
            unimplemented!("Unimplemented data type: {:?}", data_type);
        }
    };

    Ok(Some(s))
}
