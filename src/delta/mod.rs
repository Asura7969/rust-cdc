use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;
use arrow::array::{StringArray, UInt16Array};
use arrow::array::Int32Array;
use arrow::compute::math_op;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::json;
use deltalake::{action, checkpoints, DeltaDataTypeVersion, DeltaTable, DeltaTableBuilder, DeltaTableConfig, DeltaTableError, DeltaTableMetaData, SchemaDataType, SchemaField};
use arrow::record_batch::RecordBatch;
use datafusion::datasource::file_format::parquet::{DEFAULT_PARQUET_EXTENSION, ParquetFormat};
use datafusion::datasource::listing::ListingOptions;
use datafusion::prelude::{ParquetReadOptions, SessionContext};
use datafusion::sql::parser::DFParser;
use deltalake::action::{Action, Add, Remove};
use deltalake::checkpoints::CheckpointError;
use deltalake::storage::DeltaObjectStore;
use deltalake::writer::{DeltaWriter, RecordBatchWriter};
use parquet2::FallibleStreamingIterator;
use parquet::file::serialized_reader::SerializedFileReader;
use serde_json::{Map, Value};
use parquet::file::reader::FileReader;
use parquet::schema::types::Type;
use sqlparser::ast::{AlterTableOperation, ColumnDef, Ident, ObjectName};
use sqlparser::ast::AlterTableOperation::{AddColumn, DropColumn, RenameColumn, RenameTable};
use sqlparser::ast::Statement::{AlterTable, Drop};
use serde::{Deserialize, Serialize};
use stable_bloom_filter::stable::StableBloomFilter;

mod helper;
mod value_buffer;
mod writer;

pub use writer::DataWriterError;
use crate::delta::value_buffer::{DataOffset, UniqueKey, ValueBuffers};
use crate::delta::writer::DataWriter;
use crate::error::Error;

/// [delta schema]
///
/// [delta schema]: https://github.com/delta-io/delta/blob/master/PROTOCOL.md#Schema-Serialization-Format

pub enum Record {
    /// binlog_name, offset, body
    Mysql(String, u64, OpBody),
    /// partition, offset, body
    Kafka(i32, i64, OpBody),
}

pub enum OpBody {
    Add(Row),
    // offset, before, after
    Update(Row, Row),
    Delete(Row),
    Query(String),
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct Row {
    value: Map<String, Value>
}

struct IngestProcessor {
    table: DeltaTable,
    delta_writer: DataWriter,
    value_buffers: ValueBuffers,
    latency_timer: Instant,
    filter: StableBloomFilter,
    opts: IngestOptions,
}

const EMPTY_STR:&str = "";

impl IngestProcessor {
    async fn new(
        table_uri: &str,
        opts: IngestOptions,
    ) -> Result<IngestProcessor, Error> {
        let filter = StableBloomFilter::new_default(10_000, 0.01);
        let table = DeltaTableBuilder::from_uri(table_uri).with_storage_options(HashMap::new()).build()?;
        let delta_writer = DataWriter::for_table(&table, HashMap::new())?;

        Ok(IngestProcessor {
            table,
            delta_writer,
            value_buffers: ValueBuffers::default(),
            latency_timer: Instant::now(),
            filter,
            opts,
        })
    }

    fn extract_partition_value(&mut self, row: &Row) -> String {
        match &self.opts.partition_field {
            Some(field) => {
                if let Some(v) = row.value.get(field.as_str()) {
                    v.to_string()
                } else {
                    EMPTY_STR.to_string()
                }
            },
            _ => EMPTY_STR.to_string()
        }
    }

    fn extract_unique_key_value(&mut self, row: &Row) -> UniqueKey {
        match &self.opts.unique_key_fields {
            Some(fields) => {
                let v = fields.iter().map(|field| {
                    if let Some(v) = row.value.get(field.as_str()) {
                        v.to_string()
                    } else {
                        EMPTY_STR.to_string()
                    }
                }).collect::<Vec<_>>().join("");
                UniqueKey::Str(v)
            },
            _ => UniqueKey::Str(EMPTY_STR.to_string())
        }
    }

    async fn process_record(&mut self, record: Record) {
        match record {
            Record::Mysql(file_name, pos, body) => {
                match body {
                    OpBody::Add(row) => {
                        // serde_json::to_value(row).map_err(|e| todo!())?
                        self.add(row);
                    },
                    // offset, before, after
                    OpBody::Update(before, after) => {
                        // before
                        self.remove(before);
                        // after
                        self.add(after);
                    },
                    OpBody::Delete(row) => {
                        self.remove(row);
                    },
                    OpBody::Query(query) => {

                    },
                }
            },
            Record::Kafka(partition_id, offset, body) => {
                // to_kafka_value(body)
                todo!()
            },
            _ => {
                unimplemented!()
            }
        };
        // self.delta_writer.write(vec![values]);
    }

    /// mysql binlog
    ///     insert:
    ///         partition = partition field value
    ///         key       = UniqueKey(field value or Combined field values)
    ///     update = remove old value + add new value
    /// kafka
    ///     partition = kafka partition
    ///     key       = kafka partition offset
    async fn add(&mut self,
                 row: Row) -> Result<(), DeltaTableError> {
        let partition = self.extract_partition_value(&row);
        let unique_key = self.extract_unique_key_value(&row);
        let value = Value::Object(row.value);

        self.value_buffers.add(partition, unique_key, value)
    }

    async fn remove(&mut self,
                    row: Row) -> Result<(), DeltaTableError> {
        let partition = self.extract_partition_value(&row);
        let unique_key = self.extract_unique_key_value(&row);
        self.value_buffers.remove(partition, unique_key)
    }



}

fn to_kafka_value(record: OpBody) -> Vec<Value> {
    todo!()
}


pub struct IngestOptions {
    /// Unique per topic per environment. **Must** be the same for all processes that are part of a single job.
    /// It's used as a prefix for the `txn` actions to track messages offsets between partition/writers.
    pub app_id: String,
    /// Max desired latency from when a message is received to when it is written and
    /// committed to the target delta table (in seconds)
    pub allowed_latency: u64,
    /// Number of messages to buffer before writing a record batch.
    pub max_messages_per_batch: usize,
    /// Desired minimum number of compressed parquet bytes to buffer in memory
    /// before writing to storage and committing a transaction.
    pub min_bytes_per_file: usize,
    /// An optional dead letter table to write messages that fail deserialization, transformation or schema validation.
    pub dlq_table_uri: Option<String>,
    /// If `true` then application will write checkpoints on each 10th commit.
    pub write_checkpoints: bool,
    /// Additional properties to initialize the Kafka consumer with.
    pub additional_kafka_settings: Option<HashMap<String, String>>,
    pub partition_field: Option<String>,
    pub unique_key_fields: Option<Vec<String>>,
    /// A statsd endpoint to send statistics to.
    pub statsd_endpoint: String,
}

impl Default for IngestOptions {
    fn default() -> Self {
        IngestOptions {
            app_id: "cdc_delta_ingest".to_string(),
            allowed_latency: 300,
            max_messages_per_batch: 5000,
            min_bytes_per_file: 134217728,
            dlq_table_uri: None,
            additional_kafka_settings: None,
            partition_field: None,
            unique_key_fields: None,
            write_checkpoints: false,
            statsd_endpoint: "localhost:8125".to_string(),
        }
    }
}


#[cfg(test)]
mod tests {

    use super::*;

    use sqlparser::ast::{Ident, ObjectType, Statement};
    use sqlparser::ast::Statement::{AlterTable, Truncate};
    // use datafusion::execution::context::ExecutionContext;

    const TABLE_PATH:&str = "file:///E:/rustProject/rust-cdc/delta_table_test";

    pub async fn create_table_from_schema() -> DeltaTable {
        let schema = deltalake::Schema::new(vec![
                SchemaField::new(
            "Id".to_string(),
            SchemaDataType::primitive("integer".to_string()),
            true,
            HashMap::new()),
                SchemaField::new(
                    "name".to_string(),
                    SchemaDataType::primitive("string".to_string()),
                    true,
                    HashMap::new(),
                )
        ]);
        create_delta_table(TABLE_PATH,
                           Some("delta-rs_test_table".to_owned()),
                           Some("Table created by delta-rs tests".to_owned()),
                           schema,
                           vec![format!("Id")],
                           HashMap::new()).await.unwrap()
    }

    pub(crate) async fn try_create_checkpoint(
        table: &mut DeltaTable,
        version: DeltaDataTypeVersion,
    ) -> Result<(), CheckpointError> {
        if version % 10 == 0 {
            let table_version = table.version();
            // if there's new version right after current commit, then we need to reset
            // the table right back to version to create the checkpoint
            let version_updated = table_version != version;
            if version_updated {
                table.load_version(version).await?;
            }

            checkpoints::create_checkpoint(table).await?;
            log::info!("Created checkpoint version {}.", version);

            let removed = checkpoints::cleanup_metadata(table).await?;
            if removed > 0 {
                log::info!("Metadata cleanup, removed {} obsolete logs.", removed);
            }

            if version_updated {
                table.update().await?;
            }
        }
        Ok(())
    }

    fn build_record_batch(schema: &Schema,
                          ids: Vec<u16>,
                          names:Vec<String>) -> RecordBatch {
        RecordBatch::try_new(Arc::new(schema.clone()),
                             vec![Arc::new(UInt16Array::from(ids)), Arc::new(StringArray::from(names))]
        ).unwrap()
    }
    #[tokio::test]
    async fn write_to_delta() {
        let mut table = create_table_from_schema().await;

        let schema = Schema::new(vec![
                Field::new("Id", DataType::UInt16, true),
                Field::new("name", DataType::Utf8, true)
            ]);
        let records = vec![
            (1, "a"), (2, "b"), (3, "c"), (4, "d"), (5, "e"),
            (6, "a"), (7, "b"), (8, "c"), (9, "c"), (10, "c")
        ];

        let mut writer = RecordBatchWriter::for_table(&table).unwrap();

        for (id, name) in records {
            let batch = build_record_batch(&schema, vec![id], vec![name.to_string()]);
            if let Err(error) = writer.write(batch).await {
                panic!("{:?}", error)
            }
            let mut add_actions = writer.flush().await.unwrap();

            let mut tx1 = table.create_transaction(None);

            let actions = add_actions.drain(..).map(Action::add).collect();

            tx1.add_actions(actions);
            let commit = tx1.prepare_commit(None, None).await.unwrap();

            table.update().await.unwrap();

            let version = table.version() + 1;
            let commit_result = table.try_commit_transaction(&commit, version).await;

            match commit_result {
                Ok(v) =>{
                    if v != version {
                        panic!("version 不匹配")
                    }
                    assert_eq!(v, version);
                    try_create_checkpoint(&mut table, version).await.unwrap();

                },
                Err(error) => {
                    panic!("{:?}", error)
                }
            }
        }

    }

    #[tokio::test]
    async fn create_table() {

        // let mut table_path = PathBuf::from(table.table_uri());
        // let add = &add_actions[0];
        // let path = table_path.join(&add.path);
        //
        // let file = File::open(path.as_path()).unwrap();
        // let reader = SerializedFileReader::new(file).unwrap();
        //
        // let metadata = reader.metadata();
        // let schema_desc = metadata.file_metadata().schema_descr();
        //
        // let columns = schema_desc
        //     .columns()
        //     .iter()
        //     .map(|desc| desc.name().to_string())
        //     .collect::<Vec<String>>();
        // assert_eq!(columns, vec!["name".to_string()]);
    }

    async fn read_checkpoint(path: &str) -> (Type, Vec<Action>) {
        println!("path: {}", path);
        let file = File::open(path).unwrap();
        let reader = SerializedFileReader::new(file).unwrap();
        let schema = reader.metadata().file_metadata().schema();
        let mut row_iter = reader.get_row_iter(None).unwrap();
        let mut actions = Vec::new();
        while let Some(record) = row_iter.next() {
            actions.push(Action::from_parquet_record(schema, &record).unwrap())
        }
        (schema.clone(), actions)
    }

    #[tokio::test]
    async fn load_table() {

        let cp_path = format!(
            "E:\\rustProject\\rust-cdc\\delta_table_test\\_delta_log\\00000000000000000010.checkpoint.parquet"
        );
        let (schema, actions) = read_checkpoint(&cp_path).await;

        // println!("schema: {:?}", schema);
        println!("actions: {:?}", actions);

        let mut table = deltalake::open_table(TABLE_PATH).await.unwrap();

        let files_urls = table.get_file_uris().collect::<Vec<_>>();
        println!("{}", table);

        let x = table.state.files();
        println!("{:?}", x);
        let x1 = table.get_state().commit_infos();
        println!("commit infos: {:?}", x1);

        let metadata = table.get_metadata().unwrap();
        println!("partition columns: {:?}", &metadata.partition_columns);

        let set = table.get_file_set();
        println!("file set: {:?}", set);

        let result = table.get_metadata().unwrap();
        println!("files: {:?}", result);

        let state = table.get_state();
        println!("files: {:?}", state);

        // let mut ctx = ExecutionContext::new();
        // ctx.register_table("demo", Arc::new(table)).unwrap();
        //
        // let batches = ctx
        //     .sql("SELECT * FROM demo").await.unwrap()
        //     .collect()
        //     .await.unwrap();
        // println!("{:?}", batches[0])

        let mut ctx = SessionContext::new();

        let file_format = ParquetFormat::default().with_enable_pruning(true);
        let listing_options = ListingOptions {
            file_extension: DEFAULT_PARQUET_EXTENSION.to_owned(),
            format: Arc::new(file_format),
            table_partition_cols: vec!["Id".to_string()],
            collect_stat: true,
            target_partitions: 10,
        };

        let schema = Schema::new(vec![
            // Field::new("Id", DataType::UInt16, true),
            Field::new("name", DataType::Utf8, true)
        ]);

        ctx.register_listing_table(
            "test_delta",
            &format!("{}", "E:\\rustProject\\rust-cdc\\delta_table_test"),
            listing_options,
            Some(Arc::new(schema)),
            None,
        ).await.unwrap();

        // execute the query
        let df = ctx
            .sql("SELECT * FROM test_delta",)
            .await.unwrap();

        // print the results
        df.show().await.unwrap();
    }


    pub async fn commit_add(table: &mut DeltaTable, add: &Add) -> i64 {
        commit_actions(table, vec![Action::add(add.clone())]).await
    }

    pub async fn commit_removes(table: &mut DeltaTable, removes: Vec<&Remove>) -> i64 {
        let vec = removes
            .iter()
            .map(|r| Action::remove((*r).clone()))
            .collect();
        commit_actions(table, vec).await
    }

    pub async fn commit_actions(table: &mut DeltaTable, actions: Vec<Action>) -> i64 {
        let mut tx = table.create_transaction(None);
        tx.add_actions(actions);
        tx.commit(None, None).await.unwrap()
    }


    use sqlparser::dialect::GenericDialect;
    use sqlparser::parser::Parser;
    use crate::delta::helper::create_delta_table;

    #[test]
    fn query_sql_parse() {
        let sql = "select id, name from user where id > 5 and name != 'beat'";
        // let dialect = GenericDialect {}; // or AnsiDialect, or your own dialect ...
        // let statements:Vec<Statement> = Parser::parse_sql(&dialect, sql).unwrap();

        let deque = DFParser::parse_sql(&sql).unwrap();

        println!("AST: {:?}", deque);
    }

    #[test]
    fn ddl_sql_parse() {
        let sql = "ALTER TABLE rustcdc ADD COLUMN name VARCHAR(50)";

        let dialect = GenericDialect {}; // or AnsiDialect, or your own dialect ...

        let statements:Vec<Statement> = Parser::parse_sql(&dialect, sql).unwrap();

        for statement in statements {
            match statement {
                AlterTable {
                    name,
                    operation,
                } => {
                    let idents:Vec<Ident> = name.0;
                    if let Some(ident) = idents.get(0) {
                        let table_name = ident.to_string();
                    }

                },
                Drop {
                    /// The type of the object to drop: TABLE, VIEW, etc.
                    object_type,
                    /// An optional `IF EXISTS` clause. (Non-standard.)
                    if_exists,
                    /// One or more objects to drop. (ANSI SQL requires exactly one.)
                    names,
                    /// Whether `CASCADE` was specified. This will be `false` when
                    /// `RESTRICT` or no drop behavior at all was specified.
                    cascade,
                    /// Hive allows you specify whether the table's stored data will be
                    /// deleted along with the dropped table
                    purge,
                } if object_type == ObjectType::Table => {

                },
                Truncate {
                    table_name,
                    ..
                } => {
                    let idents:Vec<Ident> = table_name.0;
                    if let Some(ident) = idents.get(0) {
                        let table_name = ident.to_string();
                    }
                },
                // CreateTable
                _ => {}
            }
        }

        // println!("AST: {:?}", statements);
    }
}

pub enum OpEnum {
    Add,
    Update,
    DropColumn,
    RenameColumn(String, String),
    RenameTable(String, String, String),
}

pub struct FieldAction {
    op: OpEnum,
    name: String,
}


fn parse_alter_table_op(operation: AlterTableOperation) {
    match operation {

        AddColumn {
            column_def: ColumnDef {
                name: Ident{ value, quote_style },
                data_type,
                collation,
                options
            }
        } => {
            // (OpEnum::Add, value, Some(data_type));
        },
        DropColumn {
            column_name: Ident{ value, quote_style },
            if_exists,
            cascade,
        } => {
            // (OpEnum::DropColumn, value, None);
        },
        RenameColumn {
            old_column_name: Ident{ value: o_value, quote_style: o_quote_style },
            new_column_name: Ident{ value: n_value, quote_style: n_quote_style },
        } => {
            // (OpEnum::RenameColumn(o_value, n_value), value, None);
        },
        RenameTable { table_name } => {
            let x:Vec<Ident> = table_name.0;
        },
        _ => {}
    }


    todo!()
}
