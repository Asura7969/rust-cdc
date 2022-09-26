use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use arrow::array::{StringArray, UInt16Array};
use arrow::array::Int32Array;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::json;
use deltalake::{action, checkpoints, DeltaDataTypeVersion, DeltaTable, DeltaTableBuilder, DeltaTableConfig, DeltaTableMetaData, SchemaDataType, SchemaField};
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


/// [delta schema]
///
/// [delta schema]: https://github.com/delta-io/delta/blob/master/PROTOCOL.md#Schema-Serialization-Format
pub enum DeltaStructType {
    String(bool),
    Long(bool),
    Integer(bool),
    Short(bool),
    Byte(bool),
    Float(bool),
    Double(bool),
    Decimal(bool),
    Boolean(bool),
    Binary(bool),
    Date(bool),
    Timestamp(bool),
    Array(DeltaStructType, bool),
    Map(DeltaStructType, DeltaStructType, bool),
}



/// table_uri: column_name -> column_type
pub fn create_delta_schema(table_schema: &HashMap<String, DeltaStructType>) -> deltalake::Schema {
    let fields = table_schema.iter().map(|(column_name, column_type)| {
        let field_name = column_name.clone();
        let (c_type, nullable) = match column_type {
            DeltaStructType::String(nullable) => ("string".to_string(), *nullable),
            DeltaStructType::Long(nullable) => ("long".to_string(), *nullable),
            DeltaStructType::Integer(nullable) => ("integer".to_string(), *nullable),
            DeltaStructType::Short(nullable) => ("short".to_string(), *nullable),
            DeltaStructType::Byte(nullable) => ("byte".to_string(), *nullable),
            DeltaStructType::Float(nullable) => ("float".to_string(), *nullable),
            DeltaStructType::Double(nullable) => ("double".to_string(), *nullable),
            DeltaStructType::Decimal(nullable) => ("decimal".to_string(), *nullable),
            DeltaStructType::Boolean(nullable) => ("boolean".to_string(), *nullable),
            DeltaStructType::Binary(nullable) => ("binary".to_string(), *nullable),
            DeltaStructType::Date(nullable) => ("date".to_string(), *nullable),
            DeltaStructType::Timestamp(nullable) => ("timestamp".to_string(), *nullable),
            _ => {
                todo!()
            }
            // DeltaStructType::Array(nullable) => ("array".to_string(), *nullable),
            // DeltaStructType::Map(nullable) => ("map".to_string(), *nullable),
        };
        SchemaField::new(field_name, SchemaDataType::primitive(c_type), nullable, HashMap::new())
    }).collect::<Vec<_>>();
    deltalake::Schema::new(fields)
}

#[cfg(test)]
mod tests {

    use super::*;

    use sqlparser::ast::{Ident, ObjectType, Statement};
    use sqlparser::ast::Statement::{AlterTable, Truncate};
    // use datafusion::execution::context::ExecutionContext;

    const TABLE_PATH:&str = "file:///E:/rustProject/rust-cdc/delta_table_test";

    pub async fn create_table_from_schema() -> DeltaTable {
        let schema = deltalake::Schema::new(
            vec![
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
                )]);

        let table_meta = DeltaTableMetaData::new(
            Some("delta-rs_test_table".to_owned()),
            Some("Table created by delta-rs tests".to_owned()),
            None,
            schema,
            vec![format!("Id")],
            HashMap::new(),
        );

        let mut table = DeltaTableBuilder::from_uri(&TABLE_PATH)
            .with_allow_http(true)
            .build()
            .unwrap();
        let protocol = action::Protocol {
            min_reader_version: 1,
            min_writer_version: 2,
        };

        table.create(table_meta, protocol, None, None).await.unwrap();

        table
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

            deltalake::checkpoints::create_checkpoint(table).await?;
            log::info!("Created checkpoint version {}.", version);

            let removed = deltalake::checkpoints::cleanup_metadata(table).await?;
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
