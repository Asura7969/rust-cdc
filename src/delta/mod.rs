use arrow::array::Int32Array;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::json;




#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::fs::File;
    use std::sync::Arc;
    use super::*;
    use deltalake::{action, DeltaTable, DeltaTableBuilder, DeltaTableConfig, DeltaTableMetaData, SchemaDataType, SchemaField};
    use arrow::record_batch::RecordBatch;
    use deltalake::action::{Action, Add, Remove};
    use deltalake::storage::DeltaObjectStore;
    use deltalake::writer::{DeltaWriter, RecordBatchWriter};
    use parquet::file::serialized_reader::SerializedFileReader;
    use serde_json::{Map, Value};
    use parquet::file::reader::FileReader;
    use datafusion::execution::context::ExecutionContext;

    const TABLE_PATH:&str = "file:///E:/rustProject/rust-cdc/delta_table_test";

    pub async fn create_table_from_schema() -> DeltaTable {
        let schema = deltalake::Schema::new(vec![SchemaField::new(
            "Id".to_string(),
            SchemaDataType::primitive("integer".to_string()),
            true,
            HashMap::new(),
        )]);

        let table_meta = DeltaTableMetaData::new(
            Some("delta-rs_test_table".to_owned()),
            Some("Table created by delta-rs tests".to_owned()),
            None,
            schema.clone(),
            vec![],
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

    #[tokio::test]
    async fn create_table() {
        let mut table = create_table_from_schema().await;

        let schema = Schema::new(vec![Field::new("Id", DataType::Int32, true)]);
        let a = Int32Array::from(vec![1, 2, 3]);
        let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(a)]).unwrap();

        let mut writer = RecordBatchWriter::for_table(&table).unwrap();

        if let Err(error) = writer.write(batch).await {
            panic!(error)
        }
        let add_actions = writer.flush().await.unwrap();
        let add = &add_actions[0];
        let path = table_dir.path().join(&add.path);

        let file = File::open(path.as_path()).unwrap();
        let reader = SerializedFileReader::new(file).unwrap();

        let metadata = reader.metadata();
        let schema_desc = metadata.file_metadata().schema_descr();

        let columns = schema_desc
            .columns()
            .iter()
            .map(|desc| desc.name().to_string())
            .collect::<Vec<String>>();
        assert_eq!(columns, vec!["Id".to_string()]);
    }

    #[tokio::test]
    async fn load_table() {
        let mut table = deltalake::open_table(TABLE_PATH).await.unwrap();
        let mut ctx = ExecutionContext::new();
        ctx.register_table("demo", Arc::new(table)).unwrap();

        let batches = ctx
            .sql("SELECT * FROM demo").await.unwrap()
            .collect()
            .await.unwrap();
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
}
