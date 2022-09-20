




#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;
    use deltalake::{DeltaTable, DeltaTableBuilder, DeltaTableConfig};
    use deltalake::storage::DeltaObjectStore;
    use deltalake::writer::{DeltaWriter, RecordBatchWriter};
    use deltalake::writer::test_utils::get_record_batch;

    #[tokio::test]
    async fn open_table() {
        // let table_uri = "file:///foo/bar";
        let table_dir = tempfile::tempdir().unwrap();
        let table_path = table_dir.path();
        let config = HashMap::new();
        let backend: Arc<DeltaObjectStore> = DeltaTableBuilder::from_uri(table_path)
            .with_storage_options(config)
            .build_storage()
            .unwrap();

        let batch = get_record_batch(None, false);

        let mut table = DeltaTable::new(backend, DeltaTableConfig::default()).unwrap();
        let mut writer = RecordBatchWriter::for_table(&table).unwrap();

        writer.write(batch).await.unwrap();
        let adds = writer.flush().await.unwrap();

        assert_eq!(adds.len(), 1);
    }
}
