




#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;
    use deltalake::{DeltaTable, DeltaTableBuilder, DeltaTableConfig};
    use deltalake::storage::DeltaObjectStore;
    use deltalake::writer::{DeltaWriter, RecordBatchWriter};
    use deltalake::writer::test_utils::get_record_batch;

    #[test]
    fn open_table() {
        let table_uri = "file:///foo/bar";

        let config = HashMap::new();
        let backend: Arc<DeltaObjectStore> = DeltaTableBuilder::from_uri(table_uri)
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
