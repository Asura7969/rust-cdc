use std::collections::HashMap;
use std::fmt::format;
use deltalake::{action, DeltaTable, DeltaTableBuilder, DeltaTableMetaData, SchemaDataType, SchemaField, SchemaTypeStruct};
use crate::error::Error;

/// 创建delta table
pub(crate) async fn create_delta_table(delta_table_uri: &str,
                                       table_name: Option<String>,
                                       description: Option<String>,
                                       schema: SchemaTypeStruct,
                                       partition_columns: Vec<String>,
                                       configuration: HashMap<String, Option<String>>,) -> Result<DeltaTable, Error> {

    let table_meta = DeltaTableMetaData::new(
        table_name,
        description,
        None,
        schema,
        partition_columns,
        configuration,
    );
    let mut table = DeltaTableBuilder::from_uri(delta_table_uri)
        .with_allow_http(true)
        .build()?;
    let protocol = action::Protocol {
        min_reader_version: 1,
        min_writer_version: 2,
    };
    table.create(table_meta, protocol, None, None).await?;

    Ok(table)

}
