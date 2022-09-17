mod stream;
mod auth;

use std::collections::BTreeMap;
use crate::error::Error;
use crate::mysql::protocol::text::{Ping, Quit};
use futures_core::future::BoxFuture;
use futures_util::FutureExt;
use std::fmt::{self, Debug, Formatter};
use tokio::io::AsyncWriteExt;
use serde::{Serialize, Deserialize};

pub(crate) use stream::{MySqlStream};
use crate::mysql::event::{ColTypes};

pub(crate) const MAX_PACKET_SIZE: u32 = 1024;

/// A connection to a MySQL database.
pub struct MySqlConnection {
    // underlying TCP stream,
    // wrapped in a potentially TLS stream,
    // wrapped in a buffered stream
    pub(crate) stream: MySqlStream,

    // transaction status
    pub(crate) transaction_depth: usize,

    pub(crate) table_map: TableMap,

}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct SingleTableMap {
    pub(crate) table_id: u64,
    pub(crate) schema_name: String,
    pub(crate) table_name: String,
    pub(crate) columns: Vec<ColTypes>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct TableMap {
    inner: BTreeMap<u64, SingleTableMap>,
}

impl Default for TableMap {
    fn default() -> Self {
        Self::new()
    }
}

impl TableMap {
    pub fn new() -> Self {
        TableMap {
            inner: BTreeMap::new(),
        }
    }

    pub fn handle(
        &mut self,
        table_id: u64,
        schema_name: String,
        table_name: String,
        columns: Vec<ColTypes>,
    ) -> SingleTableMap {
        let map = SingleTableMap {
            table_id,
            schema_name,
            table_name,
            columns,
        };

        self.inner.insert(table_id, map.clone());
        map
    }

    pub fn add_table(&mut self,
                     table_id: u64,
                     table: SingleTableMap) {
        self.inner.insert(table_id, table);
    }

    pub fn get(&self, table_id: u64) -> Option<&SingleTableMap> {
        self.inner.get(&table_id)
    }
}

impl Debug for MySqlConnection {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("MySqlConnection").finish()
    }
}

impl MySqlConnection {

    fn close(mut self) -> BoxFuture<'static, Result<(), Error>> {
        Box::pin(async move {
            self.stream.send_packet(Quit).await?;
            self.stream.shutdown().await?;

            Ok(())
        })
    }

    fn close_hard(mut self) -> BoxFuture<'static, Result<(), Error>> {
        Box::pin(async move {
            self.stream.shutdown().await?;
            Ok(())
        })
    }

    fn ping(&mut self) -> BoxFuture<'_, Result<(), Error>> {
        Box::pin(async move {
            self.stream.wait_until_ready().await?;
            self.stream.send_packet(Ping).await?;
            self.stream.recv_ok().await?;

            Ok(())
        })
    }

    #[doc(hidden)]
    fn flush(&mut self) -> BoxFuture<'_, Result<(), Error>> {
        self.stream.wait_until_ready().boxed()
    }

    #[doc(hidden)]
    fn should_flush(&self) -> bool {
        !self.stream.wbuf.is_empty()
    }

}
