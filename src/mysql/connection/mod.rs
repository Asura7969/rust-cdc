// use crate::common::StatementCache;
// use crate::connection::{Connection, LogSettings};
use crate::error::Error;
// use crate::mysql::protocol::statement::StmtClose;
use crate::mysql::protocol::text::{Ping, Quit};
// use crate::mysql::statement::MySqlStatementMetadata;
// use crate::mysql::{MySql, MySqlConnectOptions};
// use crate::transaction::Transaction;
use futures_core::future::BoxFuture;
use futures_util::FutureExt;
use std::fmt::{self, Debug, Formatter};
use tokio::io::AsyncWriteExt;

mod establish;
mod stream;
mod auth;

pub(crate) use stream::{MySqlStream, Waiting};

const MAX_PACKET_SIZE: u32 = 1024;

/// A connection to a MySQL database.
pub struct MySqlConnection {
    // underlying TCP stream,
    // wrapped in a potentially TLS stream,
    // wrapped in a buffered stream
    pub(crate) stream: MySqlStream,

    // transaction status
    pub(crate) transaction_depth: usize,

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
