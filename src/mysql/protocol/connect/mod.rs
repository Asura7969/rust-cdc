//! Connection Phase
//!
//! <https://dev.mysql.com/doc/internals/en/connection-phase.html>

mod auth_switch;
mod handshake;
mod handshake_response;
mod ssl_request;
mod binlog_fp;
mod com_binlog_dump;

pub(crate) use auth_switch::{AuthSwitchRequest, AuthSwitchResponse};
pub(crate) use handshake::Handshake;
pub(crate) use handshake_response::HandshakeResponse;
pub(crate) use ssl_request::SslRequest;
pub(crate) use binlog_fp::BinlogFilenameAndPosition;
pub(crate) use com_binlog_dump::ComBinlogDump;
