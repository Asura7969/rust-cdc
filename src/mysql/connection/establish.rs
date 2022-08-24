use bytes::buf::Buf;
use bytes::{Bytes, BytesMut};

use crate::{err_parse, err_protocol};
use crate::error::Error;
use crate::mysql::connection::{MySqlStream, MAX_PACKET_SIZE, MySqlConnection, TableMap};
use crate::mysql::event::{Event, EventData, EventHeaderV4, EventType};
use crate::mysql::protocol::connect::{
    AuthSwitchRequest,
    AuthSwitchResponse,
    BinlogFilenameAndPosition,
    ComBinlogDump,
    Handshake,
    HandshakeResponse
};
use crate::mysql::protocol::text::Query;
use crate::mysql::protocol::Capabilities;

impl MySqlConnection {
    pub(crate) async fn establish(host: &str,
                                  port: u16,
                                  username: &str,
                                  password: Option<String>,
                                  database: Option<String>,
                                  server_id: u32,
                                  charset: String) -> Result<Self, Error> {
        let c_password = password.clone();
        let c_database = database.clone();
        let mut stream: MySqlStream = MySqlStream::connect(host, port, username, c_password, c_database, charset).await?;

        // https://dev.mysql.com/doc/dev/mysql-server/8.0.12/page_protocol_connection_phase.html
        // https://mariadb.com/kb/en/connection/

        let handshake: Handshake = stream.recv_packet().await?.decode()?;

        let mut plugin = handshake.auth_plugin;
        let mut nonce = handshake.auth_plugin_data;

        // FIXME: server version parse is a bit ugly
        // expecting MAJOR.MINOR.PATCH

        let mut server_version = handshake.server_version.split('.');

        let server_version_major: u16 = server_version
            .next()
            .unwrap_or_default()
            .parse()
            .unwrap_or(0);

        let server_version_minor: u16 = server_version
            .next()
            .unwrap_or_default()
            .parse()
            .unwrap_or(0);

        let server_version_patch: u16 = server_version
            .next()
            .unwrap_or_default()
            .parse()
            .unwrap_or(0);

        stream.server_version = (
            server_version_major,
            server_version_minor,
            server_version_patch,
        );

        stream.capabilities &= handshake.server_capabilities;
        stream.capabilities |= Capabilities::PROTOCOL_41;

        stream.capabilities.remove(Capabilities::SSL);

        let auth_response = if let (Some(plugin), Some(pwd)) = (plugin, password.clone()) {
            Some(plugin.scramble(&mut stream, pwd.as_str(), &nonce).await?)
        } else {
            None
        };

        stream.write_packet(HandshakeResponse {
            collation: stream.collation as u8,
            max_packet_size: MAX_PACKET_SIZE,
            username,
            database: database.as_deref(),
            auth_plugin: plugin,
            auth_response: auth_response.as_deref(),
        });

        stream.flush().await?;

        loop {
            let packet = stream.recv_packet().await?;
            match packet[0] {
                0x00 => {
                    let _ok = packet.ok()?;
                    break;
                }

                0xfe => {
                    let switch: AuthSwitchRequest = packet.decode()?;

                    plugin = Some(switch.plugin);
                    nonce = switch.data.chain(Bytes::new());

                    let response = switch
                        .plugin
                        .scramble(
                            &mut stream,
                            password.as_deref().unwrap_or_default(),
                            &nonce,
                        )
                        .await?;

                    stream.write_packet(AuthSwitchResponse(response));
                    stream.flush().await?;
                }

                id => {
                    if let (Some(plugin), Some(password)) = (plugin, &password) {
                        if plugin.handle(&mut stream, packet, password, &nonce).await? {
                            // plugin signaled authentication is ok
                            break;
                        }

                    // plugin signaled to continue authentication
                    } else {
                        return Err(err_protocol!(
                            "unexpected packet 0x{:02x} during authentication",
                            id
                        ));
                    }
                }
            }
        }

        // todo: fetchBinlogFilenameAndPosition
        stream.write_packet(Query("show master status"));
        stream.flush().await?;
        let packet = stream.recv_packet().await?;
        let binlog_fp: BinlogFilenameAndPosition = packet.decode()?;
        let pos = binlog_fp.binlog_position;
        let file_name = binlog_fp.binlog_filename;

        // todo: fetchBinlogChecksum
        // show global variables like 'binlog_checksum'

        // enableHeartbeat

        // send COM_BINLOG_DUMP
        stream.write_packet(ComBinlogDump {
            binlog_pos: pos,
            server_id,
            binlog_filename: file_name
        });
        stream.flush().await?;

        // load snapshort or init new default
        let mut table_map = TableMap::default();
        loop {
            match next_event(&mut stream, &mut table_map).await {
                Ok(event) => {
                    // send event to listener
                    unimplemented!()
                },
                Err(error) => {
                    // send error to listener
                    unimplemented!()
                }
            }
        }

        Ok(Self {
            stream,
            transaction_depth: 0,
            table_map,
        })
    }


}

async fn next_event(stream: &mut MySqlStream, table_map: &mut TableMap) -> Result<Event, Error> {
    let packet = stream.recv_packet().await?;
    let mut bytes = packet.0;
    let event = Event::decode(bytes, table_map)?;
    Ok(event)
}
