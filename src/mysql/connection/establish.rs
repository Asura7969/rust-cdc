use bytes::buf::Buf;
use bytes::Bytes;

use crate::err_protocol;
use crate::error::Error;
use crate::mysql::connection::{MySqlStream, MAX_PACKET_SIZE, MySqlConnection};
use crate::mysql::protocol::connect::{
    AuthSwitchRequest, AuthSwitchResponse, Handshake, HandshakeResponse,
};
use crate::mysql::protocol::Capabilities;

impl MySqlConnection {
    pub(crate) async fn establish(host: &str,
                                  port: u16,
                                  username: &str,
                                  password: Option<String>,
                                  database: Option<String>,
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

        Ok(Self {
            stream,
            transaction_depth: 0,
        })
    }
}
