use std::net::SocketAddr;
use bytes::{Buf, BufMut, Bytes, BytesMut};
use tokio::net::TcpStream;
use tokio_util::codec::{Decoder, Encoder, Framed};
use crate::mysql::MySqlConnection;
use crate::error::Error;
use crate::mysql::protocol::{Capabilities, Packet};
use futures::{SinkExt, StreamExt};
use crate::err_protocol;
use crate::mysql::error::MySqlDatabaseError;
use crate::mysql::protocol::response::ErrPacket;
use crate::mysql::io::MySqlBufExt;
use crate::io::{BufExt, Decode};

const MAX_BLOCK_LENGTH: usize = 16777212;

struct MySqlBuilder {
    host: String,
    port: u16,
    username: String,
    password: Option<String>,
    database: Option<String>,
    server_id: u32,
    charset: String,
    pipes_as_concat: bool,
}

impl MySqlBuilder {

    fn new() -> Self {
        Self {
            port: 3306,
            host: String::from("localhost"),
            // socket: None,
            username: String::from("root"),
            password: None,
            database: None,
            charset: String::from("utf8mb4"),
            server_id: 33675,
            // collation: None,
            // ssl_mode: MySqlSslMode::Preferred,
            // ssl_ca: None,
            pipes_as_concat: true,
        }
    }

    fn host(mut self, host: String) -> Self {
        self.host = host;
        self
    }

    fn port(mut self, port: u16) -> Self {
        self.port = port;
        self
    }

    fn username(mut self, username: String) -> Self {
        self.username = username;
        self
    }

    fn password(mut self, password: Option<String>) -> Self {
        self.password = password;
        self
    }

    fn database(mut self, database: Option<String>) -> Self {
        self.database = database;
        self
    }

    fn server_id(mut self, server_id: u32) -> Self {
        self.server_id = server_id;
        self
    }

    async fn connect(mut self) -> Result<(), Error> {
        let destination = format!("{}:{}", self.host.clone(), self.port);
        // 参数异常
        let addr: SocketAddr = destination.parse().unwrap();
        let socket = TcpStream::connect(&addr).await?;


        let secure_socket = Framed::new(socket,
                                        PacketCodec::new(get_capabilities(self.database.clone())));
        let (mut to_server, mut from_server) = secure_socket.split();

        let mut stream = MyStream { };
        stream.init();
        Ok(())
    }




    // async fn build(&mut self) -> Result<Self, Error> {
    //     let mut conn = MySqlConnection::establish(
    //         self.host.as_str(), self.port, self.username.as_str(), self.password.clone(),
    //         self.database.clone(), self.server_id, self.charset.clone())?;
    //
    //     let mut options = String::new();
    //     if self.pipes_as_concat {
    //         options.push_str(
    //             r#"SET sql_mode=(SELECT CONCAT(@@sql_mode, ',PIPES_AS_CONCAT,NO_ENGINE_SUBSTITUTION')),"#);
    //     } else {
    //         options.push_str(
    //             r#"SET sql_mode=(SELECT CONCAT(@@sql_mode, ',NO_ENGINE_SUBSTITUTION')),"#,
    //         );
    //     }
    //     options.push_str(r#"time_zone='+00:00',"#);
    //     options.push_str(&format!(
    //         r#"NAMES {} COLLATE {};"#,
    //         conn.stream.charset.as_str(),
    //         conn.stream.collation.as_str()
    //     ));
    //     unimplemented!()
    //     // conn.execute(&*options).await?;
    // }
}

fn get_capabilities(database: Option<String>) -> Capabilities {
    let mut capabilities = Capabilities::PROTOCOL_41
        | Capabilities::IGNORE_SPACE
        | Capabilities::DEPRECATE_EOF
        | Capabilities::FOUND_ROWS
        | Capabilities::TRANSACTIONS
        | Capabilities::SECURE_CONNECTION
        | Capabilities::PLUGIN_AUTH_LENENC_DATA
        | Capabilities::MULTI_STATEMENTS
        | Capabilities::MULTI_RESULTS
        | Capabilities::PLUGIN_AUTH
        | Capabilities::PS_MULTI_RESULTS
        | Capabilities::SSL;

    if database.is_some() {
        capabilities |= Capabilities::CONNECT_WITH_DB;

    }
    capabilities
}

struct MyStream {

}


impl MyStream {
    fn init(&mut self) {

    }
}

#[derive(Debug, PartialEq, Clone)]
pub(crate) enum Status {
    START,
    PACKET,
}

struct PacketCodec {
    status: Status,
    packet_size: usize,
    capabilities: Capabilities,
}

impl PacketCodec {
    fn new(capabilities: Capabilities) -> Self {
        PacketCodec {status: Status::START, packet_size: 0, capabilities}
    }
}

impl Decoder for PacketCodec {
    type Item = Packet<Bytes>;
    type Error = Error;

    fn decode(&mut self, buf: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        if self.status == Status::START {
            if buf.len() < 4 {
                return Ok(None);
            }
            let mut header = buf.split_off(4);
            let packet_size = header.get_uint_le(3) as usize;
            let sequence_id = header.get_u8();
            let _sequence_id = sequence_id.wrapping_add(1);
            if buf.remaining() < packet_size {
                self.status = Status::PACKET;
                self.packet_size = packet_size;
                return Ok(None);
            }
        } else if buf.remaining() < self.packet_size {
            return Ok(None);
        }

        let payload = buf.split_off(self.packet_size).freeze();

        // TODO: packet compression
        // TODO: packet joining

        if payload
            .get(0)
            .ok_or(err_protocol!("Packet empty"))?
            .eq(&0xff)
        {
            self.packet_size = 0;
            self.status = Status::START;
            // self.waiting.pop_front();

            // instead of letting this packet be looked at everywhere, we check here
            // and emit a proper Error
            return Err(
                MySqlDatabaseError(ErrPacket::decode_with(payload, self.capabilities)?).into(),
            );
        }

        if self.packet_size == MAX_BLOCK_LENGTH {
            // todo: packet size more than 16MB
            return Err(err_protocol!("packet size more than 16MB"));
        }
        self.packet_size = 0;
        self.status = Status::START;
        Ok(Some(Packet(payload)))
    }
}

impl Encoder<Bytes> for PacketCodec {
    type Error = Error;

    fn encode(&mut self, item: Bytes, dst: &mut BytesMut) -> Result<(), Self::Error> {
        Ok(dst.put(item))
    }
}
