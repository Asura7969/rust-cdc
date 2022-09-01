use std::io;
use std::io::ErrorKind;
use std::net::SocketAddr;
use bytes::{Buf, BufMut, Bytes, BytesMut};
use tokio::net::TcpStream;
use tokio_util::codec::{Decoder, Encoder, Framed};
use crate::mysql::{Event, MAX_PACKET_SIZE, MySqlConnection, MysqlEvent, TableMap};
use crate::error::{DatabaseError, Error};
use crate::mysql::protocol::{Capabilities, Packet};
use futures::{FutureExt, TryFutureExt, SinkExt, StreamExt};
use futures_util::future::ok;
use futures_util::stream::{SplitSink, SplitStream};
use tokio::sync::broadcast;
use tokio::sync::broadcast::{Sender, Receiver};
use crate::err_protocol;
use crate::mysql::error::MySqlDatabaseError;
use crate::mysql::protocol::response::ErrPacket;
use crate::mysql::io::MySqlBufExt;
use crate::io::{BufExt, Decode, Encode};
use crate::mysql::collation::{CharSet, Collation};
use crate::mysql::protocol::connect::{AuthSwitchRequest, AuthSwitchResponse, BinlogFilenameAndPosition, ComBinlogDump, Handshake, HandshakeResponse};
use crate::mysql::protocol::text::Query;

const MAX_BLOCK_LENGTH: usize = 16777212;

struct MySqlOption {
    host: String,
    port: u16,
    username: String,
    password: Option<String>,
    database: Option<String>,
    server_id: u32,
    charset: String,
    collation: Option<String>,
    pipes_as_concat: bool,
}

impl MySqlOption {

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
            collation: None,
            pipes_as_concat: true,
        }
    }

    fn host(mut self, host: &str) -> Self {
        self.host = host.to_owned();
        self
    }

    fn port(mut self, port: u16) -> Self {
        self.port = port;
        self
    }

    fn username(mut self, username: &str) -> Self {
        self.username = username.to_owned();
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

    pub fn collation(mut self, collation: &str) -> Self {
        self.collation = Some(collation.to_owned());
        self
    }

    async fn connect(mut self) -> Result<(), Error> {
        let destination = format!("{}:{}", self.host.clone(), self.port);
        // 参数异常
        let addr: SocketAddr = destination.parse().unwrap();
        let socket = TcpStream::connect(&addr).await?;

        let capabilities = get_capabilities(self.database.clone());
        let secure_socket = Framed::new(socket,
                                        PacketCodec::new(capabilities.clone()));
        let (to_server, from_server) = secure_socket.split();

        let mut stream = MyStream::new(to_server, from_server, self, capabilities);
        stream.establish();
        stream.set_pipes_as_concat().await?;

        // todo: load snapshort or init new default

        stream.notify_listener().await;

        Ok(())
    }

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


pub struct Listener {
    fn_read: Box<dyn FnMut(MysqlEvent)>,
    fn_err: Box<dyn FnMut(Box<dyn DatabaseError>)>,
}

impl Listener {

    fn new(fn_read: Box<dyn FnMut(MysqlEvent)>,
           fn_err: Box<dyn FnMut(Box<dyn DatabaseError>)>,) -> Self {
        Self { fn_read, fn_err }
    }

}

pub struct MyStream {
    to_server: SplitSink<Framed<TcpStream, PacketCodec>, Bytes>,
    from_server: SplitStream<Framed<TcpStream, PacketCodec>>,
    option: MySqlOption,
    pub(crate) capabilities: Capabilities,
    pub(crate) sequence_id: u8,
    pub(crate) wbuf: Vec<u8>,
    pub(crate) server_version: (u16, u16, u16),
    table_map: TableMap,
    listeners: Vec<(Listener, Receiver<MysqlEvent>)>,
    sender: Sender<MysqlEvent>,
    receiver: Receiver<MysqlEvent>,
}


impl MyStream {

    fn new(to_server: SplitSink<Framed<TcpStream, PacketCodec>, Bytes>,
           from_server: SplitStream<Framed<TcpStream, PacketCodec>>,
           option: MySqlOption,
           capabilities: Capabilities,) -> Self {
        let (sender, receiver) = broadcast::channel(16);
        Self {
            to_server,
            from_server,
            option,
            capabilities,
            sequence_id: 0,
            wbuf: Vec::with_capacity(1024),
            server_version: (0, 0, 0),
            table_map: TableMap::default(),
            listeners: Vec::new(),
            sender,
            receiver
        }
    }

    fn register_listener(&mut self, listener: Listener) {
        self.listeners.push((listener, self.sender.subscribe()));
    }

    async fn notify_listener(&mut self) {

        let (tx, mut rx1) = broadcast::channel(100);
        for (mut listener, mut re) in self.listeners {
            tokio::spawn(async move {
                loop {
                    match re.recv().await {
                        Ok(msg) => (listener.fn_read)(msg),
                        Err(err) => {
                            // let error = MySqlDatabaseError(ErrPacket { error_code: 1, sql_state: None, error_message: err.to_string() });
                            // (listener.fn_err)(Box::new(error))
                        },
                    }
                }
            });
        }

        loop {
            match next_event(self, &mut self.table_map).await {
                Ok(event) => {
                    // send event to listener
                    tx.send(event).unwrap();
                },
                Err(error) => {
                    for (mut listener, _) in self.listeners {
                        // let error = MySqlDatabaseError(ErrPacket { error_code: 1, sql_state: None, error_message: "".to_string() });
                        // (listener.fn_err)(Box::new(error))
                    }
                }
            }
        }
    }

    async fn set_binlog_pos(&mut self) -> Result<(), Error> {
        // todo: fetchBinlogFilenameAndPosition
        self.write_packet(Query("show master status")).await;
        // stream.flush().await?;
        let binlog_fp: BinlogFilenameAndPosition = self.next_packet().await?.decode()?;
        let pos = binlog_fp.binlog_position;
        let file_name = binlog_fp.binlog_filename;

        // todo: fetchBinlogChecksum
        // show global variables like 'binlog_checksum'

        // enableHeartbeat

        // send COM_BINLOG_DUMP
        self.write_packet(ComBinlogDump {
            binlog_pos: pos,
            server_id: self.option.server_id,
            binlog_filename: file_name
        }).await;
        // stream.flush().await?;
        Ok(())
    }

    async fn establish(&mut self) -> Result<(), Error> {
        match self.next_packet().await {
            Ok(pk) => {
                let handshake: Handshake = pk.decode()?;

                let mut plugin = handshake.auth_plugin;
                let mut nonce = handshake.auth_plugin_data;

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

                self.server_version = (
                    server_version_major,
                    server_version_minor,
                    server_version_patch,
                );

                self.capabilities &= handshake.server_capabilities;
                self.capabilities |= Capabilities::PROTOCOL_41;

                self.capabilities.remove(Capabilities::SSL);

                let auth_response = if let (Some(plugin), Some(pwd)) = (plugin, self.option.password.clone()) {
                    Some(plugin.scramble(self, pwd.as_str(), &nonce).await?)
                } else {
                    None
                };
                let charset: CharSet = self.option.charset.parse()?;
                let collation: Collation = charset.default_collation();
                self.write_packet(HandshakeResponse {
                    collation: collation as u8,
                    max_packet_size: MAX_PACKET_SIZE,
                    username: self.option.username.clone().as_str(),
                    database: self.option.database.clone().as_deref(),
                    auth_plugin: plugin,
                    auth_response: auth_response.as_deref(),
                }).await;

                loop {
                    let packet = self.next_packet().await?;
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
                                    self,
                                    self.option.password.clone().as_deref().unwrap_or_default(),
                                    &nonce,
                                )
                                .await?;

                            self.write_packet(AuthSwitchResponse(response)).await;
                            // stream.flush().await?;
                        }

                        id => {
                            if let (Some(plugin), Some(password)) = (plugin, &self.option.password.clone()) {
                                if plugin.handle(self, packet, password, &nonce).await? {
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
                Ok(())
            },
            Err(err) => {
                Err(err)
            }
        }
    }

    async fn set_pipes_as_concat(&mut self) -> Result<(), Error> {
        let mut options = BytesMut::new();
        if self.option.pipes_as_concat {
            options.put(
                &b"SET sql_mode=(SELECT CONCAT(@@sql_mode, ',PIPES_AS_CONCAT,NO_ENGINE_SUBSTITUTION')),"[..]);
        } else {
            options.put(
                &b"SET sql_mode=(SELECT CONCAT(@@sql_mode, ',NO_ENGINE_SUBSTITUTION')),"[..],
            );
        }
        options.put(&b"time_zone='+00:00',"[..]);
        options.put(format!(
            r#"NAMES {} COLLATE {};"#,
            self.option.charset.as_str(),
            self.option.collation.as_ref().unwrap().as_str()
        ).as_bytes());
        if let Err(_) = self.to_server.send(options.freeze()).await {
            return Err(Error::Prepare("set sql modes error".to_owned()))
        }
        Ok(())
        // conn.execute(&*options).await?;
    }

    pub(crate) async fn next_packet(&mut self) -> Result<Packet<Bytes>, Error> {
        let (pk, id) = self.from_server.next().await.ok_or(err_protocol!("next packet decode error"))??;
        self.sequence_id = id;
        Ok(pk)
    }

    pub(crate) async fn write_packet<'en, T>(&mut self, payload: T)
        where
            T: Encode<'en, Capabilities>,
    {
        Packet(payload).encode_with(&mut self.wbuf,
                                    (self.capabilities, &mut self.sequence_id));

        let bytes = Bytes::from(self.wbuf.to_vec());
        if let Err(e) = self.to_server.send(bytes).await {
            println!("send packet error: {:?}", e);
        }
        self.wbuf.clear();
    }

    pub(crate) fn flush(&mut self) {
        let _ = self.to_server.flush();
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
    sequence_id: u8,
}

impl PacketCodec {
    fn new(capabilities: Capabilities) -> Self {
        PacketCodec {status: Status::START, packet_size: 0, capabilities, sequence_id: 0 }
    }
}

impl Decoder for PacketCodec {
    type Item = (Packet<Bytes>, u8);
    type Error = crate::error::Error;

    fn decode(&mut self, buf: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        if self.status == Status::START {
            if buf.len() < 4 {
                return Ok(None);
            }
            let mut header = buf.split_off(4);
            let packet_size = header.get_uint_le(3) as usize;
            let sequence_id = header.get_u8();
            self.sequence_id = sequence_id.wrapping_add(1);
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
        let id = self.sequence_id;
        Ok(Some((Packet(payload), id)))
    }
}

impl Encoder<Bytes> for PacketCodec {
    type Error = Error;

    fn encode(&mut self, item: Bytes, dst: &mut BytesMut) -> Result<(), Self::Error> {
        Ok(dst.put(item))
    }
}

async fn next_event(stream: &mut MyStream, table_map: &mut TableMap) -> Result<MysqlEvent, Error> {
    let packet = stream.next_packet().await?;
    let mut bytes = packet.0;
    // todo
    let (event, _) = Event::decode(bytes, table_map)?;
    Ok(event)
}
