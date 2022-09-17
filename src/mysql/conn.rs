use std::cell::RefCell;
use std::collections::HashMap;
use std::future::Future;
use std::net::SocketAddr;
use std::str::from_utf8;
use std::sync::{Arc, Mutex, RwLock};
use std::time::Duration;
use bytes::{Buf, BufMut, Bytes, BytesMut};
use tokio::net::TcpStream;
use tokio_util::codec::{Decoder, Encoder, Framed};
use crate::mysql::{ColumnDefinition, decode_column_def, Event, EventType, MatchStrategy, MAX_PACKET_SIZE, MysqlEvent, MysqlPayload, TableMap};
use crate::error::Error;
use crate::mysql::protocol::{Capabilities, Packet};
use crate::mysql::protocol::response::{EofPacket, Status as crate_Status};
use crate::mysql::io::MySqlBufExt;

use futures::{SinkExt, StreamExt};
use futures_util::stream::{SplitSink, SplitStream};
use log::{error, info, warn};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::runtime::Builder;
use tokio::task::JoinHandle;
use tokio::time;
use crate::{err_parse, err_protocol, init_logger};
use crate::mysql::error::MySqlDatabaseError;
use crate::mysql::protocol::response::{ErrPacket, OkPacket};
use crate::io::{BufExt, Decode, Encode};
use crate::mysql::collation::{CharSet, Collation};
use crate::mysql::protocol::connect::{AuthSwitchRequest, AuthSwitchResponse, BinlogFilenameAndPosition, ComBinlogDump, Handshake, HandshakeResponse};
use crate::mysql::protocol::row::Row;
use crate::mysql::protocol::text::{Ping, Query};
use crate::snapshot::{FileCommitter, LogCommitter, SnapShotType};

const MAX_BLOCK_LENGTH: usize = 16777212;

pub struct MySqlOption {
    host: String,
    port: u16,
    username: String,
    password: Option<String>,
    database: Option<String>,
    table: Option<Vec<String>>,
    server_id: u32,
    charset: String,
    collation: Option<String>,
    pipes_as_concat: bool,
    snapshot: Option<SnapShotType>,
    log_committer: Option<Box<dyn LogCommitter>>,
}

impl MySqlOption {

    pub fn new() -> Self {
        Self {
            port: 3306,
            host: String::from("localhost"),
            username: String::from("root"),
            password: None,
            database: None,
            table: None,
            charset: String::from("utf8mb4"),
            server_id: 33675,
            // collation: None,
            // ssl_mode: MySqlSslMode::Preferred,
            // ssl_ca: None,
            collation: None,
            pipes_as_concat: true,
            snapshot: Some(SnapShotType::FILE),
            log_committer: None
        }
    }

    pub fn host(mut self, host: &str) -> Self {
        self.host = host.to_owned();
        self
    }

    pub fn port(mut self, port: u16) -> Self {
        self.port = port;
        self
    }

    pub fn username(mut self, username: &str) -> Self {
        self.username = username.to_owned();
        self
    }

    pub fn password(mut self, password: Option<String>) -> Self {
        self.password = password;
        self
    }

    /// database option
    /// --
    /// - *: all databases
    /// - rustcdc.*: Tables with under the rustcdc database
    /// - rustcdc.test*: Tables with the prefix test under the rustcdc database
    pub fn database(mut self, database: &str) -> Self {
        self.database = Some(database.to_owned());
        self
    }

    pub fn table(mut self, table: Vec<String>) -> Self {
        self.table = Some(table);
        self
    }

    pub fn server_id(mut self, server_id: u32) -> Self {
        self.server_id = server_id;
        self
    }

    pub fn collation(mut self, collation: &str) -> Self {
        self.collation = Some(collation.to_owned());
        self
    }

    pub fn charset(mut self, charset: &str) -> Self {
        self.charset = charset.to_owned();
        self
    }

    pub fn snapshot(mut self, snapshot_type: SnapShotType) -> Self {
        self.snapshot = Some(snapshot_type);
        self
    }

    pub fn log_committer(mut self, log_committer: Box<dyn LogCommitter>) -> Self {
        self.log_committer = Some(log_committer);
        self
    }

    pub async fn connect(self) -> Result<MyStream, Error> {
        init_logger();

        let destination = format!("{}:{}", self.host.clone(), self.port);
        info!("目标地址: {}", destination);
        // TODO:参数异常
        let addr: SocketAddr = destination.parse().unwrap();
        let socket = TcpStream::connect(&addr).await?;

        let capabilities = get_capabilities(self.database.clone());
        let secure_socket = Framed::new(socket,
                                        PacketCodec::new(capabilities.clone()));
        let (to_server, from_server) = secure_socket.split();

        let mut stream = MyStream::new(to_server, from_server, self, capabilities);

        stream.establish().await?;
        stream.set_pipes_as_concat().await?;
        stream.set_checksum().await?;
        stream.set_binlog_pos().await?;
        info!("rust cdc connected successfully!");
        Ok(stream)
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
    fn_err: Box<dyn FnMut(Error)>,
}

impl Listener {
    pub fn new(fn_read: Box<dyn FnMut(MysqlEvent)>,
           fn_err: Box<dyn FnMut(Error)>,) -> Self {
        Self { fn_read, fn_err }
    }
}

impl Default for Listener {
    fn default() -> Self {
        Self { fn_read: Box::new(|event| {
            println!("event: {:?}", event);
        }), fn_err: Box::new(|err| {
            eprintln!("err: {:?}", err);
        }) }
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
    listener: Listener,
    binlog_file_name: String,
    log_committer: Arc<Mutex<Box<dyn LogCommitter + Send>>>,
    match_strategy: MatchStrategy,

}

fn get_log_committer(option: &MySqlOption) -> Box<dyn LogCommitter + Send> {
    if let Some(snapshot_type) = &option.snapshot{
        match *snapshot_type {
            SnapShotType::FILE => Box::new(FileCommitter::default()),
            _ => unimplemented!()
        }
    } else {
        // TODO
        unimplemented!()
    }

}
impl MyStream {

    fn new(to_server: SplitSink<Framed<TcpStream, PacketCodec>, Bytes>,
           from_server: SplitStream<Framed<TcpStream, PacketCodec>>,
           option: MySqlOption,
           capabilities: Capabilities) -> Self {
        let log_committer = get_log_committer(&option);
        let db = match &option.database {
            Some(db) => db.clone(),
            None => "*".to_owned()
        };

        let tables = match &option.table {
            Some(db) => db.clone(),
            None => vec!["*".to_owned()]
        };
        let match_strategy = MatchStrategy::new(vec![db], tables);

        Self {
            to_server,
            from_server,
            option,
            capabilities,
            sequence_id: 0,
            wbuf: Vec::with_capacity(1024),
            server_version: (0, 0, 0),
            table_map: TableMap::default(),
            listener: Listener::default(),
            binlog_file_name: "".to_owned(),
            log_committer: Arc::new(Mutex::new(log_committer)),
            match_strategy
        }
    }



    pub fn register_listener(&mut self, listener: Listener) {
        self.listener = listener;
    }

    fn start_recorder(&mut self) -> impl Future<Output=()> {
        let mut lock = self.log_committer.lock().unwrap();
        lock.open().unwrap();
        drop(lock);
        let mut recorder = Arc::clone(&self.log_committer);
        // let mut file_name_lock = Arc::clone(&self.binlog_file_name);
        // let mut log_pos_lock = Arc::clone(&self.binlog_position);

        async move {
            info!("log recorder successfully!");
            loop {
                time::sleep(Duration::from_secs(5)).await;
                // let file_name = (&*file_name_lock.read().unwrap().clone());
                // let log_pos = *log_pos_lock.read().unwrap();
                // let r = LogEntry::new(file_name, log_pos);
                let mut guard = recorder.lock().unwrap();
                match guard.commit() {
                    Err(err) => error!("recorder error: {:?}", err),
                    _ => {}
                };
            }
            ()
        }
    }

    pub async fn start(&mut self) {
        let runtime = Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .unwrap();

        runtime.spawn(self.start_recorder());

        info!("start listen event ...");
        loop {
            match self.next_event().await {
                Ok(event) => {
                    if event.header.event_type == EventType::RotateEvent {
                        let (position, next_binlog) = if let MysqlPayload::RotateEvent {
                            position,
                            next_binlog,
                            ..
                        } = event.body {
                            (position, next_binlog)
                        } else {
                            unimplemented!()
                        };

                        self.binlog_file_name = next_binlog.clone();
                        self.recode_binlog(position);
                        continue;
                    } else if event.header.event_type != EventType::TableMapEvent {
                        if event.header.log_pos > 0 {
                            self.recode_binlog(event.header.log_pos as u64);
                        }
                        if let MysqlPayload::TableMapEvent {
                            table_id,
                            schema_name: database,
                            table,
                            columns,
                            ..
                        } = &event.body {
                            self.match_strategy.hit(database.as_str(), table.as_str());
                            let table_map = self.table_map.handle(*table_id, database.clone(), table.clone(), columns.clone());
                            let mut recorder_lock = self.log_committer.lock().unwrap();
                            if let Err(error) = recorder_lock.recode_table_metadata(table_map) {
                                error!("recode table metadata error: {:?}\n table_id: {}", error, table_id);
                            }
                            drop(recorder_lock);
                        };
                    }
                    if !self.match_strategy.is_skip() {
                        // send event to listener
                        (self.listener.fn_read)(event)
                    }

                },
                Err(error) => {
                    if !self.match_strategy.is_skip() {
                        (self.listener.fn_err)(error)
                    }
                }
            }
        }
    }

    pub(crate) fn recode_binlog(&mut self, log_pos: u64) {
        let next_binlog = self.binlog_file_name.clone();
        let mut recorder_lock = self.log_committer.lock().unwrap();
        if let Err(error) = recorder_lock.recode_binlog(next_binlog.clone(), log_pos) {
            error!("recode binlog-metadata error: {:?}\n binlog: {}, pos: {}", error, next_binlog, log_pos);
        }
    }

    pub(crate) async fn set_checksum(&mut self) -> Result<(), Error> {
        self.ping().await?;
        let mut command = String::new();
        command.push_str("set @master_binlog_checksum= @@global.binlog_checksum");
        self.send_packet(Query(command.as_str())).await;
        let _ = self.next_packet().await?.ok();
        Ok(())
    }

    pub async fn ping(&mut self) -> Result<(), Error> {
        self.send_packet(Ping).await;
        self.recv_ok().await?;
        Ok(())
    }

    pub(crate) async fn recv_ok(&mut self) -> Result<OkPacket, Error> {
        self.next_packet().await?.ok()
    }

    async fn next_event(&mut self) -> Result<MysqlEvent, Error> {
        let packet = self.next_packet().await?;
        let mut bytes = packet.0;
        bytes.get_bytes(1);
        let (event, _) = Event::decode(bytes, &mut self.table_map)?;
        Ok(event)
    }

    pub(crate) async fn set_binlog_pos(&mut self) -> Result<(), Error> {

        // Load snapshot or start from latest offset
        let snapshot = match self.log_committer.lock().unwrap().get_latest_record() {
            Ok(Some(log_entry)) => {
                for (id, table) in &log_entry.tables {
                    self.table_map.add_table(*id, table.clone());
                }

                (Some(log_entry.file_name), Some(log_entry.log_pos))
            },
            _ => (None, None),
        };

        let com_binlog_dump = match snapshot {
            (Some(f), Some(o)) => {
                ComBinlogDump {
                    binlog_pos: o as u32,
                    server_id: self.option.server_id,
                    binlog_filename: f
                }
            },
            _ => {
                info!("从 binlog 最新偏移量消费!");

                self.send_packet(Query("show master status")).await;
                match self.fetch().await? {
                    Some(row) => {
                        let file_name_bytes = row.get(0).expect("binlog file name parse error");
                        let file_name = from_utf8(file_name_bytes)?;

                        let mut pos_bytes = Bytes::from(row.get(1)
                            .expect("binlog pos parse error")
                            .to_vec());
                        let pos_len = pos_bytes.len();
                        let pos: u32 = pos_bytes.get_str(pos_len)?.parse()?;
                        let gtid_set = from_utf8(&row.get(4).expect("GTID_SET parse error"))?;

                        info!("file_name: {}, pos: {}, gtid_set: {}", file_name.clone(), pos, gtid_set);

                        self.binlog_file_name = file_name.clone().to_string();
                        self.recode_binlog(pos as u64);

                        ComBinlogDump {
                            binlog_pos: pos,
                            server_id: self.option.server_id,
                            binlog_filename: file_name.to_string()
                        }
                    },
                    None => return Err(err_parse!("fetch latest binlog info failed!"))
                }
            }
        };

        // todo: fetchBinlogChecksum
        // show global variables like 'binlog_checksum'

        // enableHeartbeat
        info!("send COM_BINLOG_DUMP");
        // send COM_BINLOG_DUMP
        self.send_packet(com_binlog_dump).await;
        self.maybe_recv_eof().await?;
        Ok(())

    }

    async fn fetch(&mut self) -> Result<Option<Row>, Error> {
        let mut columns = Arc::new(Vec::new());
        loop {
            let mut packet = self.next_packet().await?;
            if packet[0] == 0x00 || packet[0] == 0xff {
                let ok = packet.ok()?;

                if ok.status.contains(crate_Status::SERVER_MORE_RESULTS_EXISTS) {
                    // more result sets exist, continue to the next one
                    continue;
                }
                return Ok(None);
            }

            let num_columns = packet.get_uint_lenenc() as usize; // column count

            let _column_names = Arc::new(recv_result_metadata(self, num_columns, Arc::make_mut(&mut columns)).await?);

            loop {
                let packet = self.next_packet().await?;

                if packet[0] == 0xfe && packet.len() < 9 {
                    let eof = packet.eof(self.capabilities)?;

                    if eof.status.contains(crate_Status::SERVER_MORE_RESULTS_EXISTS) {
                        // more result sets exist, continue to the next one
                        // self.stream.busy = Busy::Result;
                        break;
                    }
                    return Ok(None);
                }

                let row = Row::decode_row(packet.0, &columns)?;

                return Ok(Some(row))
            }
        }
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
        let mut command = String::new();
        if self.option.pipes_as_concat {
            command.push_str(r#"SET sql_mode=(SELECT CONCAT(@@sql_mode, ',PIPES_AS_CONCAT,NO_ENGINE_SUBSTITUTION')),"#);
        } else {
            command.push_str(
                r#"SET sql_mode=(SELECT CONCAT(@@sql_mode, ',NO_ENGINE_SUBSTITUTION')),"#,
            );
        }

        let charset: CharSet = self.option.charset.parse()?;
        let collation: Collation = self.option.collation
            .as_deref()
            .map(|collation| collation.parse())
            .transpose()?
            .unwrap_or_else(|| charset.default_collation());
        command.push_str(r#"time_zone='+00:00',"#);
        command.push_str(&format!(
            r#"NAMES {} COLLATE {};"#,
            charset.as_str(),
            collation.as_str()
        ));
        self.send_packet(Query(command.as_str())).await;
        let _ = self.next_packet().await?.ok();
        Ok(())
        // conn.execute(&*options).await?;
    }

    pub(crate) async fn next_packet(&mut self) -> Result<Packet<Bytes>, Error> {
        // let (pk, id) = self.from_server.next().await.ok_or(err_protocol!("next packet decode error"))??;
        let (pk, id) = self.from_server.next().await.unwrap()?;
        self.sequence_id = id;
        Ok(pk)
    }

    pub(crate) async fn send_packet<'en, T>(&mut self, payload: T)
        where
            T: Encode<'en, Capabilities>,
    {
        self.sequence_id = 0;
        self.write_packet(payload).await
    }

    pub(crate) async fn maybe_recv_eof(&mut self) -> Result<Option<EofPacket>, Error> {
        if self.capabilities.contains(Capabilities::DEPRECATE_EOF) {
            Ok(None)
        } else {
            self.recv().await.map(Some)
        }
    }

    pub(crate) async fn recv<'de, T>(&mut self) -> Result<T, Error>
        where
            T: Decode<'de, Capabilities>,
    {
        self.next_packet().await?.decode_with(self.capabilities)
    }

    pub(crate) async fn write_packet<'en, T>(&mut self, payload: T)
        where
            T: Encode<'en, Capabilities>,
    {
        // https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_command_phase.html
        Packet(payload).encode_with(&mut self.wbuf,
                                    (self.capabilities, &mut self.sequence_id));

        let bytes = Bytes::from(self.wbuf.to_vec());
        if let Err(e) = self.to_server.send(bytes).await {
            error!("send packet error: {:?}", e);
        }
        self.wbuf.clear();
    }

    pub(crate) fn flush(&mut self) {
        let _ = self.to_server.flush();
    }
}
fn recv_next_result_column(def: &ColumnDefinition, ordinal: usize) -> Result<String, Error> {
    // if the alias is empty, use the alias
    // only then use the name
    let name = match (def.name()?, def.alias()?) {
        (_, alias) if !alias.is_empty() => alias.to_owned(),
        (name, _) => name.to_owned(),
    };

    // let type_info = MySqlTypeInfo::from_column(&def);
    //
    // Ok(MySqlColumn {
    //     name,
    //     type_info,
    //     ordinal,
    //     flags: Some(def.flags),
    // })
    Ok(name)
}

async fn recv_result_metadata(
    stream: &mut MyStream,
    num_columns: usize,
    columns: &mut Vec<ColumnDefinition>,
) -> Result<HashMap<String, usize>, Error> {
    // the result-set metadata is primarily a listing of each output
    // column in the result-set

    let mut column_names = HashMap::with_capacity(num_columns);

    columns.clear();
    columns.reserve(num_columns);

    for ordinal in 0..num_columns {
        let pkg = stream.next_packet().await?;
        let (def, _) = decode_column_def(pkg.0)?;

        let column = recv_next_result_column(&def, ordinal)?;

        column_names.insert(column.clone(), ordinal);
        columns.push(def);
    }

    stream.maybe_recv_eof().await?;

    Ok(column_names)
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
            let mut header = buf.split_to(4);

            let packet_size = header.get_uint_le(3) as usize;
            let sequence_id = header.get_u8();

            self.sequence_id = sequence_id.wrapping_add(1);
            self.packet_size = packet_size;
            self.status = Status::PACKET;

            if buf.remaining() < packet_size {
                return Ok(None);
            }
        } else if buf.remaining() < self.packet_size {
            return Ok(None);
        }

        let payload = buf.split_to(self.packet_size).freeze();
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
        // println!("pl: {:?}", payload.clone().to_vec());
        Ok(Some((Packet(payload), id)))
    }
}

impl Encoder<Bytes> for PacketCodec {
    type Error = Error;

    fn encode(&mut self, item: Bytes, dst: &mut BytesMut) -> Result<(), Self::Error> {
        Ok(dst.put(item))
    }
}


