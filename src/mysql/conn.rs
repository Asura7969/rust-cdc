use std::net::SocketAddr;
use bytes::{BufMut, Bytes, BytesMut};
use tokio::net::TcpStream;
use tokio_util::codec::{Decoder, Encoder, Framed};
use crate::mysql::MySqlConnection;
use crate::error::Error;
use crate::mysql::protocol::Packet;
use futures::{SinkExt, StreamExt};

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

    async fn connect(mut self) {
        let destination = format!("{}:{}", self.host.clone(), self.port);
        // 参数异常
        let addr: SocketAddr = destination.parse().expect_err("parameter exception: host/port");
        let socket = TcpStream::connect(&addr).await?;

        let secure_socket = Framed::new(socket, PacketCodec { });
        let (mut to_server, mut from_server) = secure_socket.split();

        let mut stream = MyStream { };
        stream.init();
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

struct MyStream {

}


impl MyStream {
    fn init(&mut self) {

    }
}



struct PacketCodec {
    // stream: TcpStream,
}

impl Decoder for PacketCodec {
    type Item = Packet<Bytes>;
    type Error = Error;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        todo!()
    }
}

impl Encoder<Bytes> for MyStream {
    type Error = Error;

    fn encode(&mut self, item: Bytes, dst: &mut BytesMut) -> Result<(), Self::Error> {
        Ok(dst.put(item))
    }
}
