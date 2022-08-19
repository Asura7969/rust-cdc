use std::error::Error;
use std::io;
use std::io::ErrorKind;
use std::net::SocketAddr;
use bytes::BytesMut;
use tokio::net::TcpStream;
use tokio_util::codec::Framed;

// const HOST_PORT:String = String::from("test-mysql.starcharge.cloud:3306");

// #[tokio::main]
fn main() {
//
//     let addr: SocketAddr = String::from("test-mysql.starcharge.cloud:3306").parse().unwrap();
//     let stream = TcpStream::connect(&addr).await?;
//
//     // let secure_socket = Framed::new(socket, TransportCodec::new());
//     // let (mut to_server, mut from_server) = secure_socket.split();
}


pub struct BinLogClient<'a> {
    input_stream: ByteArrayInputStream<'a>,
}

impl <'a>BinLogClient<'a> {

    async fn receive_greeting(stream: &TcpStream) -> Result<(), Box<dyn Error>> {
        let mut msg = BytesMut::new();
        // let mut buf = vec![0; 1024];
        loop {
            stream.readable().await?;
            match stream.try_read(&mut msg) {
                Ok(n) => {
                    msg.truncate(n);
                    break;
                }
                Err(ref e) if e.kind() == ErrorKind::WouldBlock => {
                    continue;
                }
                Err(e) => {
                    return Err(e.into());
                }
            }
        }
        println!("GOT = {:?}", msg);
        Ok(())
    }
}


pub struct ByteArrayInputStream<'a> {
    stream: &'a TcpStream,
    peek: Option<i32>,
    block_length: i32,
}

impl <'a> ByteArrayInputStream<'a> {
    fn new(stream: &'a TcpStream) -> Self {
        Self{stream, peek: None, block_length: -1}
    }

    fn read_int(&mut self, length: i32) -> Result<i32, io::Error> {
        let mut result = 0;
        for i in 0..length {
            result |= self.read_0().unwrap() << (i << 3);
        }
        Ok(result)
    }

    fn read_long(&mut self, length: i32) -> Result<i32, io::Error> {
        let mut result: i64 = 0;
        let length = length as i64;
        for i in 0..length {
            result |= (self.read_0().unwrap() as i64) << (i << 3 as i64);
        }
        Ok(result as i32)
    }

    fn read_string(&mut self, length: i32) -> Result<String, io::Error> {
        match self.read(length) {
            Ok(bytes) => {
                if let Ok(s) = String::from_utf8(bytes) {
                    Ok(s)
                } else {
                    return Err(io::Error::from(ErrorKind::UnexpectedEof));
                }

            },
            Err(err) => Err(err),
        }
    }

    fn read_0(&mut self) -> Result<i32, io::Error> {
        let result = match self.peek {
            Some(i) => {
                self.peek = None;
                i
            },
            None => self.read_within_block_boundaries().unwrap(),
        };
        if result == -1 {
            return Err(io::Error::from(ErrorKind::UnexpectedEof));
        }
        return Ok(result);
    }

    fn read(&mut self, length: i32) -> Result<Vec<u8>, io::Error> {
        self.fill(0, length)
    }

    fn read_within_block_boundaries(&mut self) -> Result<i32, io::Error> {
        unimplemented!()
        // if self.block_length != -1 {
        //     if self.block_length == 0 {
        //         return Ok(-1);
        //     }
        //     self.block_length -= 1;
        // }
        // return inputStream.read();
    }

    fn fill(&mut self, offset: i32, length: i32) -> Result<Vec<u8>, io::Error> {
        unimplemented!()
        // let mut bytes:Vec<u8> = Vec::with_capacity(length as usize);
        // let mut remaining = length;
        // while remaining != 0 {
        //     let read = read(&mut bytes, offset + length - remaining, remaining);
        //     if read == -1 {
        //         return Err(io::Error::from(ErrorKind::UnexpectedEof));
        //     }
        //     remaining -= read;
        // }
        // Ok(bytes)
    }

}
