#![allow(dead_code)]

use std::io;
use std::net::SocketAddr;
use std::path::Path;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::TcpStream;


#[derive(Debug)]
pub enum Socket {
    Tcp(TcpStream),

}

impl Socket {
    pub async fn connect_tcp(host: &str, port: u16) -> io::Result<Self> {
        // Trim square brackets from host if it's an IPv6 address as the `url` crate doesn't do that.
        TcpStream::connect((host.trim_matches(|c| c == '[' || c == ']'), port))
            .await
            .map(Socket::Tcp)
    }

    pub fn local_addr(&self) -> Option<SocketAddr> {
        match self {
            Self::Tcp(tcp) => tcp.local_addr().ok(),
        }
    }

    #[cfg(not(unix))]
    pub async fn connect_uds(_: impl AsRef<Path>) -> io::Result<Self> {
        Err(io::Error::new(
            io::ErrorKind::Other,
            "Unix domain sockets are not supported outside Unix platforms.",
        ))
    }

    pub async fn shutdown(&mut self) -> io::Result<()> {
        use tokio::io::AsyncWriteExt;

        match self {
            Socket::Tcp(s) => s.shutdown().await,
        }
    }
}

impl AsyncRead for Socket {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut super::PollReadBuf<'_>,
    ) -> Poll<io::Result<super::PollReadOut>> {
        match &mut *self {
            Socket::Tcp(s) => Pin::new(s).poll_read(cx, buf),
        }
    }
}

impl AsyncWrite for Socket {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        match &mut *self {
            Socket::Tcp(s) => Pin::new(s).poll_write(cx, buf),
        }
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match &mut *self {
            Socket::Tcp(s) => Pin::new(s).poll_flush(cx),
        }
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match &mut *self {
            Socket::Tcp(s) => Pin::new(s).poll_shutdown(cx),

        }
    }

}
