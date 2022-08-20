mod socket;

pub use socket::Socket;

type PollReadBuf<'a> = tokio::io::ReadBuf<'a>;
type PollReadOut = ();
