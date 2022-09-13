use rustcdc::error::Error;
use rustcdc::mysql::{Listener, MySqlOption};
use rustcdc::snapshot::FileCommitter;


#[tokio::main]
async fn main() -> Result<(), Error> {

    let committer = FileCommitter::default();

    let mut stream = MySqlOption::new(Box::new(committer))
        .host("127.0.0.1")
        .port(3556)
        .username("root")
        .password(None)
        .database(Some("rustcdc".to_string()))
        .charset("utf8mb4")
        .server_id(5)
        .connect().await?;

    stream.register_listener(Listener::default());

    stream.start().await;

    Ok(())
}
