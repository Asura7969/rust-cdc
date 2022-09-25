use rustcdc::error::Error;
use rustcdc::mysql::{Listener, MySqlOption};
use rustcdc::snapshot::SnapShotType;


#[tokio::main]
async fn main() -> Result<(), Error> {

    let mut stream = MySqlOption::new()
        .host("127.0.0.1")
        .port(3556)
        .username("root")
        .password(None)
        .database("rustcdc")
        .table(vec!["*".to_string()]) // all tables
        .charset("utf8mb4")
        .server_id(5)
        .snapshot(SnapShotType::FILE) // default
        .connect().await?;

    stream.register_listener(Listener::default());

    stream.start().await;

    Ok(())
}
