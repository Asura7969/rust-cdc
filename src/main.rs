use rustcdc::error::Error;
use rustcdc::mysql::{Listener, MySqlOption};
use rustcdc::snapshot::SnapShotType;


#[tokio::main]
async fn main() -> Result<(), Error> {

    /// database option
    /// --
    /// - *: all databases
    /// - rustcdc.*: Tables with under the rustcdc database
    /// - rustcdc.test*: Tables with the prefix test under the rustcdc database

    let mut stream = MySqlOption::new()
        .host("127.0.0.1")
        .port(3556)
        .username("root")
        .password(None)
        .database("rustcdc.*")
        .charset("utf8mb4")
        .server_id(5)
        .snapshot(SnapShotType::FILE) // default
        .connect().await?;

    stream.register_listener(Listener::default());

    stream.start().await;

    Ok(())
}
