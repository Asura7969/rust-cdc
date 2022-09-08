# rust-cdc

MySQL binlog parser impl with Rust

Parsed events matrix:

| Hex  | Event Name               | Parsed | Note               |
| ---- | ------------------------ |--------|--------------------|
| 0x01 | START_EVENT_V3           | N      | too old to support |
| 0x02 | QUERY_EVENT              | Y      |                    |
| 0x03 | STOP_EVENT               | N      |                    |
| 0x04 | ROTATE_EVENT             | Y      |                    |
| 0x05 | INTVAR_EVENT             | Y      |                    |
| 0x06 | LOAD_EVENT               | N      |                    |
| 0x07 | SLAVE_EVENT              | N      |                    |
| 0x08 | CREATE_FILE_EVENT        | N      |                    |
| 0x09 | APPEND_BLOCK_EVENT       | N      |                    |
| 0x0a | EXEC_LOAD_EVENT          | N      |                    |
| 0x0b | DELETE_FILE_EVENT        | N      |                    |
| 0x0c | NEW_LOAD_EVENT           | N      |                    |
| 0x0d | RAND_EVENT               | N      |                    |
| 0x0e | USER_VAR_EVENT           | N      |                    |
| 0x0f | FORMAT_DESCRIPTION_EVENT | Y      |                    |
| 0x10 | XID_EVENT                | Y      |                    |
| 0x11 | BEGIN_LOAD_QUERY_EVENT   | Y      |                    |
| 0x12 | EXECUTE_LOAD_QUERY_EVENT | Y      |                    |
| 0x13 | TABLE_MAP_EVENT          | Y      | not fully tested   |
| 0x14 | WRITE_ROWS_EVENTv0       | N      |                    |
| 0x15 | UPDATE_ROWS_EVENTv0      | N      |                    |
| 0x16 | DELETE_ROWS_EVENTv0      | N      |                    |
| 0x17 | WRITE_ROWS_EVENTv1       | N      |                    |
| 0x18 | UPDATE_ROWS_EVENTv1      | N      |                    |
| 0x19 | DELETE_ROWS_EVENTv1      | N      |                    |
| 0x1a | INCIDENT_EVENT           | N      |                    |
| 0x1b | HEARTBEAT_EVENT          | N      |                    |
| 0x1c | IGNORABLE_EVENT          | N      |                    |
| 0x1d | ROWS_QUERY_EVENT         | Y      |                    |
| 0x1e | WRITE_ROWS_EVENTv2       | Y      | not fully tested   |
| 0x1f | UPDATE_ROWS_EVENTv2      | Y      | not fully tested   |
| 0x20 | DELETE_ROWS_EVENTv2      | Y      | not fully tested   |
| 0x21 | GTID_EVENT               | Y      |                    |
| 0x22 | ANONYMOUS_GTID_EVENT     | Y      |                    |
| 0x23 | PREVIOUS_GTIDS_EVENT     | Y      |                    |

### example

```rust
use rustcdc::error::Error;
use rustcdc::mysql::{Listener, MySqlOption};

#[tokio::main]
async fn main() -> Result<(), Error> {
    let mut stream = MySqlOption::new()
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
```
