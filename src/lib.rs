extern crate core;

use std::str::FromStr;
use chrono::Local;
use log::LevelFilter;
use std::io::prelude::*;

pub mod mysql;
pub mod io;
pub(crate) mod net;
pub mod snapshot;

#[macro_use]
pub mod error;



pub(crate) fn init_logger() {
    let log_level = std::env::var("RUST_LOG")
        .ok()
        .and_then(|l| LevelFilter::from_str(l.as_str()).ok())
        .unwrap_or(log::LevelFilter::Info);

    let _ = env_logger::Builder::new()
        .format(move |buf, record| {
            writeln!(
                buf,
                "{} [{}] - {}",
                Local::now().format("%Y-%m-%dT%H:%M:%S"),
                record.level(),
                record.args(),
            )
        })
        .filter(None, log_level)
        .try_init();
}
