mod protocol;
mod error;
// pub(crate) mod statement;
// pub(crate) mod text;
mod io;
mod connection;
mod collation;
mod conn;
pub mod event;
mod value;


use regex::Regex;
pub use event::*;
pub use connection::*;
pub use conn::*;

const ALL: &str = "*";

pub struct MatchStrategy {
    regexes: Vec<Regex>,
    skip_event: bool,
}

/// 校验是否存在 *, 若存在只使用 * 匹配
fn check_all(vec: Vec<String>) -> Vec<String> {
    if vec.contains(&ALL.to_owned()) {
        vec![ALL.to_owned()]
    } else {
        vec
    }
}

fn transform_expression(v: String, is_database: bool) -> String {
    if v.contains(ALL) {
        let mut d = v.clone();
        match d.strip_prefix(ALL) {
            Some(s) => if is_database { format!("{}(.)*\\.", s) } else { format!("{}(.)*", s) },
            None => d
        }
    } else {
        v
    }
}

impl MatchStrategy {
    fn new(databases: Vec<String>, tables: Vec<String>) -> Self {
        let mut databases = check_all(databases);
        let tables = check_all(tables);

        let regexes = databases.iter()
            .flat_map(|database| {
                let db = transform_expression(database.clone(), true);
                tables.iter()
                    .map(|table| {
                        let expr = format!("{}{}", db, transform_expression(table.clone(), false));
                        Regex::new(expr.as_str()).unwrap()
                    })
                    .collect::<Vec<Regex>>()
            }).collect::<Vec<Regex>>();

        Self { regexes, skip_event: false }
    }

    fn hit(&mut self, event: &MysqlEvent) -> bool {
        if event.header.event_type != EventType::TableMapEvent {
            // &self.regexes.to_vec().

            todo!()
        } else {
            false
        }
    }
}
