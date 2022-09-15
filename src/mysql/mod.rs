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


use std::cell::Cell;
use regex::Regex;
pub use event::*;
pub use connection::*;
pub use conn::*;

const ALL: &str = "*";

pub struct MatchStrategy {
    regexes: Vec<Regex>,
    skip_event: Cell<bool>,
}

/// 校验是否存在 *, 若存在只使用 * 匹配
fn check_all(vec: Vec<String>) -> Vec<String> {
    if vec.contains(&ALL.to_owned()) {
        vec![]
    } else {
        vec
    }
}

fn transform_expr(v: String, is_database: bool) -> String {
    let replace_star = v.replace(ALL, "(.)*");
    if is_database {
        format!("{}\\.", replace_star)
    } else {
        replace_star
    }
}

impl MatchStrategy {
    pub(crate) fn new(databases: Vec<String>, tables: Vec<String>) -> Self {
        let mut databases = check_all(databases);
        let tables = check_all(tables);

        let databases = if databases.is_empty() {
            vec![String::from("[\\s\\S]*\\.")]
        } else {
            databases.iter()
                .map(|db| transform_expr(db.clone(), true))
                .collect::<Vec<String>>()
        };

        let tables = if tables.is_empty() {
            vec![String::from("[\\s\\S]*")]
        } else {
            tables.iter()
                .map(|tb| transform_expr(tb.clone(), false))
                .collect::<Vec<String>>()
        };

        let regexes = databases.iter()
            .flat_map(|database_expr| {
                tables.iter()
                    .map(|table_expr| {
                        let expr = format!("{}{}", database_expr, table_expr.clone());
                        Regex::new(expr.as_str()).unwrap()
                    })
                    .collect::<Vec<Regex>>()
            }).collect::<Vec<Regex>>();

        Self { regexes, skip_event: Cell::new(false) }
    }

    pub(crate) fn hit(&self, db: &str, table: &str) -> bool {
        let db_table = format!("{}.{}", db, table);
        if self.regexes.iter().any(|r| r.is_match(db_table.as_str())) {
            self.skip_event.set(false);
            return true
        }
        !self.skip_event.get()
    }

    pub(crate) fn is_skip(&self) -> bool {
        self.skip_event.get()
    }
}


#[test]
fn match_test() {

    let database = vec!["flink_db*".to_owned(), "spark_db".to_owned()];
    let table = vec!["user_info".to_owned(), "order_info_*".to_owned()];

    let match_strategy = MatchStrategy::new(database, table);

    assert_eq!(match_strategy.hit("flink_db", "user_info"), true);
    assert_eq!(match_strategy.hit("flink_db", "order_info_1"), true);
    assert_eq!(match_strategy.hit("spark_db", "user_info"), true);
    assert_eq!(match_strategy.hit("spark_db", "order_info_2"), true);

    assert_eq!(!match_strategy.hit("hadoop_db", "user_info"), false);
    assert_eq!(!match_strategy.hit("hadoop_db", "order_info_"), false);
    assert_eq!(!match_strategy.hit("flink_db1", "user_info_1"), false);
    assert_eq!(!match_strategy.hit("flink_db1", "order_info_"), false);


    let match_strategy2 = MatchStrategy::new(vec!["*".to_string()], vec!["*".to_string()]);

    assert_eq!(match_strategy2.hit("flink_db", "user_info"), true);
    assert_eq!(match_strategy2.hit("hadoop_db", "user_info"), true);
}
