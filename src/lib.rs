mod hlc;
mod interval_tree;
mod rustyDB;
mod storage;

use rocksdb::{Options, DB};
use serde::de::Error;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct StorageError {
    pub(crate) message: String,
}

impl StorageError {
    pub(crate) fn new(message: String) -> Self {
        Self { message }
    }
}

impl From<rocksdb::Error> for StorageError {
    fn from(err: rocksdb::Error) -> Self {
        Self {
            message: err.into_string(),
        }
    }
}

impl From<String> for StorageError {
    fn from(message: String) -> Self {
        Self { message }
    }
}

pub type DBResult<T> = Result<T, StorageError>;

fn main() {
    let path = "test_temp_db";
    let db = DB::open_default(path).unwrap();
    db.put(b"my key 2", b"my value 2").unwrap();
    db.put(b"my key 3", b"my value 2").unwrap();
    // let iterator = rustyDB
    // match db.get(b"my key") {
    //     Ok(Some(value)) => println!("retrieved value {}", String::from_utf8(value).unwrap()),
    //     Ok(None) => println!("value not found"),
    //     Err(e) => println!("operational problem encountered: {}", e),
    // }
    println!("Hello, world!");
}
