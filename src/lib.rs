mod hlc;
mod interval_tree;
mod rustyDB;
mod storage;

use rocksdb::{Options, DB};
use serde::de::Error;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct StorageError {
    pub message_id: String,
    pub message: String,
}

impl StorageError {
    pub(crate) fn new(message_id: String, message: String) -> Self {
        Self {
            message_id,
            message,
        }
    }
}

impl From<rocksdb::Error> for StorageError {
    fn from(err: rocksdb::Error) -> Self {
        Self {
            message: err.into_string(),
            message_id: "rocksdb_error".to_owned(),
        }
    }
}

pub type ErrorIdentifier = String;
pub const WRITE_INTENT_ERROR: ErrorIdentifier = "write_intent_error".to_owned();

pub type StorageResult<T> = Result<T, StorageError>;

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
