pub mod concurrency;
pub mod db;
pub mod execute;
mod hlc;
mod interval;
mod latch_manager;
mod llrb;
pub mod lock_table;
mod storage;
mod timestamp_oracle;

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

pub static WRITE_INTENT_ERROR: &str = "write_intent_error";

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

#[cfg(test)]
mod tests {
    use serde::{Deserialize, Serialize};

    #[derive(Debug, Serialize, Deserialize)]
    struct Test {
        foo: bool,
    }

    #[test]
    fn test() {
        let mvcc = Test { foo: true };
        let str = serde_json::to_string(&mvcc).unwrap();
        let meta = serde_json::from_str::<Test>(&str).unwrap();
        let huh = "";

        let test_bool = true;
        let bool_str = serde_json::to_string(&test_bool).unwrap();
        let meta = serde_json::from_str::<bool>(&bool_str).unwrap();
        // println!("value: {:?}", meta);

        let vec = serde_json::to_vec(&test_bool).unwrap();
        let back = serde_json::from_slice::<bool>(&vec).unwrap();
        // println!("value: {:?}", back);

        let str = "foo";
        let vec = serde_json::to_vec(&str).unwrap();
        let back = serde_json::from_slice::<&str>(&vec).unwrap();
        println!("value: {:?}", back);
        // serde_json::from_slice(value)
    }
}
