mod rustyDB;

use rocksdb::{Options, DB};

pub struct RustyError {
    pub(crate) message: String,
}

impl RustyError {
    pub(crate) fn new(message: String) -> Self {
        Self { message }
    }
}

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
