use rocksdb::{Options, DB};

fn main() {
    let path = "test_path";
    let db = DB::open_default(path).unwrap();
    db.put(b"my key", b"my value").unwrap();
    match db.get(b"my key") {
        Ok(Some(value)) => println!("retrieved value {}", String::from_utf8(value).unwrap()),
        Ok(None) => println!("value not found"),
        Err(e) => println!("operational problem encountered: {}", e),
    }
    println!("Hello, world!");
}
