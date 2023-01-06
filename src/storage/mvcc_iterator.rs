use rocksdb::*;

use super::{mvcc_key::MVCCKey, Value};

pub struct IterOptions {}

pub type KVBytes = (Box<[u8]>, Box<[u8]>);

pub fn new_mvcc_iterator<'a>(iter_options: IterOptions, db: rocksdb::DB) -> MVCCIterator<'a> {
    // RocksDBIterator { db: db, it: () }
    todo!()
}

// A wrapper around rocksdb iterator
pub struct MVCCIterator<'a> {
    pub db: rocksdb::DB,
    pub it: DBIterator<'a>,
    curr_kv: KVBytes,
}

impl<'a> MVCCIterator<'a> {
    fn next(&mut self) -> () {
        let kv_bytes = self.it.next();
    }

    fn current_key(&mut self) -> MVCCKey {
        todo!()
    }

    fn current_value() -> Value {
        todo!()
    }

    fn seek_ge(&mut self, key: MVCCKey) -> () {
        // loop {
        //     let next = self.it.next();
        //     match next {
        //         Some(res) => {
        //             if let Ok((k, v)) = res {
        //                 let key = String::from_utf8(k.to_vec()).unwrap();
        //             }
        //         }
        //         None => break,
        //     }
        // }
        todo!()
    }

    fn valid(&mut self) -> bool {
        todo!()
    }
}

mod tests {
    use rocksdb::{IteratorMode, DB};

    use crate::storage::storage::Storage;

    #[test]
    fn foo() {
        let storage = Storage::new("./tmp/hello");
        let db = storage.db;
        db.put("foo", "bar").unwrap();
        let mut it = db.iterator(IteratorMode::Start);
        let next = it.next();
        match next {
            Some(res) => {
                let (k, v) = res.unwrap();
                let key = String::from_utf8(k.to_vec()).unwrap();
                // let value = String::from_utf8(v.to_vec()).unwrap();
                let value = serde_json::from_slice::<&str>(v.as_ref());
                println!("key: {}", key);
                println!("value: {:?}", value);
            }
            _ => {}
        }
    }
}
