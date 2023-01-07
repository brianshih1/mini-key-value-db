use rocksdb::*;
use serde::de::DeserializeOwned;

use super::{
    mvcc_key::{decode_mvcc_key, encode_mvcc_key, MVCCKey},
    Value,
};

pub struct IterOptions {
    prefix: bool,
}

pub type KVBytes = (Box<[u8]>, Box<[u8]>);

pub fn new_mvcc_iterator<'a>(iter_options: IterOptions, db: rocksdb::DB) -> MVCCIterator<'a> {
    // RocksDBIterator { db: db, it: () }
    todo!()
}

// A wrapper around rocksdb iterator
pub struct MVCCIterator<'a> {
    pub it: DBIterator<'a>,

    // Determines whether to use prefix seek or not
    prefix: bool,

    curr_kv: Option<KVBytes>,

    is_done: bool,
}

impl<'a> MVCCIterator<'a> {
    fn new(db: &'a DB, options: IterOptions) -> Self {
        let it = db.iterator(IteratorMode::Start);
        MVCCIterator {
            it,
            prefix: options.prefix,
            curr_kv: None,
            is_done: false,
        }
    }

    // Returns true if pointing at valid entry, false otherwise
    fn next(&mut self) -> bool {
        // Returns None when iteration is finished.
        let optional_res = self.it.next();
        match optional_res {
            Some(res) => match res {
                Ok(kv) => {
                    self.curr_kv = Some(kv);
                    true
                }
                _ => {
                    self.is_done = true;
                    self.curr_kv = None;
                    false
                }
            },
            _ => false,
        }
    }

    // will panic if called without a valid key.
    // To avoid that, call valid first to verify
    // not the most efficient solution now
    fn current_key(&mut self) -> MVCCKey {
        let (k, _) = self.curr_kv.as_ref().unwrap();
        let vec = Vec::from(k.as_ref());
        decode_mvcc_key(&vec)
    }

    // not the most efficient solution now
    fn current_value(&mut self) -> Value {
        let (_, v) = self.curr_kv.as_ref().unwrap();
        let vec = Vec::from(v.as_ref());
        vec
    }

    fn current_value_serialized<T: DeserializeOwned>(&mut self) -> T {
        let (_, v) = self.curr_kv.as_ref().unwrap();
        let vec = Vec::from(v.as_ref());
        serde_json::from_slice::<T>(&vec).unwrap()
    }

    // Valid returns whether or not the iterator is pointing at a valid entry.
    // If it is false, then current_key, current_value, next(), etc should not be called
    fn valid(&mut self) -> bool {
        !self.is_done
    }

    fn seek_ge(&mut self, key: MVCCKey) -> () {
        let encoded = encode_mvcc_key(&key);

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
}

mod tests {
    use rocksdb::{IteratorMode, DB};

    use crate::{
        hlc::timestamp::Timestamp,
        storage::{
            mvcc,
            mvcc_key::{encode_mvcc_key, MVCCKey},
            storage::Storage,
        },
    };

    use super::{IterOptions, MVCCIterator};

    #[test]
    fn test_coercion() {
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

    #[test]
    fn test_next() {
        let mut storage = Storage::new_cleaned("./tmp/hello");
        let mvcc_key = MVCCKey::new(
            "hello",
            Timestamp {
                logical_time: 12,
                wall_time: 12,
            },
        );
        storage.put_mvcc_serialized(&mvcc_key, 12);
        let mut iterator = MVCCIterator::new(&storage.db, IterOptions { prefix: false });
        iterator.next();
        assert!(iterator.valid());
        let key = iterator.current_key();
        assert_eq!(key, mvcc_key);

        let value = iterator.current_value_serialized::<i32>();
        println!("value: {:?}", value);
        assert_eq!(value, 12);
    }
}
