use std::iter::Peekable;

use rocksdb::*;
use serde::de::DeserializeOwned;

use super::{
    mvcc_key::{decode_mvcc_key, encode_mvcc_key, MVCCKey},
    Value,
};

pub struct IterOptions {
    pub prefix: bool,
}

pub type KVBytes = (Box<[u8]>, Box<[u8]>);

pub fn new_mvcc_iterator<'a>(iter_options: IterOptions, db: rocksdb::DB) -> MVCCIterator<'a> {
    // RocksDBIterator { db: db, it: () }
    todo!()
}

// A wrapper around rocksdb iterator
pub struct MVCCIterator<'a> {
    pub it: Peekable<DBIterator<'a>>,

    // Determines whether to use prefix seek or not
    prefix: bool,

    curr_kv: Option<KVBytes>,

    // if is_done is true, then curr_kv is guaranteed to be None
    is_done: bool,
}

impl<'a> MVCCIterator<'a> {
    // next() will immediately be called to try obtain a valid tuple
    pub fn new(db: &'a DB, options: IterOptions) -> Self {
        let it = db.iterator(IteratorMode::Start).peekable();
        let mut mvcc_it = MVCCIterator {
            it,
            prefix: options.prefix,
            curr_kv: None,
            is_done: false,
        };
        mvcc_it.next();
        mvcc_it
    }

    // Returns true if pointing at valid entry, false otherwise
    pub fn next(&mut self) -> bool {
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
            None => {
                self.is_done = true;
                self.curr_kv = None;
                false
            }
        }
    }

    // will panic if called without a valid key.
    // To avoid that, call valid first to verify
    // not the most efficient solution now
    pub fn current_key(&mut self) -> MVCCKey {
        let (k, _) = self.curr_kv.as_ref().unwrap();
        MVCCIterator::convert_raw_key_to_mvcc_key(k)
    }

    pub fn current_raw_key(&mut self) -> &Box<[u8]> {
        let (k, _) = self.curr_kv.as_ref().unwrap();
        k
    }

    pub fn convert_raw_key_to_mvcc_key(raw_key: &Box<[u8]>) -> MVCCKey {
        let vec = Vec::from(raw_key.as_ref());
        let hm = decode_mvcc_key(&vec);
        if let None = hm {
            let foo = "";
        }
        decode_mvcc_key(&vec).unwrap()
    }

    // not the most efficient solution now
    pub fn current_value(&mut self) -> Value {
        let (_, v) = self.curr_kv.as_ref().unwrap();
        let vec = Vec::from(v.as_ref());
        vec
    }

    pub fn current_value_serialized<T: DeserializeOwned>(&mut self) -> T {
        let (_, v) = self.curr_kv.as_ref().unwrap();
        let vec = Vec::from(v.as_ref());
        serde_json::from_slice::<T>(&vec).unwrap()
    }

    // Valid returns whether or not the iterator is pointing at a valid entry.
    // If it is false, then current_key, current_value, next(), etc should not be called
    pub fn valid(&mut self) -> bool {
        !self.is_done
    }

    // Prefix: true would be best if seek_ge is called
    // Advances the iterator to the first MVCCKey >= key.
    // Returns true if the iterator is pointing at an entry that's
    // <= provided key, false otherwise.
    // This is a bit counter intuitive because greater than actually means
    // a smaller timestamp. But because our custom comparator stores
    // the most recent timestamp first, the ordering is opposite to the
    // semantic comparison of timestamps
    pub fn seek_ge(&mut self, key: &MVCCKey) -> bool {
        let mut found_valid = false;
        if !self.valid() {
            return false;
        }
        let curr_key = self.current_key();
        if &curr_key <= key {
            return true;
        }
        loop {
            let peeked = self.it.peek();

            match peeked {
                Some(res) => match res {
                    Ok((k, _)) => {
                        let peeked_key = MVCCIterator::convert_raw_key_to_mvcc_key(k);
                        if &peeked_key <= key {
                            found_valid = true;
                            self.next();
                            break;
                        } else {
                            self.next();
                            continue;
                        }
                    }
                    _ => break,
                },
                None => break,
            }
        }
        found_valid
    }
}

mod tests {
    use rocksdb::{IteratorMode, DB};

    use crate::{
        hlc::timestamp::{get_intent_timestamp, Timestamp},
        storage::{
            mvcc,
            mvcc_key::{encode_mvcc_key, MVCCKey},
            storage::Storage,
            str_to_key,
        },
    };

    use super::{IterOptions, MVCCIterator};

    #[test]
    fn test_current_key_and_current_value() {
        let mut storage = Storage::new_cleaned("./tmp/testt");
        let mvcc_key = MVCCKey::new(
            "hello",
            Timestamp {
                logical_time: 12,
                wall_time: 12,
            },
        );
        storage.put_serialized_with_mvcc_key(mvcc_key.to_owned(), 12);
        let mut iterator = MVCCIterator::new(&storage.db, IterOptions { prefix: false });
        assert!(iterator.valid());
        let key = iterator.current_key();
        assert_eq!(key, mvcc_key);

        let value = iterator.current_value_serialized::<i32>();
        println!("value: {:?}", value);
        assert_eq!(value, 12);

        iterator.next();
        assert_eq!(iterator.valid(), false);
    }

    #[test]
    fn test_order_of_iteration_with_intent_timestamp() {
        let mut storage = Storage::new_cleaned("./tmp/testt");
        let key = "hello";

        let mvcc_key_2 = MVCCKey::new(
            &key,
            Timestamp {
                logical_time: 2,
                wall_time: 2,
            },
        );

        let key_2_value = 12;

        let mvcc_key_12 = MVCCKey::new(
            &key,
            Timestamp {
                logical_time: 12,
                wall_time: 12,
            },
        );
        let key_12_value = 15;

        let intent_key = MVCCKey::new(&key, get_intent_timestamp());
        let intent_value = 10;
        storage
            .put_serialized_with_mvcc_key(mvcc_key_12.to_owned(), key_12_value)
            .unwrap();
        storage
            .put_serialized_with_mvcc_key(mvcc_key_2.to_owned(), key_2_value)
            .unwrap();
        storage
            .put_serialized_with_mvcc_key(intent_key.to_owned(), intent_value)
            .unwrap();

        let mut iterator = MVCCIterator::new(&storage.db, IterOptions { prefix: false });

        assert!(iterator.valid());
        let current_key = iterator.current_key();
        assert_eq!(current_key, intent_key);
        let value = iterator.current_value_serialized::<i32>();
        assert_eq!(value, intent_value);

        iterator.next();
        assert!(iterator.valid());
        let current_key = iterator.current_key();
        assert_eq!(current_key, mvcc_key_12);
        let value = iterator.current_value_serialized::<i32>();
        assert_eq!(value, key_12_value);

        iterator.next();
        assert!(iterator.valid());
        let current_key = iterator.current_key();
        assert_eq!(current_key, mvcc_key_2);
        let value = iterator.current_value_serialized::<i32>();
        assert_eq!(value, key_2_value);

        iterator.next();
        assert_eq!(iterator.valid(), false);
    }

    mod test_seek_ge {
        use crate::{
            hlc::timestamp::Timestamp,
            storage::{
                mvcc_iterator::{IterOptions, MVCCIterator},
                mvcc_key::MVCCKey,
                storage::Storage,
            },
        };

        #[test]
        fn test_multiple_timestamps_with_same_prefix() {
            let mut storage = Storage::new_cleaned("./tmp/testt");
            let key = "foo";
            let mvcc_key_1 = MVCCKey::new(
                key,
                Timestamp {
                    logical_time: 1,
                    wall_time: 1,
                },
            );
            storage
                .put_serialized_with_mvcc_key(mvcc_key_1, 12)
                .unwrap();

            let mvcc_key_6 = MVCCKey::new(
                key,
                Timestamp {
                    wall_time: 6,
                    logical_time: 6,
                },
            );
            storage
                .put_serialized_with_mvcc_key(mvcc_key_6.to_owned(), 12)
                .unwrap();

            let mvcc_key_4 = MVCCKey::new(
                key,
                Timestamp {
                    wall_time: 4,
                    logical_time: 4,
                },
            );
            storage
                .put_serialized_with_mvcc_key(mvcc_key_4, 12)
                .unwrap();

            let mut iterator = MVCCIterator::new(&storage.db, IterOptions { prefix: true });

            let key5 = MVCCKey::new(
                key,
                Timestamp {
                    wall_time: 5,
                    logical_time: 5,
                },
            );
            let seek_res = iterator.seek_ge(&key5);
            assert_eq!(seek_res, true);
            assert_eq!(iterator.current_key(), mvcc_key_6);
        }

        #[test]
        fn empty_db() {
            let storage = Storage::new_cleaned("./tmp/testt");
            let key = "foo";
            let mvcc_key_1 = MVCCKey::new(
                key,
                Timestamp {
                    logical_time: 1,
                    wall_time: 1,
                },
            );
            let mut iterator = MVCCIterator::new(&storage.db, IterOptions { prefix: true });
            let seek_res = iterator.seek_ge(&mvcc_key_1);
            assert_eq!(seek_res, false);
        }
    }
}
