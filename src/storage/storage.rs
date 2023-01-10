use std::{cmp::Ordering, path::Path};

use rocksdb::DB;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use uuid::Uuid;

use crate::{StorageError, StorageResult};

use super::{
    mvcc_iterator::{IterOptions, MVCCIterator},
    mvcc_key::{decode_mvcc_key, encode_mvcc_key, MVCCKey},
};

pub struct Storage {
    pub db: DB,
}

impl Storage {
    // A very non-performant way to sort keys...
    // MVCCKeys are sorted in descending orders since we want the most recent
    // timestamp to be sorted first
    fn compare(first: &[u8], second: &[u8]) -> Ordering {
        let first_mvcc = decode_mvcc_key(&first.to_vec());
        let second_mvcc = decode_mvcc_key(&second.to_vec());
        match (first_mvcc, second_mvcc) {
            (None, None) => first.cmp(second),
            (None, Some(_)) => first.cmp(second),
            (Some(_), None) => first.cmp(second),
            (Some(first_mvcc), Some(second_mvcc)) => {
                let key_ordering = first_mvcc.key.cmp(&second_mvcc.key);
                match key_ordering {
                    Ordering::Less => key_ordering,
                    Ordering::Equal => {
                        if first_mvcc.is_intent_key() {
                            // intent keys (keys with empty timestamps) are always sorted
                            // on top of timestamped keys
                            Ordering::Less
                        } else if second_mvcc.is_intent_key() {
                            Ordering::Greater
                        } else {
                            second_mvcc.timestamp.cmp(&first_mvcc.timestamp)
                        }
                    }
                    Ordering::Greater => key_ordering,
                }
            }
        }
    }

    // path example: "./tmp/data";
    pub fn new(path: &str) -> Storage {
        let mut options = rocksdb::Options::default();
        options.create_if_missing(true);
        options.set_comparator("mvcc_ordering", Storage::compare);
        let db = DB::open(&options, path).unwrap();
        Storage { db }
    }

    pub fn new_iterator(&self, iter_options: IterOptions) -> MVCCIterator {
        MVCCIterator::new(&self.db, iter_options)
    }

    // path example: "./tmp/data";
    pub fn new_cleaned(path: &str) -> Storage {
        if Path::new(path).exists() {
            std::fs::remove_dir_all(path).unwrap();
        }
        Storage::new(path)
    }

    pub fn put_raw(&mut self, key: &str, value: Vec<u8>) -> StorageResult<()> {
        self.db.put(key, value).map_err(|e| StorageError::from(e))
    }

    pub fn put_serialized<T: Serialize>(&mut self, key: &str, value: T) -> StorageResult<()> {
        let str_res = serde_json::to_string(&value);
        match str_res {
            Ok(serialized) => self.put_raw(&key, serialized.into_bytes()),
            Err(err) => Err(StorageError::new("put_error".to_owned(), err.to_string())),
        }
    }

    pub fn put_serialized_with_mvcc_key<T: Serialize>(
        &mut self,
        key: MVCCKey,
        value: T,
    ) -> StorageResult<()> {
        let encoded = encode_mvcc_key(&key);
        let str_res = serde_json::to_string(&value);
        match str_res {
            Ok(serialized) => Ok(self.db.put(encoded, serialized.into_bytes()).unwrap()),
            Err(err) => Err(StorageError::new(
                "put_mvcc_error".to_owned(),
                err.to_string(),
            )),
        }
    }

    pub fn get_serialized_with_mvcc_key<T: DeserializeOwned>(
        &mut self,
        key: &MVCCKey,
    ) -> StorageResult<T> {
        let encoded = encode_mvcc_key(key);
        let res = self.db.get(encoded);
        match res {
            Ok(optional) => match optional {
                Some(value) => Ok(serde_json::from_slice::<T>(&value).unwrap()),
                None => Err(StorageError::new(
                    "not found".to_owned(),
                    "not found".to_owned(),
                )),
            },
            Err(err) => Err(StorageError::new(
                "get_mvcc_error".to_owned(),
                err.to_string(),
            )),
        }
    }

    pub fn get_serialized<T: DeserializeOwned>(&self, key: &str) -> StorageResult<T> {
        let res = self.db.get(key);
        match res {
            Ok(optional) => match optional {
                Some(value) => Ok(serde_json::from_slice::<T>(&value).unwrap()),
                None => Err(StorageError::new(
                    "not_found".to_owned(),
                    "not_found".to_owned(),
                )),
            },
            Err(err) => Err(StorageError::new(
                "serialized_error".to_owned(),
                err.to_string(),
            )),
        }
    }
}

#[cfg(test)]
mod Test {
    use serde::{Deserialize, Serialize};

    use crate::{hlc::timestamp::Timestamp, storage::mvcc_key::MVCCKey};

    use super::Storage;

    #[derive(Debug, Serialize, Deserialize, Eq, PartialEq, PartialOrd, Ord)]
    struct Test {
        foo: bool,
    }

    #[test]
    fn put_and_get() {
        let mut storage = Storage::new_cleaned("./tmp/foo");
        let test = Test { foo: false };
        storage.put_serialized("foo", &test).unwrap();
        let retrieved = storage.get_serialized::<Test>("foo").unwrap();
        assert_eq!(test, retrieved);
    }

    #[test]
    fn put_mvcc() {
        let mut storage = Storage::new_cleaned("./tmp/foo");
        let mvcc_key = MVCCKey::new(
            "hello",
            Timestamp {
                logical_time: 12,
                wall_time: 12,
            },
        );
        storage
            .put_serialized_with_mvcc_key(mvcc_key.to_owned(), 12)
            .unwrap();
        let retrieved = storage
            .get_serialized_with_mvcc_key::<i32>(&mvcc_key)
            .unwrap();
        assert_eq!(retrieved, 12);
    }

    mod storage_order {
        use rocksdb::IteratorMode;

        use crate::{
            hlc::timestamp::Timestamp,
            storage::{mvcc_iterator::MVCCIterator, mvcc_key::MVCCKey, storage::Storage},
        };

        #[test]
        fn check_order_no_intent() {
            let mut storage = Storage::new_cleaned("./tmp/foo");
            let first_mvcc_key = MVCCKey::new("a", Timestamp::new(1, 0));
            let second_mvcc_key = MVCCKey::new("a", Timestamp::new(2, 0));

            storage
                .put_serialized_with_mvcc_key(second_mvcc_key.to_owned(), 13)
                .unwrap();
            storage
                .put_serialized_with_mvcc_key(first_mvcc_key.to_owned(), 12)
                .unwrap();
            let mut it = storage.db.iterator(IteratorMode::Start);
            let (k, v) = it.next().unwrap().unwrap();
            let key = MVCCIterator::convert_raw_key_to_mvcc_key(&k);
            assert_eq!(key, second_mvcc_key);
            let (second_k, v) = it.next().unwrap().unwrap();
            let second_key = MVCCIterator::convert_raw_key_to_mvcc_key(&second_k);
            assert_eq!(second_key, first_mvcc_key);
        }

        #[test]
        fn check_order_intent() {
            let mut storage = Storage::new_cleaned("./tmp/foobars");
            let key = "a";
            let intent_key = MVCCKey::create_intent_key_with_str(key);
            let non_intent_key = MVCCKey::new(key, Timestamp::new(2, 0));
            let put_res1 = storage.put_serialized_with_mvcc_key(intent_key.to_owned(), 12);
            let put_res2 = storage.put_serialized_with_mvcc_key(non_intent_key.to_owned(), 13);

            let mut it = storage.db.iterator(IteratorMode::Start);

            let (k, _) = it.next().unwrap().unwrap();
            let key = MVCCIterator::convert_raw_key_to_mvcc_key(&k);
            assert_eq!(key, intent_key);
            let next = it.next();
            let (second_k, _) = next.unwrap().unwrap();
            let second_key = MVCCIterator::convert_raw_key_to_mvcc_key(&second_k);
            assert_eq!(second_key, non_intent_key);
        }
    }

    mod compare {
        use std::cmp::Ordering;

        use crate::{
            hlc::timestamp::Timestamp,
            storage::{
                mvcc_key::{create_intent_key, encode_mvcc_key, MVCCKey},
                storage::Storage,
                str_to_key,
            },
        };

        #[test]
        fn different_key() {
            let first_mvcc_key = MVCCKey::new("a", Timestamp::new(12, 12));
            let second_mvcc_key = MVCCKey::new("b", Timestamp::new(12, 12));
            let ordering = Storage::compare(
                &encode_mvcc_key(&first_mvcc_key),
                &encode_mvcc_key(&second_mvcc_key),
            );
            assert_eq!(ordering, Ordering::Less)
        }

        #[test]
        fn same_key() {
            let first_mvcc_key = MVCCKey::new("a", Timestamp::new(10, 12));
            let second_mvcc_key = MVCCKey::new("a", Timestamp::new(12, 12));
            let ordering = Storage::compare(
                &encode_mvcc_key(&first_mvcc_key),
                &encode_mvcc_key(&second_mvcc_key),
            );
            assert_eq!(ordering, Ordering::Greater)
        }

        #[test]
        fn intent_key() {
            let intent_key = MVCCKey::create_intent_key_with_str("a");
            let second_mvcc_key = MVCCKey::new("a", Timestamp::new(12, 12));
            let ordering = Storage::compare(
                &encode_mvcc_key(&intent_key),
                &encode_mvcc_key(&second_mvcc_key),
            );
            assert_eq!(ordering, Ordering::Less);

            let ordering = Storage::compare(
                &encode_mvcc_key(&second_mvcc_key),
                &encode_mvcc_key(&intent_key),
            );
            assert_eq!(ordering, Ordering::Greater)
        }

        #[test]
        fn none_mvcc() {
            let first_key = "a";
            let second_key = "b";

            let ordering = Storage::compare(&str_to_key(first_key), &str_to_key(second_key));
            assert_eq!(ordering, Ordering::Less)
        }
    }
}
