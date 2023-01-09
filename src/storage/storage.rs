use std::path::Path;

use rocksdb::DB;
use serde::{de::DeserializeOwned, Deserialize, Serialize};

use crate::{StorageError, StorageResult};

use super::{
    mvcc_iterator::{IterOptions, MVCCIterator},
    mvcc_key::{encode_mvcc_key, MVCCKey},
};

pub struct Storage {
    pub db: DB,
}

impl Storage {
    // path example: "./tmp/data";
    pub fn new(path: &str) -> Storage {
        let mut options = rocksdb::Options::default();
        options.create_if_missing(true);
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
        let mut options = rocksdb::Options::default();
        options.create_if_missing(true);
        let db = DB::open(&options, path).unwrap();
        Storage { db }
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

    pub fn put_mvcc_serialized<T: Serialize>(
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

    pub fn get_mvcc_serialized<T: DeserializeOwned>(&mut self, key: &MVCCKey) -> StorageResult<T> {
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
        storage.put_mvcc_serialized(mvcc_key.to_owned(), 12);
        let retrieved = storage.get_mvcc_serialized::<i32>(&mvcc_key).unwrap();
        assert_eq!(retrieved, 12);
    }
}
