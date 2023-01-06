use rocksdb::DB;
use serde::{de::DeserializeOwned, Deserialize, Serialize};

use crate::{DBResult, StorageError};

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

    pub fn put_raw(&mut self, key: &str, value: Vec<u8>) -> DBResult<()> {
        self.db.put(key, value).map_err(|e| StorageError::from(e))
    }

    pub fn put_serialized<T: Serialize>(&mut self, key: &str, value: T) -> DBResult<()> {
        let str_res = serde_json::to_string(&value);
        match str_res {
            Ok(serialized) => self.put_raw(&key, serialized.into_bytes()),
            Err(err) => Err(StorageError::from(err.to_string())),
        }
    }

    pub fn get_serialized<T: DeserializeOwned>(&mut self, key: &str) -> DBResult<T> {
        let res = self.db.get(key);
        match res {
            Ok(optional) => match optional {
                Some(value) => Ok(serde_json::from_slice::<T>(&value).unwrap()),
                None => Err(StorageError {
                    message: "not found".to_owned(),
                }),
            },
            Err(err) => Err(StorageError::from(err.to_string())),
        }
    }
}

#[cfg(test)]
mod Test {
    use serde::{Deserialize, Serialize};

    use super::Storage;

    #[derive(Debug, Serialize, Deserialize)]
    struct Test {
        foo: bool,
    }

    #[test]
    fn put_and_get() {
        let mut storage = Storage::new("./tmp/foo");
        let test = Test { foo: false };
        storage.put_serialized("foo", &test).unwrap();
        let retrieved = storage.get_serialized::<Test>("foo").unwrap();
        println!("value: {:?}", retrieved);
    }
}
