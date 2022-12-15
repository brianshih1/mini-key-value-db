use std::path::Path;

use crate::RustyError;
use rocksdb::{ColumnFamily, DBIterator, Error, Options, DB};
use serde::{de::DeserializeOwned, Serialize};
use serde_json::{json, Value};

pub struct Storage {
    db: rocksdb::DB,
}

pub type RustyResult<T> = Result<T, RustyError>;

impl Storage {
    pub fn new(path: &str) -> Storage {
        let mut options = Options::default();
        options.create_if_missing(true);
        let cfs = rocksdb::DB::list_cf(&options, path).unwrap_or(vec![]);
        let db = DB::open_cf(&options, path, cfs).map_err(|e| println!("Failed to open: {}", e));
        return Storage { db: db.unwrap() };
    }

    pub fn clean_db(&mut self, path: &str) {
        if Path::new(path).exists() {
            std::fs::remove_dir_all(path).unwrap();
        }
    }

    pub fn create_column_family(&mut self, cf_name: &str) -> Result<(), Error> {
        let options = rocksdb::Options::default();
        self.db.create_cf(cf_name, &options)
    }

    fn get_column_family(&self, cf_name: &str) -> RustyResult<&ColumnFamily> {
        self.db
            .cf_handle(&cf_name)
            .ok_or(RustyError::new("".to_owned()))
    }

    pub fn put<V>(&mut self, cf_name: &str, key: &str, value: V) -> Result<(), RustyError>
    where
        V: AsRef<[u8]>,
    {
        self.get_column_family(cf_name).and_then(|cf| {
            self.db
                .put_cf(cf, key, value)
                .map_err(|e| RustyError::new("".to_owned()))
        })
    }

    pub fn getValue(&self, cf_name: &str, key: &str) -> RustyResult<Option<Vec<u8>>> {
        let value = self.get_column_family(cf_name).and_then(|cf| {
            self.db
                .get_cf(cf, key)
                .map_err(|e| RustyError::new("".to_owned()))
        });
        value
    }

    pub fn get_preseek_iterator(
        &mut self,
        cf_name: &str,
        prefix_name: &str,
    ) -> Result<DBIterator, RustyError> {
        let cf_handle = self.db.cf_handle(cf_name);
        match cf_handle {
            Some(cf) => Ok(self.db.prefix_iterator_cf(cf, prefix_name)),
            None => Err(RustyError::new("no cfHandle found for prefix".to_owned())),
        }
    }

    pub fn find_doc_kvs(
        &mut self,
        cf_name: &str,
        prefix_name: &str,
    ) -> RustyResult<Vec<(String, Box<[u8]>)>> {
        let mut resultVec = Vec::new();
        let mut iterator = self.get_preseek_iterator(cf_name, prefix_name);

        match iterator {
            Ok(mut it) => loop {
                let next = it.next();
                match next {
                    Some(res) => {
                        if let Ok((k, v)) = res {
                            let key = String::from_utf8(k.to_vec()).unwrap();
                            let does_prefix_match = &key == prefix_name;
                            resultVec.push((key, v));
                            if does_prefix_match {
                                return Ok(resultVec);
                            }
                        }
                    }
                    None => break,
                }
            },
            _ => {}
        };
        return Ok(resultVec);
    }
    /**
     * If we store
     * {
     *   SubKey1: {
     *      SubKey2: Value1,
     *      SubKey3: Value2
     *   }
     * }
     *
     * Then this gets stored as four KV pairs:
     *
     * DocKey, T10 -> {} // This is an init marker
     * DocKey, SubKey1, T10 -> {}
     * DocKey, SubKey2, T10 -> Value1
     * DocKey, SubKey2, T10 -> Value2
     */

    // TODO: Also do a put_serialized
    pub fn put_value(&mut self, key: Vec<&str>, value: Value) -> RustyResult<()> {}

    /**
     * e.g. flatten_value("document1", ["foo", "bar", "baz"], {...})
     */
    fn flatten_value(
        &mut self,
        docKey: &str,
        path: Vec<&str>,
        value: Value,
    ) -> RustyResult<Vec<(&str, Value)>> {
        if path.len() == 0 {
            return [(docKey, value)];
        }
    }
}

mod tests {
    use std::path::Path;

    use rocksdb::DB;
    use serde_json::Value;

    use crate::rustyDB::storage::storage::Storage;

    struct Person {
        name: String,
        age: u8,
    }

    #[test]
    fn test_preseek_iteration() {
        let path = "test_temp_db";

        let mut storage = Storage::new(path);
        let column_fam = "col_fam";
        let key1 = "my key 1";
        storage.clean_db(path);
        storage.create_column_family(column_fam).unwrap();
        storage.put(column_fam, key1, b"my value 1").unwrap();
        storage.put(column_fam, "my key 2", b"my value 2").unwrap();
        storage.put(column_fam, "foo", b"bar").unwrap();
        storage.put(column_fam, "my", b"my value 2").unwrap();
        storage.put(column_fam, "my key 3", b"my value 2").unwrap();

        match storage.getValue(column_fam, key1) {
            Ok(Some(value)) => println!("retrieved value {}", String::from_utf8(value).unwrap()),
            _ => {
                println!("nothing found...")
            }
        }

        let prefix = "my";
        // let mut iterator = storage.get_preseek_iterator(column_fam, prefix).unwrap();

        let storage_kvs = storage.find_doc_kvs(column_fam, prefix);
        // let firstRecord = iterator.next().unwrap();
        // firstRecord.map(|(key, value)| {
        //     println!("key: {}", String::from_utf8(key.to_vec()).unwrap());
        //     println!("value: {}", String::from_utf8(value.to_vec()).unwrap());
        // });
        // db.put(b"my key 2", b"my value 2").unwrap();
        // db.put(b"my key 3", b"my value 2").unwrap();
        // storage.get_preseek_iterator(cf_name, prefix_name);
        if let Ok(kvs) = storage_kvs {
            let iter = kvs.iter();

            for (k, v) in iter {
                println!("key: {}", k);
                println!("value: {}", String::from_utf8(v.to_vec()).unwrap());
            }
        }
    }

    #[test]
    fn test_random() {
        let data = r#"
        {
            "name": "John Doe",
            "age": 43,
            "phones": [
                "+44 1234567",
                "+44 2345678"
            ]
        }"#;

        // Parse the string of data into serde_json::Value.
        let v: Value = serde_json::from_str(data).unwrap();
        let person = Person {
            name: "foo".to_owned(),
            age: 1,
        };

        if v.is_object() {
            for (name, value) in v.as_object().unwrap().iter() {}
        }
    }
}
