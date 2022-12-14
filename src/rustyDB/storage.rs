pub mod storage;

mod tests {
    use rocksdb::DB;

    use crate::rustyDB::storage::storage::Storage;

    #[test]
    fn run() {
        let path = "test_temp_db";
        let mut storage = Storage::new(path);
        let column_fam = "col_fam";
        let key1 = "my key 1";
        storage.create_column_family(column_fam);
        storage
            .put(column_fam, key1, b"my value 1")
            .map_err(|e| println!("Error: {}", e.message));
        storage.put(column_fam, "my key 2", b"my value 2");
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
        println!("foo");
    }
}
