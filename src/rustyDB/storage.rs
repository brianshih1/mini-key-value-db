pub mod storage;

mod tests {
    use rocksdb::DB;

    use crate::rustyDB::storage::storage::Storage;

    #[test]
    fn run() {
        let path = "test_temp_db";
        let mut storage = Storage::new(path);
        let column_fam = "col_fam";
        storage.put(column_fam, "my key 2", b"my value 2");
        storage.put(column_fam, "my key 2", b"my value 2");
        // db.put(b"my key 2", b"my value 2").unwrap();
        // db.put(b"my key 3", b"my value 2").unwrap();
        // storage.get_preseek_iterator(cf_name, prefix_name);
        println!("foo");
    }
}
