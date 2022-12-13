use rocksdb::{Error, Options, DB};

pub struct Storage {
    db: rocksdb::DB,
}

impl Storage {
    fn create_column_family(&mut self, cf_name: &str) -> Result<(), Error> {
        let options = rocksdb::Options::default();
        self.db.create_cf(cf_name, &options)
    }
}
