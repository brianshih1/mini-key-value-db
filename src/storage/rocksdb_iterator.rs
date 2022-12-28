use rocksdb::DBIterator;

use super::engine::{IterOptions, MVCCIterator};

pub fn new_rocksdb_iterator<'a>(iter_options: IterOptions, db: rocksdb::DB) -> RocksDBIterator<'a> {
    RocksDBIterator { db: db, it: () }
}

struct RocksDBIterator<'a> {
    pub db: rocksdb::DB,
    pub it: DBIterator<'a>,
}

impl<'a> MVCCIterator for RocksDBIterator<'a> {
    fn seek_ge(&mut self) -> () {
        todo!()
    }

    fn valid(&mut self) -> bool {
        todo!()
    }

    fn next(&mut self) -> () {
        todo!()
    }
}
