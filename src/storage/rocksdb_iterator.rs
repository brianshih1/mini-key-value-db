use rocksdb::DBIterator;

use super::{
    mvcc_key::MVCCKey,
    engine::{IterOptions, MVCCIterator},
};

pub fn new_rocksdb_iterator<'a>(iter_options: IterOptions, db: rocksdb::DB) -> RocksDBIterator<'a> {
    // RocksDBIterator { db: db, it: () }
    todo!()
}

// A wrapper around rocksdb iterator
pub struct RocksDBIterator<'a> {
    pub db: rocksdb::DB,
    pub it: DBIterator<'a>,
}

impl<'a> MVCCIterator for RocksDBIterator<'a> {
    fn seek_ge(&mut self, key: MVCCKey) -> () {
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

    fn valid(&mut self) -> bool {
        todo!()
    }

    fn next(&mut self) -> () {
        todo!()
    }
}
