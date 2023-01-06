use rocksdb::DBIterator;

use super::{mvcc_key::MVCCKey, Value};

struct IterOptions {}

pub fn new_mvcc_iterator<'a>(iter_options: IterOptions, db: rocksdb::DB) -> MVCCIterator<'a> {
    // RocksDBIterator { db: db, it: () }
    todo!()
}

// A wrapper around rocksdb iterator
pub struct MVCCIterator<'a> {
    pub db: rocksdb::DB,
    pub it: DBIterator<'a>,
}

impl<'a> MVCCIterator<'a> {
    fn next() -> () {}

    fn current_key() -> MVCCKey {
        todo!()
    }

    fn current_value() -> Value {
        todo!()
    }

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
}
