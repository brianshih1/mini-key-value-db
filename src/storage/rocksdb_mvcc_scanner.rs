use super::{data::Key, rocksdb_iterator::RocksDBIterator};
use crate::hlc::timestamp::Timestamp;

pub struct RocksMVCCScanner<'a> {
    mvcc_iterator: RocksDBIterator<'a>,

    // TODO: lockTable

    // start of scan (doesn't contain MVCC timestamp)
    pub start: Key,

    // end of the scan (doesn't contain MVCC timestamp)
    pub end: Key,

    // Timestamp that MVCCScan/MVCCGet was called
    pub ts: Timestamp,
    // TODO: Results
}

impl<'a> RocksMVCCScanner<'a> {
    // seeks to the start key and adds one KV to the result set
    // pub fn get(&mut self) {
    //     self.mvcc_iterator.seek_ge(MVCCKey {});
    // }
}
