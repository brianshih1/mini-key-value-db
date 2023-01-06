use crate::{hlc::timestamp::Timestamp, DBResult};

use super::{
    mvcc::MVCCMetadata,
    mvcc_key::{encode_mvcc_key, MVCCKey},
    Key, Value, ValueWithTimestamp,
};

struct MVCCReaderWriter {
    db: rocksdb::DB,
}

struct MVCCGetOptions {}

// Reader
impl MVCCReaderWriter {
    fn get_mvcc_metadata(&mut self, key: Key) -> DBResult<Option<Value>> {
        todo!()
    }

    /**
     * Gets the most recent value that has a timestamp less than the provided timestamp.
     * If errorOnIntent is true, then a WriteIntentError will be returned.
     */
    fn get_mvcc(
        key: Key,
        timestamp: Timestamp,
        options: MVCCGetOptions,
    ) -> DBResult<Option<ValueWithTimestamp>> {
    }
}

// Writer
impl MVCCReaderWriter {
    fn clear_intent(&mut self) -> () {
        todo!()
    }

    fn put_mvcc(&mut self, key: MVCCKey, value: Value) -> () {
        let encoded_key = encode_mvcc_key(&key);
        self.db.put(encoded_key, value).unwrap();
    }

    fn put_intent(&mut self, key: Key, mvcc_metadata: MVCCMetadata) -> () {
        todo!()
    }

    fn put_engine_key(&mut self) -> () {
        todo!()
    }
}
