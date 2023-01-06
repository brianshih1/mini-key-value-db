use crate::hlc::timestamp::Timestamp;

mod keys;
mod mvcc;
mod mvcc_iterator;
mod mvcc_key;
mod mvcc_scanner;
mod storage;
mod ts_oracle;
mod txn;
mod writer;

pub type Key = Vec<u8>;

pub type Value = Vec<u8>;

pub struct ValueWithTimestamp {
    raw_value: Value,
    timestamp: Timestamp,
}
