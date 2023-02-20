use std::fmt::{self, Display};

use serde::Serialize;

use crate::hlc::timestamp::Timestamp;

pub mod mvcc;
pub mod mvcc_iterator;
pub mod mvcc_key;
pub mod mvcc_scanner;
pub mod storage;
pub mod txn;

pub type Key = Vec<u8>;

pub type Value = Vec<u8>;

pub fn serialized_to_value<T: Serialize>(value: T) -> Value {
    serde_json::to_string(&value).unwrap().into_bytes()
}

pub fn str_to_key(str: &str) -> Key {
    str.as_bytes().to_vec()
}

pub fn boxed_byte_to_byte_vec(value: &Box<[u8]>) -> Key {
    Vec::from(value.as_ref())
}

pub struct ValueWithTimestamp {
    raw_value: Value,
    timestamp: Timestamp,
}
