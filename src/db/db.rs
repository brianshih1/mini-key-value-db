use serde::de::DeserializeOwned;
use uuid::Uuid;

pub struct DB {}

pub struct Timestamp {}

impl DB {
    pub fn set_time(&self, timestamp: Timestamp) {}

    pub fn advance_time_by(&self) {}

    pub fn now(&self) {}

    pub fn write<T: DeserializeOwned>(&self, key: &str, value: T, txn_id: Uuid) {}

    pub fn read<T: DeserializeOwned>(&self, key: &str, txn_id: Uuid) -> T {
        todo!()
    }

    pub fn read_without_txn<T: DeserializeOwned>(&self, key: &str, timestamp: Timestamp) -> T {
        todo!()
    }

    pub fn begin_txn(&self, timestamp: Timestamp) {}

    pub fn abort_txn(&self) {}

    pub fn commit_txn(&self) {}
}
