use std::{
    collections::HashMap,
    sync::{Arc, RwLock},
};

use serde::de::DeserializeOwned;
use uuid::Uuid;

use crate::{
    execute::{
        executor::Executor,
        request::{GetRequest, GetResponse, Request, RequestMetadata, RequestUnion, ResponseUnion},
    },
    hlc::timestamp::Timestamp as HLCTimestamp,
    storage::{str_to_key, txn::Txn},
};

pub type TxnLink = Arc<RwLock<Txn>>;

pub struct DB {
    executor: Executor,
    current_time: RwLock<Timestamp>,
    txns: RwLock<HashMap<Uuid, TxnLink>>,
}

#[derive(Debug, Clone, Copy)]
pub struct Timestamp {
    pub value: u64,
}

impl Timestamp {
    pub fn advance_by(&self, step: u64) -> Timestamp {
        Timestamp {
            value: self.value + step,
        }
    }

    pub fn to_hlc_timestamp(&self) -> HLCTimestamp {
        HLCTimestamp::new(self.value, self.value.try_into().unwrap())
    }
}

impl DB {
    pub fn set_time(&self, timestamp: Timestamp) {
        *self.current_time.write().unwrap() = timestamp;
    }

    pub fn now(&self) -> Timestamp {
        *self.current_time.read().unwrap()
    }

    pub async fn write<T: DeserializeOwned>(&self, key: &str, value: T, txn_id: Uuid) {}

    // TODO: Result
    pub async fn read<T: DeserializeOwned>(&self, key: &str, txn_id: Uuid) -> T {
        let request_union = RequestUnion::Get(GetRequest {
            key: str_to_key(key),
        });
        let timestamp = self.now().to_hlc_timestamp();
        let request_metadata = RequestMetadata {
            txn: Txn::new_link(txn_id, timestamp, timestamp),
        };
        let read_request = Request {
            metadata: request_metadata,
            request_union,
        };
        let response = self
            .executor
            .execute_request_with_concurrency_retries(read_request)
            .await;
        let (key, value) = match response {
            ResponseUnion::Get(r) => r.value,
            _ => unreachable!(),
        };

        serde_json::from_slice::<T>(&value).unwrap()
    }

    pub async fn read_without_txn<T: DeserializeOwned>(
        &self,
        key: &str,
        timestamp: Timestamp,
    ) -> T {
        todo!()
    }

    pub async fn begin_txn(&self, timestamp: Timestamp) {}

    pub async fn abort_txn(&self) {}

    pub async fn commit_txn(&self) {}
}
