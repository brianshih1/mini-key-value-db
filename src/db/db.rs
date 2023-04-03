use std::{
    collections::HashMap,
    future::Future,
    sync::{Arc, RwLock},
};

use serde::{de::DeserializeOwned, Serialize};
use uuid::Uuid;

use crate::{
    execute::{
        executor::{ExecuteError, Executor},
        request::{
            BeginTxnRequest, CommitTxnRequest, GetRequest, PutRequest, Request, RequestMetadata,
            RequestUnion, ResponseUnion,
        },
    },
    hlc::{
        clock::{Clock, ManualClock},
        timestamp::Timestamp as HLCTimestamp,
    },
    storage::{str_to_key, txn::Txn},
};

pub type TxnLink = Arc<RwLock<Txn>>;

pub type TxnMap = Arc<RwLock<HashMap<Uuid, TxnLink>>>;

pub struct DB {
    executor: Executor,
    txns: Arc<RwLock<HashMap<Uuid, TxnLink>>>,
    clock: RwLock<ManualClock>,
}

#[derive(Debug, Clone, Copy)]
pub struct Timestamp {
    pub value: u64,
}

pub enum CommitTxnResult {
    Success(CommitTxnSuccess),
    Fail,
}

pub struct CommitTxnSuccess {
    pub commit_timestamp: HLCTimestamp,
}

impl Timestamp {
    pub fn new(time: u64) -> Self {
        Timestamp { value: time }
    }

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
    // TODO: Should we have a new_cleaned and keep a new?
    // path example: "./tmp/data";
    pub fn new(path: &str, initial_time: Timestamp) -> Self {
        let txns = Arc::new(RwLock::new(HashMap::new()));
        if initial_time.value == 0 {
            panic!("DB time cannot start from 0 as it is reserved for intents")
        }
        DB {
            executor: Executor::new(path, txns.clone()),
            txns: Arc::new(RwLock::new(HashMap::new())),
            clock: RwLock::new(Clock::manual(initial_time.value)),
        }
    }

    pub fn set_time(&self, timestamp: Timestamp) {
        let mut clock = self.clock.write().unwrap();
        clock.receive_timestamp(HLCTimestamp::new(timestamp.value, 0));
    }

    pub fn now_hlc(&self) -> HLCTimestamp {
        let mut clock = self.clock.write().unwrap();
        *clock.get_timestamp()
    }

    pub fn now(&self) -> Timestamp {
        let mut clock = self.clock.write().unwrap();
        let hlc_timestamp = clock.get_timestamp();
        Timestamp {
            value: hlc_timestamp.wall_time,
        }
    }

    // TODO: Return potential error
    pub async fn write<T: Serialize>(&self, key: &str, value: T, txn_id: Uuid) {
        let request_union = RequestUnion::Put(PutRequest {
            key: str_to_key(key),
            value: serde_json::to_string(&value).unwrap().into_bytes(),
        });
        let txn = self.get_txn(txn_id);
        let request_metadata = RequestMetadata { txn };
        let write_request = Request {
            metadata: request_metadata,
            request_union,
        };
        let res = self
            .executor
            .execute_request_with_concurrency_retries(write_request)
            .await;
        res.unwrap();
        // match response {
        //     ResponseUnion::Put(_) => {}
        //     _ => unreachable!(),
        // };
    }

    // TODO: Result
    pub async fn read<T: DeserializeOwned>(&self, key: &str, txn_id: Uuid) -> Option<T> {
        let request_union = RequestUnion::Get(GetRequest {
            key: str_to_key(key),
        });
        let txn = self.get_txn(txn_id);
        let request_metadata = RequestMetadata { txn };
        let read_request = Request {
            metadata: request_metadata,
            request_union,
        };
        let response = self
            .executor
            .execute_request_with_concurrency_retries(read_request)
            .await;

        match response {
            Ok(res) => match res {
                ResponseUnion::Get(get_result) => get_result
                    .value
                    .and_then(|(_, value)| Some(serde_json::from_slice::<T>(&value).unwrap())),
                _ => unreachable!(),
            },
            Err(err) => match err {
                ExecuteError::FailedToCommit => unreachable!(),
            },
        }
    }

    pub async fn read_without_txn<T: DeserializeOwned>(
        &self,
        key: &str,
        timestamp: Timestamp,
    ) -> T {
        todo!()
    }

    pub async fn run_txn<Fut>(&self, f: impl FnOnce(&Self) -> Fut)
    where
        Fut: Future<Output = bool>,
    {
    }

    // pub async fn run_txn<Fut>(&self, f: impl FnOnce(db: &Self) -> Fut () where
    // Fut: Future<Output = bool>) {}

    pub async fn begin_txn(&self) -> Uuid {
        let (txn_id, txn) = self.create_txn_internal();
        let request_metadata = RequestMetadata { txn };
        let txn_request = RequestUnion::BeginTxn(BeginTxnRequest { txn_id });
        let request = Request {
            metadata: request_metadata,
            request_union: txn_request,
        };
        let response = self
            .executor
            .execute_request_with_concurrency_retries(request)
            .await;
        // match response {
        //     ResponseUnion::BeginTransaction(_) => {}
        //     _ => unreachable!(),
        // };
        txn_id
    }

    pub async fn abort_txn(&self) {}

    // TODO: We should return the final timestamps if possible - easier for testing
    // Returns whether the commit was successful or if a retry was necessary
    pub async fn commit_txn(&self, txn_id: Uuid) -> CommitTxnResult {
        let txn = self.get_txn(txn_id);
        let request_metadata = RequestMetadata { txn };
        let txn_request = RequestUnion::CommitTxn(CommitTxnRequest {});
        let request = Request {
            metadata: request_metadata,
            request_union: txn_request,
        };
        let response = self
            .executor
            .execute_request_with_concurrency_retries(request)
            .await;
        match response {
            Ok(res) => match res {
                ResponseUnion::CommitTxn(commit_res) => {
                    CommitTxnResult::Success(CommitTxnSuccess {
                        commit_timestamp: commit_res.commit_timestamp,
                    })
                }
                _ => unreachable!(),
            },
            Err(_) => CommitTxnResult::Fail,
        }
    }

    fn create_txn_internal(&self) -> (Uuid, TxnLink) {
        let txn_id = Uuid::new_v4();
        let txn = Txn::new_link(txn_id, self.now_hlc());
        let mut txns = self.txns.write().unwrap();
        txns.insert(txn_id, txn.clone());
        (txn_id, txn)
    }

    fn get_txn(&self, txn_id: Uuid) -> TxnLink {
        let txns = self.txns.read().unwrap();
        let txn = txns.get(&txn_id);
        match txn {
            Some(txn_link) => txn_link.clone(),
            None => {
                panic!("No txn found for {}", txn_id)
            }
        }
    }
}
