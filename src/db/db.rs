use std::{
    collections::HashMap,
    future::Future,
    sync::{Arc, RwLock},
};

use serde::{de::DeserializeOwned, Serialize};
use std::panic;
use tokio::sync::mpsc;
use uuid::Uuid;

use crate::{
    execute::{
        executor::{ExecuteError, Executor},
        request::{
            AbortTxnRequest, BeginTxnRequest, CommitTxnRequest, GetRequest, PutRequest, Request,
            RequestMetadata, RequestUnion, ResponseUnion,
        },
    },
    hlc::{
        clock::{Clock, ManualClock},
        timestamp::Timestamp as HLCTimestamp,
    },
    storage::{str_to_key, txn::Txn},
};

use super::request_queue::{TaskQueue, TaskQueueRequest};

pub type TxnLink = Arc<RwLock<Txn>>;

pub type TxnMap = Arc<RwLock<HashMap<Uuid, TxnLink>>>;

pub struct InternalDB {
    executor: Arc<Executor>,
    txns: TxnMap,
    clock: RwLock<ManualClock>,
    thread_pool: TaskQueue,
}

pub struct DB {
    db: Arc<InternalDB>,
}

#[derive(Debug, Clone, Copy)]
pub struct Timestamp {
    pub value: u64,
}

pub enum CommitTxnResult {
    Success(CommitTxnSuccess),
    Fail(CommitTxnFailureReason),
}

pub struct CommitTxnSuccess {
    pub commit_timestamp: HLCTimestamp,
}

pub enum CommitTxnFailureReason {
    ReadRefreshFail,
    TxnAborted,
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
    pub fn new_cleaned(path: &str, initial_time: Timestamp) -> Self {
        DB {
            db: Arc::new(InternalDB::new_cleaned(path, initial_time)),
        }
    }

    pub fn new(path: &str, initial_time: Timestamp) -> Self {
        DB {
            db: Arc::new(InternalDB::new(path, initial_time)),
        }
    }

    pub fn set_time(&self, timestamp: Timestamp) {
        self.db.set_time(timestamp);
    }

    // TODO: This should return an error - deadlock detection
    pub async fn write<T: Serialize>(
        &self,
        key: &str,
        value: T,
        txn_id: Uuid,
    ) -> Result<ResponseUnion, ExecuteError> {
        self.db.write(key, value, txn_id).await
    }

    pub async fn read<T: DeserializeOwned>(&self, key: &str, txn_id: Uuid) -> Option<T> {
        self.db.read(key, txn_id).await
    }

    pub async fn read_without_txn<T: DeserializeOwned>(
        &self,
        key: &str,
        timestamp: Timestamp,
    ) -> T {
        self.db.read_without_txn(key, timestamp).await
    }

    /**
     * Creates a transaction. All reads and writes with the TxnContext will be using
     * the created txn.
     *
     * An uncaught panic thrown from the transactional code will rollback the transaction
     * to the start of the transaction scope.
     *
     * If the transaction function finishes executing, the transaction will commit
     */
    pub async fn run_txn<'a, Fut>(&self, f: impl FnOnce(Arc<TxnContext>) -> Fut + std::marker::Copy)
    where
        Fut: Future<Output = ()>,
    {
        loop {
            let txn_id = self.db.begin_txn().await;
            let context = Arc::new(TxnContext {
                txn_id,
                db: self.db.clone(),
            });
            let cloned = context.clone();

            // TODO: What if this panics? We should abort if so
            f(cloned).await;
            let res = self.db.commit_txn(txn_id).await;
            match res {
                CommitTxnResult::Success(_) => break,
                CommitTxnResult::Fail(_) => {
                    // TODO: Should we abort before continuing?
                    continue;
                }
            }
        }
    }

    pub async fn begin_txn(&self) -> Uuid {
        self.db.begin_txn().await
    }

    pub async fn abort_txn(&self, txn_id: Uuid) {
        self.db.abort_txn(txn_id).await
    }

    // TODO: We should return the final timestamps if possible - easier for testing
    // Returns whether the commit was successful or if a retry was necessary
    pub async fn commit_txn(&self, txn_id: Uuid) -> CommitTxnResult {
        self.db.commit_txn(txn_id).await
    }
}

impl InternalDB {
    // TODO: Should we have a new_cleaned and keep a new?
    // path example: "./tmp/data";
    pub fn new_cleaned(path: &str, initial_time: Timestamp) -> Self {
        let txns = Arc::new(RwLock::new(HashMap::new()));
        if initial_time.value == 0 {
            panic!("DB time cannot start from 0 as it is reserved for intents")
        }
        let (sender, receiver) = mpsc::channel::<TaskQueueRequest>(1);
        let sender = Arc::new(sender);
        let executor = Arc::new(Executor::new_cleaned(path, txns.clone(), sender.clone()));
        let txns = Arc::new(RwLock::new(HashMap::new()));
        let thread_pool = TaskQueue::new(receiver, executor.clone(), txns.clone(), sender.clone());
        InternalDB {
            executor,
            txns,
            clock: RwLock::new(Clock::manual(initial_time.value)),
            thread_pool,
        }
    }

    pub fn new(path: &str, initial_time: Timestamp) -> Self {
        let txns = Arc::new(RwLock::new(HashMap::new()));
        if initial_time.value == 0 {
            panic!("DB time cannot start from 0 as it is reserved for intents")
        }
        let (sender, receiver) = mpsc::channel::<TaskQueueRequest>(1);
        let sender = Arc::new(sender);

        let executor = Arc::new(Executor::new(path, txns.clone(), sender.clone()));
        let txns = Arc::new(RwLock::new(HashMap::new()));
        let thread_pool = TaskQueue::new(receiver, executor.clone(), txns.clone(), sender.clone());
        InternalDB {
            executor,
            txns,
            clock: RwLock::new(Clock::manual(initial_time.value)),
            thread_pool,
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
    pub async fn write<T: Serialize>(
        &self,
        key: &str,
        value: T,
        txn_id: Uuid,
    ) -> Result<ResponseUnion, ExecuteError> {
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
        self.executor
            .execute_request_with_concurrency_retries(write_request)
            .await
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
                ExecuteError::ReadRefreshFailure => unreachable!(),
                ExecuteError::TxnCommitted => todo!(),
                ExecuteError::TxnAborted => todo!(),
            },
        }
    }

    pub async fn read_without_txn<T: DeserializeOwned>(
        &self,
        _key: &str,
        _timestamp: Timestamp,
    ) -> T {
        todo!()
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
        self.executor
            .execute_request_with_concurrency_retries(request)
            .await
            .unwrap();

        txn_id
    }

    pub async fn abort_txn(&self, txn_id: Uuid) {
        let txn_request = RequestUnion::AbortTxn(AbortTxnRequest {});
        let txn = self.get_txn(txn_id);
        let request_metadata = RequestMetadata { txn };

        let request = Request {
            metadata: request_metadata,
            request_union: txn_request,
        };
        let _response = self
            .executor
            .execute_request_with_concurrency_retries(request)
            .await;
    }

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
            Err(err) => match err {
                ExecuteError::ReadRefreshFailure => {
                    CommitTxnResult::Fail(CommitTxnFailureReason::ReadRefreshFail)
                }
                ExecuteError::TxnCommitted => todo!(),
                ExecuteError::TxnAborted => {
                    CommitTxnResult::Fail(CommitTxnFailureReason::TxnAborted)
                }
            },
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

impl Drop for InternalDB {
    fn drop(&mut self) {}
}

pub struct TxnContext {
    txn_id: Uuid,
    db: Arc<InternalDB>,
}

impl TxnContext {
    pub async fn write<T: Serialize>(
        &self,
        key: &str,
        value: T,
    ) -> Result<ResponseUnion, ExecuteError> {
        self.db.write(key, value, self.txn_id).await
    }

    pub async fn read<T: DeserializeOwned>(&self, key: &str) -> Option<T> {
        self.db.read(key, self.txn_id).await
    }
}
