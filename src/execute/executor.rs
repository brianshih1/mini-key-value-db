use std::sync::{Arc, RwLock};

use tokio::sync::mpsc::Sender;
use uuid::Uuid;

use crate::{
    concurrency::concurrency_manager::{ConcurrencyManager, Guard},
    db::{
        db::{TxnLink, TxnMap},
        request_queue::TaskQueueRequest,
    },
    hlc::timestamp::Timestamp,
    storage::{
        mvcc::{KVStore, MVCCGetParams},
        mvcc_key::create_intent_key,
        txn::{TransactionStatus, TxnIntent},
    },
    timestamp_oracle::oracle::TimestampOracle,
};

use super::request::{Command, Request, ResponseError, ResponseResult, ResponseUnion};

pub type ExecuteResult = Result<ResponseUnion, ExecuteError>;

#[derive(Debug)]
pub enum ExecuteError {
    ReadRefreshFailure,
    TxnCommitted,
    // Failed to execute because its transaction is already aborted
    TxnAborted,
}

pub struct Executor {
    pub concr_manager: ConcurrencyManager,
    pub store: Arc<KVStore>,
    pub timestamp_oracle: RwLock<TimestampOracle>,
}

impl Executor {
    // path example: "./tmp/data";
    pub fn new_cleaned(
        path: &str,
        txns: TxnMap,
        request_sender: Arc<Sender<TaskQueueRequest>>,
    ) -> Self {
        let store = Arc::new(KVStore::new_cleaned(path));
        Executor {
            concr_manager: ConcurrencyManager::new(txns, request_sender, Arc::clone(&store)),
            store: store,
            timestamp_oracle: RwLock::new(TimestampOracle::new()),
        }
    }

    pub fn new(path: &str, txns: TxnMap, request_sender: Arc<Sender<TaskQueueRequest>>) -> Self {
        let store = Arc::new(KVStore::new(path));
        Executor {
            concr_manager: ConcurrencyManager::new(txns, request_sender, store.clone()),
            store,
            timestamp_oracle: RwLock::new(TimestampOracle::new()),
        }
    }

    pub async fn execute_request_with_concurrency_retries(
        &self,
        request: Request,
    ) -> ExecuteResult {
        // TODO: This should return some form of Result<...>
        loop {
            let request_union = &request.request_union;
            let guard = self.concr_manager.sequence_req(&request).await;

            if let Err(err) = &guard {
                match err {
                    TxnAborted => return ExecuteResult::Err(ExecuteError::TxnAborted),
                    TxnCommitted => return ExecuteResult::Err(ExecuteError::TxnCommitted),
                }
            }

            let guard = guard.unwrap();
            let result = if request_union.is_read_only() {
                self.execute_read_only_request(&request, &guard).await
            } else {
                self.execute_write_request(&request, &guard).await
            };
            self.concr_manager.finish_req(guard).await;
            match result {
                Ok(result) => {
                    // release latches and dequeue request from lockTable

                    return Ok(result);
                }
                Err(err) => match err {
                    ResponseError::WriteIntentError(err) => {
                        self.handle_write_intent_error(err.intent.clone());
                        println!("Handle write intent error");
                    }
                    ResponseError::ReadRefreshError => {
                        return Err(ExecuteError::ReadRefreshFailure);
                    }
                    ResponseError::TxnAbortedError => {
                        return Err(ExecuteError::TxnAborted);
                    }
                },
            };
        }
    }

    pub fn handle_write_intent_error(&self, txn_intent: TxnIntent) {
        // check the txn record for the status of transaction.
        let record = self
            .store
            .get_transaction_record(txn_intent.txn_meta.txn_id);
        if let Some(txn_record) = record {
            if let TransactionStatus::ABORTED = txn_record.status {
                self.store.mvcc_delete(create_intent_key(&txn_intent.key));
            }
        }
        // If the txn has aborted, remove the txn record
    }

    pub async fn execute_write_request(&self, request: &Request, guard: &Guard) -> ResponseResult {
        // finds the max read timestamp from timestamp oracle for the spans
        // and bump the write timestamp if necessary
        let spans = request
            .request_union
            .collect_spans(request.metadata.txn.clone());
        let timestamps = spans
            .iter()
            .map(|s| {
                let res = self
                    .timestamp_oracle
                    .read()
                    .unwrap()
                    .get_max_timestamp(s.start_key.clone(), s.end_key.clone());
                res.and_then(|(timestamp, _)| Some(timestamp))
            })
            .filter_map(|t| t)
            .collect::<Vec<Timestamp>>();
        let max_timestamp_option = timestamps.iter().max();
        if let Some(max_timestamp) = max_timestamp_option {
            // bump the txn
            let txn_arc = request.metadata.txn.clone();
            let mut txn = txn_arc.write().unwrap();
            if txn.write_timestamp < *max_timestamp {
                txn.write_timestamp = *max_timestamp;
            }
        }
        request
            .request_union
            .execute(&request.metadata, &self)
            .await
    }

    pub async fn execute_read_only_request(
        &self,
        request: &Request,
        guard: &Guard,
    ) -> ResponseResult {
        let result = request
            .request_union
            .execute(&request.metadata, &self)
            .await;

        // TODO: Should we still update the cache if it failed?
        let oracle_guard = self.timestamp_oracle.write().unwrap();
        oracle_guard.update_cache(request);
        result
    }

    pub fn resolve_intents_for_txn(&self, txn: TxnLink) {
        let txn = txn.read().unwrap();
        let write_timestamp = txn.write_timestamp;
        let keys = txn.lock_spans.read().unwrap();

        for key in keys.iter() {
            self.store
                .mvcc_resolve_intent(key.clone(), write_timestamp, txn.txn_id);
        }
    }

    /**
     * Updates the txn's readTimestamp to its writeTimestamp and return true.
     * If it's not possible, return false.
     *
     * Advancing a transaction’s read timestamp from ta to tb is possible if we
     * can prove that none of the record that the transaction has read at ta
     * has been updated to the interval (ta,tb].
     *
     * To prove it, we use MVCCGet and set tb as the higher bound.
     */
    pub fn refresh_read_timestamp(&self, txn_link: TxnLink) -> bool {
        let txn = txn_link.read().unwrap();
        let txn_id = txn.txn_id;
        // TODO: Remove clone
        let read_set = txn.read_set.read().unwrap().clone();
        let to_timestamp = txn.write_timestamp;
        let from_timestamp = txn.read_timestamp;
        drop(txn);
        for key in read_set.iter() {
            let res = self.store.mvcc_get(
                &key,
                to_timestamp,
                MVCCGetParams {
                    transaction: Some(txn_link.clone()),
                },
            );
            if let Some((mvcc_key, _)) = res.value {
                let mvcc_timestamp = mvcc_key.timestamp;
                if mvcc_timestamp < from_timestamp {
                    return false;
                }
            }

            // Check if an intent which is not owned by this transaction was written
            // at or beneath the refresh timestamp.
            if let Some((intent, _)) = res.intent {
                let intent_timestamp = intent.txn_meta.write_timestamp;
                if intent.txn_meta.txn_id != txn_id && intent_timestamp < to_timestamp {
                    return false;
                }
            }
        }
        txn_link.write().unwrap().write_timestamp = to_timestamp;
        true
    }
}
