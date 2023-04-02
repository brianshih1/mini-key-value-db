use std::sync::RwLock;

use uuid::Uuid;

use crate::{
    concurrency::concurrency_manager::{ConcurrencyManager, Guard},
    db::db::{TxnLink, TxnMap},
    hlc::timestamp::Timestamp,
    storage::mvcc::KVStore,
    timestamp_oracle::oracle::TimestampOracle,
};

use super::request::{
    Command, ExecuteError, ExecuteResult, Request, ResponseUnion, SpansToAcquire,
};

pub struct Executor {
    pub concr_manager: ConcurrencyManager,
    pub writer: KVStore,
    pub timestamp_oracle: RwLock<TimestampOracle>,
}

impl Executor {
    // path example: "./tmp/data";
    pub fn new(path: &str, txns: TxnMap) -> Self {
        Executor {
            concr_manager: ConcurrencyManager::new(txns),
            writer: KVStore::new(path),
            timestamp_oracle: RwLock::new(TimestampOracle::new()),
        }
    }

    pub async fn execute_request_with_concurrency_retries(
        &self,
        request: Request,
    ) -> ResponseUnion {
        // TODO: This should return some form of Result<...>
        loop {
            let request_union = &request.request_union;
            let spans = request_union.collect_spans();
            let spans_to_acquire = SpansToAcquire {
                latch_spans: spans.clone(),
                lock_spans: spans.clone(),
            };
            let guard = self.concr_manager.sequence_req(&request).await;

            let result = if request_union.is_read_only() {
                self.execute_read_only_request(&request, &guard).await
            } else {
                self.execute_write_request(&request, &guard).await
            };

            match result {
                Ok(result) => {
                    // release latches and dequeue request from lockTable
                    self.concr_manager.finish_req(guard).await;
                    return result;
                }
                Err(err) => {
                    match err {
                        ExecuteError::WriteIntentError(_) => {
                            // TODO: When do we call finish_req in this case?
                            self.handle_write_intent_error();
                        }
                    }
                }
            };
        }
    }

    pub fn handle_write_intent_error(&self) {}

    pub async fn execute_write_request(&self, request: &Request, guard: &Guard) -> ExecuteResult {
        // TODO: applyTimestampCache - we need to make sure we bump the
        // txn.writeTimestamp before we lay any intents
        let spans = request.request_union.collect_spans();
        let timestamp_oracle = self.timestamp_oracle.read().unwrap();
        let timestamps = spans
            .iter()
            .map(|s| {
                let res =
                    timestamp_oracle.get_max_timestamp(s.start_key.clone(), s.end_key.clone());
                res.and_then(|(timestamp, _)| Some(timestamp))
            })
            .filter_map(|t| t)
            .collect::<Vec<Timestamp>>();
        let max_timestamp_option = timestamps.iter().max();
        if let Some(max_timestamp) = max_timestamp_option {
            // bump the txn
            let txn_arc = request.metadata.txn.clone();
            let mut txn = txn_arc.write().unwrap();
            txn.write_timestamp = *max_timestamp;
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
    ) -> ExecuteResult {
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
            self.writer
                .mvcc_resolve_intent(key.clone(), write_timestamp, txn.txn_id);
        }
    }
}
