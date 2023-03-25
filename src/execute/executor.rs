use std::sync::RwLock;

use crate::{
    concurrency::concurrency_manager::{ConcurrencyManager, Guard},
    storage::mvcc::KVStore,
    timestamp_oracle::oracle::TimestampOracle,
};

use super::request::{Command, ExecuteError, ExecuteResult, Request, SpansToAcquire};

struct Executor {
    concr_manager: ConcurrencyManager,
    writer: KVStore,
    timestamp_oracle: RwLock<TimestampOracle>,
}

impl Executor {
    pub async fn execute_request_with_concurrency_retries(&self, request: Request) {
        loop {
            let request_union = &request.request_union;
            let spans = request_union.collect_spans();
            let spans_to_acquire = SpansToAcquire {
                latch_spans: spans.clone(),
                lock_spans: spans.clone(),
            };
            let guard = self.concr_manager.sequence_req(&request).await;

            let result = if request_union.is_read_only() {
                self.execute_read_only_request(&request, &guard)
            } else {
                self.execute_read_write_request(&request, &guard)
            };

            if let Some(err) = result.error {
                match err {
                    ExecuteError::WriteIntentError(_) => {
                        // TODO: When do we call finish_req in this case?
                        self.handle_write_intent_error();
                    }
                }
            } else {
                // release latches and dequeue request from lockTable
                self.concr_manager.finish_req(guard);
            }
        }
    }

    pub fn handle_write_intent_error(&self) {}

    pub fn execute_read_write_request(&self, request: &Request, guard: &Guard) -> ExecuteResult {
        // TODO: applyTimestampCache - we need to make sure we bump the
        // txn.writeTimestsamp before we lay any intents
        request
            .request_union
            .execute(&request.metadata, &self.writer)
    }

    pub fn execute_read_only_request(&self, request: &Request, guard: &Guard) -> ExecuteResult {
        let result = request
            .request_union
            .execute(&request.metadata, &self.writer);

        // TODO: Should we still update the cache if it failed?
        let oracle_guard = self.timestamp_oracle.write().unwrap();
        oracle_guard.update_cache(request);
        result
    }
}
