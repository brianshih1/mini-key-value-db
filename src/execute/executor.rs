use crate::{
    concurrency::concurrency_manager::ConcurrencyManager,
    storage::{mvcc::KVStore, mvcc_key::MVCCKey},
};

use super::request::{Command, RequestUnion, SpansToAcquire};

struct Executor {
    concr_manager: ConcurrencyManager,
    writer: KVStore,
}

impl Executor {
    pub fn execute_request_with_concurrency(&self, request: RequestUnion) {
        let spans = request.collect_spans();
        let spans_to_acquire = SpansToAcquire {
            latch_spans: spans.clone(),
            lock_spans: spans.clone(),
        };
        let guard = self.concr_manager.sequence_req(spans_to_acquire, "lol");
        // // TODO: Collect spans
        if request.is_read_only() {
            self.execute_read_only_request(&request);
        } else {
            self.execute_read_write_request(&request);
        }
        self.concr_manager.release_guard(guard);
    }

    pub fn execute_read_write_request(&self, request: &RequestUnion) {}

    pub fn execute_read_only_request(&self, request: &RequestUnion) {}
}
