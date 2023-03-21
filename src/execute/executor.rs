use std::{borrow::BorrowMut, sync::Arc};

use crate::{
    concurrency::concurrency_manager::ConcurrencyManager,
    storage::{mvcc::KVStore, mvcc_key::MVCCKey},
};

use super::request::{Command, Request, RequestMetadata, RequestUnion, SpansToAcquire};

struct Executor {
    concr_manager: ConcurrencyManager,
    writer: Arc<KVStore>,
}

impl Executor {
    pub fn execute_request_with_concurrency_retries(&self, request: Request) {
        let request_union = &request.request_union;
        let spans = request_union.collect_spans();
        let spans_to_acquire = SpansToAcquire {
            latch_spans: spans.clone(),
            lock_spans: spans.clone(),
        };
        let guard = self.concr_manager.sequence_req(spans_to_acquire);
        // // TODO: Collect spans
        if request_union.is_read_only() {
            self.execute_read_only_request(&request);
        } else {
            self.execute_read_write_request(&request);
        }
        self.finish_request();
    }

    pub fn finish_request(&self) {}

    pub fn execute_read_write_request(&self, request: &Request) -> () {
        // TODO: applyTimestampCache - we need to make sure we bump the
        // txn.writeTimestsamp before we lay any intents
        // request
        //     .request_union
        //     .execute(&request.metadata, self.writer);
        todo!()
    }

    pub fn execute_read_only_request(&self, request: &Request) -> () {
        // request
        //     .request_union
        //     .execute(&request.metadata, &mut self.writer);
        todo!()
    }
}
