use crate::concurrency::concurrency_manager::ConcurrencyManager;

use super::request::{Command, Request};

struct Executor {
    concr_manager: ConcurrencyManager,
}

impl Executor {
    pub fn execute_request_with_concurrency(&self, request: Request) {
        let guard = self.concr_manager.sequence_req();
        // TODO: Collect spans
        if request.is_read_only() {
            self.execute_read_only_request(&request);
        } else {
            self.execute_read_write_request(&request);
        }
        self.execute_read_write_request(&request);
        self.concr_manager.release_guard(guard);
    }

    pub fn execute_read_write_request(&self, request: &Request) {}

    pub fn execute_read_only_request(&self, request: &Request) {}
}
