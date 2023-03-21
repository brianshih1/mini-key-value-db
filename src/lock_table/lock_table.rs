pub struct LockTableGuard {}

pub struct LockTable {}

impl LockTable {
    pub fn new() -> Self {
        todo!()
    }

    pub fn scan_and_enqueue(&self) -> LockTableGuard {
        todo!()
    }

    /**
     * This method is called after latches are dropped. It wait until
     * when the request is at the front of all wait-queues and it's safe to
     * re-acquire latches and rescan the lockTable.
     *
     * It's also responsible for pushing transaction if it times out
     */
    pub fn wait_for(&self, guard: LockTableGuard) {}
}

impl LockTableGuard {
    pub fn should_wait(&self) -> bool {
        todo!()
    }
}
