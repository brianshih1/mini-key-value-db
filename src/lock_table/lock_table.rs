use std::{collections::HashMap, sync::RwLock};

use crate::{
    execute::request::{Command, Request},
    latch_manager::latch_interval_btree::Range,
    storage::Key,
};

pub struct LockTableGuard {
    // whether the request needs to wait for the guard since it's
    // queued to a conflicting lock
    pub should_wait: bool,
}

pub struct LockTable<'a> {
    pub locks: HashMap<Key, RwLock<LockState<'a>>>,
}

pub struct LockState<'a> {
    // TODO: Holder
    pub reservation: Option<&'a LockTableGuard>,

    pub queued_writers: Vec<&'a LockTableGuard>,

    pub waiting_readers: Vec<&'a LockTableGuard>,
}

impl LockTable<'_> {
    pub fn new() -> Self {
        todo!()
    }

    /**
     * scans the lockTable for conflicting keys and wait if it finds one.
     * Once it finds a lock that it needs to wait at, it adds itself to the queue
     * and terminates early.
     *
     * The concurrency manager detects that it needs to wait with should_wait, releases latches
     * until the request has reserved the lock. Then the manager will re-acquire the latches
     * and call ScanAndEnqueue again to continue finding other conflicts.
     */
    pub fn scan_and_enqueue(
        &self,
        request: &Request<'_>,
        guard: Option<LockTableGuard>,
    ) -> LockTableGuard {
        let spans = request.request_union.collect_spans();
        let is_read_only = request.request_union.is_read_only();

        let lock_guard = match guard {
            Some(guard) => guard,
            None => LockTableGuard { should_wait: false },
        };
        for span in spans.iter() {
            let should_wait = self.try_active_wait(&lock_guard, is_read_only, span);
            if should_wait {
                lock_guard.should_wait = true;
                return lock_guard;
            }
        }
        lock_guard.should_wait = false;
        lock_guard
    }

    /**
     * try_active_wait returns whether a request's span needs to wait for a lock.
     * If yes, queue the request to the lock's queued_writers (or read_waiters if it's a read)
     */
    pub fn try_active_wait(
        &self,
        guard: &LockTableGuard,
        is_read_only: bool,
        span: &Range<Vec<u8>>,
    ) -> bool {
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

    /**
     * Dequeue removes the request from its lock wait-queues. It needs
     * to be called when the request is finished, whether it was evaluated
     * or not. The method does not release any locks.
     */
    pub fn dequeue(&self) {}
}

impl LockState<'_> {
    /**
     * Returns whether a lock can be freed - which is when there are no more queued_writers.
     * It releases all waiting_readers since readers don't need to wait anymore.
     * If there are no queued writers, return true. Otherwise, the first queued writer gets the
     * reservation.
     */
    pub fn is_lock_free() -> bool {
        todo!()
    }
}

impl LockTableGuard {
    /**
     * should_wait should be called after scan_and_enqueue - if true, it should release latches
     * and wait until the lock is reserved by the transcation or it timed out
     */
    pub fn should_wait(&self) -> bool {
        todo!()
    }
}
