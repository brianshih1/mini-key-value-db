use std::{
    collections::HashMap,
    sync::{Arc, Mutex, RwLock},
};

use tokio::{
    sync::mpsc::{channel, Receiver, Sender},
    time::{self, Duration},
};

use crate::{
    execute::request::{Command, Request},
    latch_manager::latch_interval_btree::Range,
    storage::{
        txn::{Txn, TxnIntent, TxnMetadata},
        Key,
    },
};

pub struct LockTableGuard {
    // whether the request needs to wait for the guard since it's
    // queued to a conflicting lock
    pub should_wait: bool,

    pub txn: RwLock<Txn>,

    pub sender: Arc<Sender<()>>,

    pub receiver: Mutex<Receiver<()>>,
}

pub struct LockTable {
    // TODO: Use a concurent btree instead of a hash map
    pub locks: RwLock<HashMap<Key, Arc<RwLock<LockState>>>>,
}

pub type LockTableGuardLink = Arc<RwLock<LockTableGuard>>;

/**
 * Invariants:
 * - Reservation and lock_holder cannot both exist at the same time (both not None)
 */
pub struct LockState {
    // TODO: Holder
    pub reservation: Option<LockTableGuardLink>,

    pub lock_holder: Option<TxnMetadata>,

    pub queued_writers: RwLock<Vec<LockTableGuardLink>>,

    pub waiting_readers: RwLock<Vec<LockTableGuardLink>>,
}

impl LockTable {
    pub fn new() -> Self {
        LockTable {
            locks: RwLock::new(HashMap::new()),
        }
    }

    /**
     * Add a discovered lock to lockTable and add the guard to the waitingQueue
     */
    pub fn add_discovered_lock(&self, guard: LockTableGuardLink, intent: TxnIntent) {
        let locks = self.locks.write().unwrap();
        // if locks.contains_key(k) {}
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
    pub fn scan_and_enqueue<'a>(
        &'a self,
        request: &Request<'a>,
        guard: Option<LockTableGuardLink>,
    ) -> (bool, LockTableGuardLink) {
        let spans = request.request_union.collect_spans();
        let is_read_only = request.request_union.is_read_only();
        let txn = request.metadata.txn;

        let lock_guard = match guard {
            Some(guard) => guard,
            None => {
                let (tx, rx) = channel::<()>(1);
                Arc::new(RwLock::new(LockTableGuard {
                    should_wait: false,
                    txn: RwLock::new(txn.clone()),
                    sender: Arc::new(tx),
                    receiver: Mutex::new(rx),
                }))
            }
        };
        for span in spans.iter() {
            let locks = self.locks.read().unwrap();
            let lock = locks.get(&span.start_key);
            if let Some(lock_state) = lock {
                let write_lock_state = lock_state.write().unwrap();
                let should_wait =
                    write_lock_state.try_active_wait(lock_guard.clone(), is_read_only);
                if should_wait {
                    lock_guard.as_ref().write().unwrap().should_wait = true;
                    return (true, lock_guard);
                }
            }
        }
        lock_guard.as_ref().write().unwrap().should_wait = false;
        (false, lock_guard)
    }

    /**
     * This method is called after latches are dropped. It wait until
     * when the request is at the front of all wait-queues and it's safe to
     * re-acquire latches and rescan the lockTable.
     *
     * It's also responsible for pushing transaction if it times out
     */
    pub async fn wait_for(&self, guard: LockTableGuardLink) {
        let lg = guard.as_ref().read().unwrap();
        let mut rx = lg.receiver.lock().unwrap();

        let sleep = time::sleep(Duration::from_millis(1000));
        tokio::pin!(sleep);
        tokio::select! {
            Some(_) = rx.recv() => {
                println!("finished waiting for lock!");
                return;
            }
            _ = &mut sleep, if !sleep.is_elapsed() => {
                println!("operation timed out");
                // TODO: PushTransactions
            }

        };
    }

    /**
     * Dequeue removes the request from its lock wait-queues. It needs
     * to be called when the request is finished, whether it was evaluated
     * or not. The method does not release any locks.
     */
    pub fn dequeue(&self) {}

    /**
     * This function tells the lockTable that an existing lock was updated/released (e.g. the transaction
     * has committed / aborted).
     *
     * This function sets the lock_holder as null and calls is_lock_free to free the readers / next writer or start
     * garbage collection.
     */
    pub fn update_locks(&self) {}
}

impl<'a> LockState {
    /**
     * Returns whether a lock can be freed - which is when there are no more queued_writers.
     * It releases all waiting_readers since readers don't need to wait anymore.
     * If there are no queued writers, return true. Otherwise, the first queued writer gets the
     * reservation.
     *
     * This function should be called whenever the holder is set to null (i.e. the holder is finalized - committed/aborted)
     * or the reservation is set to null (dequeud - doneRequest). In other words, the next waiter's turn is up.
     */
    pub fn is_lock_free() -> bool {
        todo!()
    }

    /**
     * try_active_wait returns whether a request's span needs to wait for a lock.
     * If yes, queue the request to the lock's queued_writers (or read_waiters if it's a read)
     */
    pub fn try_active_wait<'b>(&self, guard: LockTableGuardLink, is_read_only: bool) -> bool {
        let guard_ref = guard.as_ref();
        let mut lg = guard_ref.write().unwrap();
        let lg_txn = lg.txn.read().unwrap();
        if let Some(ref holder) = self.lock_holder {
            // the request already holds the lock
            if holder.txn_id == lg_txn.txn_id {
                return false;
            }
        }
        // TODO: Check if the lockHolderTxn is finalized - release the lock by calling lockIsFree
        // when might this happen?

        // A read running into an uncommitted intent with a higher timestamp ignores
        // the intent and does not need to wait.
        if is_read_only {
            match self.lock_holder {
                Some(ref holder) => {
                    if lg_txn.read_timestamp < holder.write_timestamp {
                        return false;
                    }
                    self.waiting_readers.write().unwrap().push(guard.clone());
                }
                None => {
                    // Reads only wait for lock holder, not reservation
                    return false;
                }
            }
        } else {
            // write request

            // TODO: Do we need to handle the case where the guard is already in the queue?
            // In our MVP how might that even happen?
            self.queued_writers.write().unwrap().push(guard.clone());
        }
        drop(lg_txn);
        lg.should_wait = true;
        true
    }
}

impl LockTableGuard {
    /**
     * should_wait should be called after scan_and_enqueue - if true, it should release latches
     * and wait until the lock is reserved by the transcation or it timed out
     */
    pub fn should_wait(&self) -> bool {
        self.should_wait
    }

    pub fn done_waiting_at_lock(&self) {}
}

mod test {
    mod lock_table {
        mod scan_and_enqueue {}

        mod wait_for {}

        mod dequeue {}

        mod update_locks {}
    }

    mod lock_state {
        mod is_lock_free {}

        mod try_active_wait {}
    }

    mod lock_table_guard {
        mod should_wait {}

        mod done_waiting_at_lock {}
    }
}
