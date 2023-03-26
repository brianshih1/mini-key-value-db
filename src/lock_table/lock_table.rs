use std::{
    cell::RefCell,
    collections::HashMap,
    sync::{Arc, Mutex, RwLock},
};

use tokio::{
    sync::mpsc::{channel, Receiver, Sender},
    time::{self, Duration},
};
use uuid::Uuid;

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

    // TODO: Does this Txn need to be in RwLock? Are we going to mutate it somewhere?
    // (i.e. bump the readTimestmap)
    pub txn: RwLock<Txn>,

    pub sender: Arc<Sender<()>>,

    pub receiver: Mutex<Receiver<()>>,

    pub guard_id: Uuid,
}

pub struct LockTable {
    // TODO: Use a concurent btree instead of a hash map
    pub locks: RwLock<HashMap<Key, LockStateLink>>,
}

pub type LockStateLink = Arc<RwLock<LockState>>;

pub type LockTableGuardLink = Arc<RwLock<LockTableGuard>>;

/**
 * Invariants:
 * - Reservation and lock_holder cannot both exist at the same time (both not None)
 */
pub struct LockState {
    // TODO: Holder
    pub reservation: RefCell<Option<LockTableGuardLink>>,

    pub lock_holder: RefCell<Option<TxnMetadata>>,

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
        let mut locks = self.locks.write().unwrap();
        let lock_state = if locks.contains_key(&intent.key) {
            Arc::clone(locks.get(&intent.key).unwrap())
        } else {
            Arc::new(RwLock::new(LockState::new()))
        };
        locks.insert(intent.key, lock_state.clone());
        let state = lock_state.as_ref().write().unwrap();
        state.discovered_lock(guard.clone())
    }

    #[cfg(test)]
    pub fn get_lock_state(&self, key: &Key) -> Option<LockStateLink> {
        let locks = self.locks.read().unwrap();
        locks.get(key).and_then(|v| Some(v.clone()))
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
        request: &Request,
        guard: Option<LockTableGuardLink>,
    ) -> (bool, LockTableGuardLink) {
        let spans = request.request_union.collect_spans();
        let is_read_only = request.request_union.is_read_only();
        let txn = request.metadata.txn;

        let lock_guard = match guard {
            Some(guard) => guard,
            None => LockTableGuard::new_lock_table_guard_link(txn.clone()),
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
    pub fn new() -> Self {
        LockState {
            reservation: RefCell::new(None),
            lock_holder: RefCell::new(None),
            queued_writers: RwLock::new(Vec::new()),
            waiting_readers: RwLock::new(Vec::new()),
        }
    }
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

    pub fn register_guard(&self, guard: LockTableGuardLink) {
        let lg = guard.as_ref().read().unwrap();
        let txn = lg.txn.read().unwrap();
        let mut lock_holder = self.lock_holder.borrow_mut();
        if lock_holder.is_none() {
            *lock_holder = Some(txn.metadata)
        }

        if self.reservation.borrow().is_some() {
            self.queued_writers.write().unwrap().push(guard.clone());
        }
    }

    /**
     * try_active_wait returns whether a request's span needs to wait for a lock.
     * If yes, queue the request to the lock's queued_writers (or read_waiters if it's a read).
     *
     * Note that this method does not actually wait.
     */
    pub fn try_active_wait<'b>(&self, guard: LockTableGuardLink, is_read_only: bool) -> bool {
        let guard_ref = guard.as_ref();
        let mut lg = guard_ref.write().unwrap();
        let lg_txn = lg.txn.read().unwrap();
        let lock_holder = self.lock_holder.borrow();
        if let Some(ref holder) = *lock_holder {
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
            match &*lock_holder {
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

    // An uncommitted intent (lock_state) was discovered by a lock_table_guard
    pub fn discovered_lock(&self, guard: LockTableGuardLink) {
        let mut holder = self.lock_holder.borrow_mut();
        if holder.is_none() {
            let txn_meta = LockTableGuard::get_txn_meta(&guard);
            *holder = Some(txn_meta)
        } else {
            let mut queued_writers = self.queued_writers.write().unwrap();
            queued_writers.push(guard.clone())
        }
        let mut reservation = self.reservation.borrow_mut();
        if reservation.is_some() {
            *reservation = None
        }
    }

    #[cfg(test)]
    pub fn get_queued_writer_ids(&self) -> Vec<Uuid> {
        let writers = self.queued_writers.read().unwrap();
        let ids = writers
            .iter()
            .map(|x| {
                let lg = x.as_ref().read().unwrap();
                lg.guard_id
            })
            .collect::<Vec<Uuid>>();
        ids
    }

    #[cfg(test)]
    pub fn get_waiting_readers_ids(&self) -> Vec<Uuid> {
        let writers = self.waiting_readers.read().unwrap();
        let ids = writers
            .iter()
            .map(|x| {
                let lg = x.as_ref().read().unwrap();
                lg.guard_id
            })
            .collect::<Vec<Uuid>>();
        ids
    }

    #[cfg(test)]
    pub fn get_holder_txn_id(&self) -> Option<Uuid> {
        let holder = self.lock_holder.borrow();
        holder.and_then(|txn_meta| Some(txn_meta.txn_id))
    }
}

impl LockTableGuard {
    pub fn new(txn: Txn) -> Self {
        let (tx, rx) = channel::<()>(1);
        LockTableGuard {
            should_wait: false,
            txn: RwLock::new(txn.clone()),
            sender: Arc::new(tx),
            receiver: Mutex::new(rx),
            guard_id: Uuid::new_v4(),
        }
    }

    pub fn new_lock_table_guard_link(txn: Txn) -> LockTableGuardLink {
        Arc::new(RwLock::new(LockTableGuard::new(txn)))
    }

    /**
     * should_wait should be called after scan_and_enqueue - if true, it should release latches
     * and wait until the lock is reserved by the transcation or it timed out
     */
    pub fn should_wait(&self) -> bool {
        self.should_wait
    }

    pub fn done_waiting_at_lock(&self) {}

    pub fn get_txn_meta(guard_link: &LockTableGuardLink) -> TxnMetadata {
        let read_guard = guard_link.as_ref().read().unwrap();
        let txn_guard = read_guard.txn.read().unwrap();
        txn_guard.metadata.clone()
    }

    #[cfg(test)]
    pub fn get_guard_id(guard: LockTableGuardLink) -> Uuid {
        let g = guard.as_ref().read().unwrap();
        g.guard_id
    }

    #[cfg(test)]
    pub fn get_txn_id(guard: LockTableGuardLink) -> Uuid {
        let g = guard.as_ref().read().unwrap();
        let txn_id = g.txn.read().unwrap().txn_id;
        txn_id
    }
}

#[cfg(test)]
mod test {
    use core::time;

    use uuid::Uuid;

    use crate::{
        execute::request::{PutRequest, Request, RequestMetadata, RequestUnion},
        hlc::timestamp::Timestamp,
        storage::{serialized_to_value, str_to_key, txn::Txn, Key},
    };

    use super::{LockStateLink, LockTable, LockTableGuard, LockTableGuardLink};

    pub fn assert_holder_txn_id(lock_state_link: LockStateLink, txn_id: Uuid) {
        let lock_state = lock_state_link.as_ref().read().unwrap();
        let holder = lock_state.lock_holder.borrow();
        assert!(holder.is_some());
        assert_eq!(lock_state.get_holder_txn_id(), Some(txn_id));
    }

    pub fn get_guard_id(guard_link: LockTableGuardLink) -> Uuid {
        guard_link.as_ref().read().unwrap().guard_id
    }

    // Test struture for LockState to assert what is being held by a LockState
    pub struct TestLockState {
        pub queued_writers: Vec<Uuid>,  // guard ids
        pub waiting_readers: Vec<Uuid>, // guard ids
        pub lock_holder: Option<Uuid>,  // txn_id
        pub reservation: Option<Uuid>,  // guard_id
    }

    #[cfg(test)]
    pub fn assert_lock_state(lock_table: &LockTable, key: Key, test_lock_state: TestLockState) {
        let lock_state_arc = lock_table.get_lock_state(&key).unwrap();
        let lock_state = lock_state_arc.as_ref().read().unwrap();
        assert_eq!(
            lock_state.get_queued_writer_ids(),
            test_lock_state.queued_writers
        );
        assert_eq!(
            lock_state.get_waiting_readers_ids(),
            test_lock_state.waiting_readers
        );
        match test_lock_state.lock_holder {
            Some(txn_id) => {
                let holder = &lock_state.lock_holder.borrow().unwrap();
                assert_eq!(txn_id, holder.txn_id)
            }
            None => {
                assert!(lock_state.lock_holder.borrow().is_none())
            }
        }
        match test_lock_state.reservation {
            Some(guard_id) => {
                let reservation_arc = &lock_state.reservation.borrow().clone().unwrap();
                let reservation = reservation_arc.as_ref().read().unwrap();
                assert_eq!(guard_id, reservation.guard_id)
            }
            None => {
                assert!(lock_state.reservation.borrow().is_none())
            }
        }
    }

    pub fn create_test_lock_table_guard() -> (Uuid, Txn, LockTableGuardLink) {
        let txn_id = Uuid::new_v4();
        let txn = Txn::new(txn_id, Timestamp::new(1, 1), Timestamp::new(1, 1));
        let lg = LockTableGuard::new_lock_table_guard_link(txn.clone());
        (txn_id, txn, lg)
    }

    pub fn create_test_lock_table_guard_with_timestamp(
        timestamp: Timestamp,
    ) -> (Uuid, Txn, LockTableGuardLink) {
        let txn_id = Uuid::new_v4();
        let txn = Txn::new(txn_id, timestamp, timestamp);
        let lg = LockTableGuard::new_lock_table_guard_link(txn.clone());
        (txn_id, txn, lg)
    }

    pub fn create_test_put_request(key: &str) -> (Request, Txn) {
        let request_union = RequestUnion::Put(PutRequest {
            key: str_to_key(key),
            value: serialized_to_value(2),
        });
        let txn_id = Uuid::new_v4();
        let timestamp = Timestamp::new(1, 2);
        let txn = Txn::new(txn_id, timestamp, timestamp);
        (
            Request {
                metadata: RequestMetadata {
                    timestamp: timestamp,
                    txn: txn.clone(),
                },
                request_union,
            },
            txn,
        )
    }

    mod lock_table {
        mod add_discovered_lock {
            use uuid::Uuid;

            use crate::{
                hlc::timestamp::Timestamp,
                lock_table::lock_table::{
                    test::{
                        assert_holder_txn_id, assert_lock_state, create_test_lock_table_guard,
                        TestLockState,
                    },
                    LockTable, LockTableGuard,
                },
                storage::{
                    str_to_key,
                    txn::{Txn, TxnIntent, TxnMetadata},
                },
            };

            #[test]
            fn empty_lock_table() {
                let lock_table = LockTable::new();
                let (txn_id, _, lg) = create_test_lock_table_guard();
                let key = str_to_key("foo");
                lock_table.add_discovered_lock(
                    lg,
                    TxnIntent::new(txn_id, Timestamp::new(1, 1), key.clone()),
                );
                let test_lock_state = TestLockState {
                    queued_writers: Vec::from([]),
                    waiting_readers: Vec::from([]),
                    lock_holder: Some(txn_id),
                    reservation: None,
                };
                assert_lock_state(&lock_table, key, test_lock_state);
            }

            #[test]
            fn two_guards_add_same_key() {
                let lock_table = LockTable::new();

                let (txn_1_id, _, lg_1) = create_test_lock_table_guard();

                let key = str_to_key("foo");
                lock_table.add_discovered_lock(
                    lg_1,
                    TxnIntent::new(txn_1_id, Timestamp::new(0, 0), key.clone()),
                );

                let (txn_2_id, _, lg_2) = create_test_lock_table_guard();

                lock_table.add_discovered_lock(
                    lg_2.clone(),
                    TxnIntent::new(txn_2_id, Timestamp::new(0, 0), key.clone()),
                );

                let test_lock_state = TestLockState {
                    queued_writers: Vec::from([lg_2.as_ref().read().unwrap().guard_id]),
                    waiting_readers: Vec::from([]),
                    lock_holder: Some(txn_1_id),
                    reservation: None,
                };
                assert_lock_state(&lock_table, key, test_lock_state);
            }
        }

        mod scan_and_enqueue {
            mod write_request {
                use uuid::Uuid;

                use crate::{
                    execute::request::{PutRequest, Request, RequestMetadata, RequestUnion},
                    hlc::timestamp::Timestamp,
                    lock_table::lock_table::{
                        test::{
                            assert_lock_state, create_test_lock_table_guard,
                            create_test_put_request, get_guard_id, TestLockState,
                        },
                        LockTable,
                    },
                    storage::{
                        serialized_to_value, str_to_key,
                        txn::{Txn, TxnIntent},
                    },
                };

                #[test]
                fn no_lock_state_for_key() {
                    let key_str = "foo";
                    let key = str_to_key(key_str);
                    let lock_table = LockTable::new();

                    let (request, _) = create_test_put_request(key_str);
                    let (should_wait, _) = lock_table.scan_and_enqueue(&request, None);
                    assert!(!should_wait);
                    let lock_state_option = lock_table.get_lock_state(&key);
                    assert!(lock_state_option.is_none());
                }

                #[test]
                fn queue_write_request_to_held_lock() {
                    let key_str = "foo";
                    let lock_table = LockTable::new();

                    // add discovered lock
                    let (txn_id, txn, lg) = create_test_lock_table_guard();
                    lock_table.add_discovered_lock(lg, txn.to_intent(str_to_key(key_str)));

                    // enqueue a WRITE request onto the discovered lock
                    let (request, _) = create_test_put_request(key_str);
                    let (should_wait, guard) = lock_table.scan_and_enqueue(&request, None);
                    assert!(should_wait);

                    let test_lock_state = TestLockState {
                        queued_writers: Vec::from([get_guard_id(guard.clone())]),
                        waiting_readers: Vec::from([]),
                        lock_holder: Some(txn_id),
                        reservation: None,
                    };
                    assert_lock_state(&lock_table, str_to_key(key_str), test_lock_state);

                    // enqueue another WRITE request to the locked state
                    let (request, _) = create_test_put_request(key_str);
                    let (should_wait_2, guard_2) = lock_table.scan_and_enqueue(&request, None);
                    assert!(should_wait_2);

                    let test_lock_state_2 = TestLockState {
                        queued_writers: Vec::from([
                            get_guard_id(guard.clone()),
                            get_guard_id(guard_2.clone()),
                        ]),
                        waiting_readers: Vec::from([]),
                        lock_holder: Some(txn_id),
                        reservation: None,
                    };
                    assert_lock_state(&lock_table, str_to_key(key_str), test_lock_state_2);
                }
            }
            mod read_request {
                use crate::{
                    hlc::timestamp::Timestamp,
                    lock_table::lock_table::{
                        test::{
                            create_test_lock_table_guard,
                            create_test_lock_table_guard_with_timestamp,
                        },
                        LockTable,
                    },
                    storage::{str_to_key, txn::TxnIntent},
                };

                #[test]
                fn queue_read_request_to_held_lock() {
                    let key_str = "foo";
                    let lock_table = LockTable::new();

                    // add discovered lock
                    let lock_timestamp = Timestamp::new(2, 2);
                    let (txn_id, txn, lg) =
                        create_test_lock_table_guard_with_timestamp(lock_timestamp);
                    lock_table.add_discovered_lock(lg, txn.to_intent(str_to_key(key_str)));

                    // enqueue a READ request onto the discovered lock
                }
            }
        }

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
