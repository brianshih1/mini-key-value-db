use std::{
    borrow::{Borrow, BorrowMut},
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

    /**
     * Whether the request associated to the guard is a read or write request
     */
    pub is_read_only: bool,

    pub wait_state: RwLock<WaitingState>,
}

pub enum WaitingState {
    /**
     * the request is waiting for another transaction to release its lock
     * or complete its own request.
     */
    Waiting,
    /**
     * the request is done waiting on this pass through the lockTable
     * and should make another call to scan_and_enqueue
     */
    DoneWaiting,
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
     * Add a discovered lock to lockTable. Add the guard to the queued_writers
     * or waiting_readers depending on if the request is_read_only.
     *
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
    pub fn scan_and_enqueue<'a>(&'a self, request: &Request) -> (bool, LockTableGuardLink) {
        let spans = request.request_union.collect_spans();
        let is_read_only = request.request_union.is_read_only();
        let txn = request.metadata.txn;

        let lock_guard = LockTableGuard::new_lock_table_guard_link(txn.clone(), is_read_only);
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
     *
     * Returns whether the lock can be garbage collected.
     */
    pub async fn update_locks(&self, key: Key, txn: Txn) -> bool {
        let locks = self.locks.read().unwrap();
        let lock_state_option = locks.get(&key);
        if lock_state_option.is_none() {
            return false;
        }
        let lock_state_link = lock_state_option.unwrap();
        let lock_state = lock_state_link.as_ref().write().unwrap();

        let reservation = lock_state.reservation.borrow();
        let mut holder_option = lock_state.lock_holder.borrow_mut();

        if reservation.is_none() && holder_option.is_none() {
            return true;
        }
        if let Some(holder) = &*holder_option {
            if holder.txn_id != txn.txn_id {
                return false;
            }
        }
        *holder_option = None;
        drop(holder_option);
        lock_state.lock_is_free().await
    }
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
     * The lock has transitioned from locked/reserved to unlocked. Ther could be waiters,
     * but there cannot be a reservation.
     *
     * All readers are freed and if there are queued writers, the first writer gets the reservation.
     *
     * Returns whether a lock can be freed - which is when there are no more queued_writers.
     *
     * This function should be called whenever the holder is set to null (i.e. the holder is finalized - committed/aborted)
     * or the reservation is set to null (dequeud - doneRequest). In other words, the next waiter's turn is up.
     */
    pub async fn lock_is_free(&self) -> bool {
        let holder = self.lock_holder.borrow();
        if holder.is_some() {
            panic!("called lock_is_free with holder");
        }

        let reservation = self.reservation.borrow();
        if reservation.is_some() {
            panic!("called lock_is_free with reservation");
        }

        let readers = self.waiting_readers.read().unwrap();
        for reader_arc in readers.iter() {
            let reader = reader_arc.as_ref().read().unwrap();
            reader.done_waiting_at_lock().await;
        }

        let mut writers = self.queued_writers.write().unwrap();
        if writers.len() == 0 {
            return true;
        }

        let first = writers.remove(0);
        let first_writer = first.as_ref().read().unwrap();
        first_writer.done_waiting_at_lock().await;

        return false;
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
                    lg.update_wait_state(WaitingState::Waiting);
                    self.waiting_readers.write().unwrap().push(guard.clone());
                }
                None => {
                    // Reads only wait for lock holder, not reservation
                    return false;
                }
            }
        } else {
            // write request

            lg.update_wait_state(WaitingState::Waiting);
            // TODO: Do we need to handle the case where the guard is already in the queue?
            // In our MVP how might that even happen?
            self.queued_writers.write().unwrap().push(guard.clone());
        }
        drop(lg_txn);
        lg.should_wait = true;
        true
    }

    // An uncommitted intent (lock_state) was discovered by a lock_table_guard
    // Push the guard onto the queued_writers
    pub fn discovered_lock(&self, guard: LockTableGuardLink) {
        let mut holder = self.lock_holder.borrow_mut();
        if holder.is_none() {
            let txn_meta = LockTableGuard::get_txn_meta(&guard);
            *holder = Some(txn_meta)
        }
        let mut reservation = self.reservation.borrow_mut();
        if reservation.is_some() {
            *reservation = None
        }

        let is_read_only = guard.as_ref().read().unwrap().is_read_only;
        let lg = guard.read().unwrap();
        lg.update_wait_state(WaitingState::Waiting);
        if is_read_only {
            let mut waiting_readers = self.waiting_readers.write().unwrap();
            waiting_readers.push(guard.clone());
        } else {
            let mut queued_writers = self.queued_writers.write().unwrap();
            queued_writers.push(guard.clone());
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
    pub fn new(txn: Txn, is_read_only: bool) -> Self {
        let (tx, rx) = channel::<()>(1);
        LockTableGuard {
            should_wait: false,
            txn: RwLock::new(txn.clone()),
            sender: Arc::new(tx),
            receiver: Mutex::new(rx),
            guard_id: Uuid::new_v4(),
            is_read_only,
            wait_state: RwLock::new(WaitingState::DoneWaiting),
        }
    }

    pub fn new_lock_table_guard_link(txn: Txn, is_read_only: bool) -> LockTableGuardLink {
        Arc::new(RwLock::new(LockTableGuard::new(txn, is_read_only)))
    }

    /**
     * should_wait should be called after scan_and_enqueue - if true, it should release latches
     * and wait until the lock is reserved by the transcation or it timed out
     */
    pub fn should_wait(&self) -> bool {
        self.should_wait
    }

    pub fn update_wait_state(&self, waiting_state: WaitingState) {
        let mut wait_state = self.wait_state.write().unwrap();
        *wait_state = waiting_state;
    }

    /**
     * Called when the request is no longer waiting at lock and should find the
     * next lock to wait at.
     */
    pub async fn done_waiting_at_lock(&self) {
        let sender = self.sender.as_ref();
        sender.send(()).await.unwrap();
        self.update_wait_state(WaitingState::DoneWaiting);
    }

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
        execute::request::{GetRequest, PutRequest, Request, RequestMetadata, RequestUnion},
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

    pub fn create_test_txn() -> Txn {
        Txn::new(Uuid::new_v4(), Timestamp::new(1, 1), Timestamp::new(1, 1))
    }

    pub fn create_test_lock_table_guard(is_read_only: bool) -> (Uuid, Txn, LockTableGuardLink) {
        let txn_id = Uuid::new_v4();
        let txn = Txn::new(txn_id, Timestamp::new(1, 1), Timestamp::new(1, 1));
        let lg = LockTableGuard::new_lock_table_guard_link(txn.clone(), is_read_only);
        (txn_id, txn, lg)
    }

    pub fn create_test_lock_table_guard_with_timestamp(
        timestamp: Timestamp,
        is_read_only: bool,
    ) -> (Uuid, Txn, LockTableGuardLink) {
        let txn_id = Uuid::new_v4();
        let txn = Txn::new(txn_id, timestamp, timestamp);
        let lg = LockTableGuard::new_lock_table_guard_link(txn.clone(), is_read_only);
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

    pub fn create_test_read_request(key: &str, timestamp: Timestamp) -> (Request, Txn) {
        let request_union = RequestUnion::Get(GetRequest {
            key: str_to_key(key),
        });
        let txn_id = Uuid::new_v4();
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
                        get_guard_id, TestLockState,
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
                let key = str_to_key("foo");

                let (txn_id, txn, lg) = create_test_lock_table_guard(false);
                lock_table.add_discovered_lock(lg.clone(), txn.to_intent(key.clone()));
                let test_lock_state = TestLockState {
                    queued_writers: Vec::from([get_guard_id(lg)]),
                    waiting_readers: Vec::from([]),
                    lock_holder: Some(txn_id),
                    reservation: None,
                };
                assert_lock_state(&lock_table, key, test_lock_state);
            }

            #[test]
            fn two_guards_add_same_key() {
                let lock_table = LockTable::new();

                let (txn_1_id, _, lg_1) = create_test_lock_table_guard(true);

                let key = str_to_key("foo");
                lock_table.add_discovered_lock(
                    lg_1.clone(),
                    TxnIntent::new(txn_1_id, Timestamp::new(0, 0), key.clone()),
                );

                let test_lock_state = TestLockState {
                    queued_writers: Vec::from([]),
                    waiting_readers: Vec::from([get_guard_id(lg_1.clone())]),
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
                    let (should_wait, _) = lock_table.scan_and_enqueue(&request);
                    assert!(!should_wait);
                    let lock_state_option = lock_table.get_lock_state(&key);
                    assert!(lock_state_option.is_none());
                }

                #[test]
                fn queue_write_request_to_held_lock() {
                    let key_str = "foo";
                    let lock_table = LockTable::new();

                    // add discovered lock
                    let (txn_id, txn, lg) = create_test_lock_table_guard(false);
                    lock_table.add_discovered_lock(lg.clone(), txn.to_intent(str_to_key(key_str)));

                    // enqueue a WRITE request onto the discovered lock
                    let (request, _) = create_test_put_request(key_str);
                    let (should_wait, guard) = lock_table.scan_and_enqueue(&request);
                    assert!(should_wait);

                    let test_lock_state = TestLockState {
                        queued_writers: Vec::from([
                            get_guard_id(lg.clone()),
                            get_guard_id(guard.clone()),
                        ]),
                        waiting_readers: Vec::from([]),
                        lock_holder: Some(txn_id),
                        reservation: None,
                    };
                    assert_lock_state(&lock_table, str_to_key(key_str), test_lock_state);

                    // enqueue another WRITE request to the locked state
                    let (request, _) = create_test_put_request(key_str);
                    let (should_wait_2, guard_2) = lock_table.scan_and_enqueue(&request);
                    assert!(should_wait_2);

                    let test_lock_state_2 = TestLockState {
                        queued_writers: Vec::from([
                            get_guard_id(lg.clone()),
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
                            assert_lock_state, create_test_lock_table_guard,
                            create_test_lock_table_guard_with_timestamp, create_test_read_request,
                            get_guard_id, TestLockState,
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
                        create_test_lock_table_guard_with_timestamp(lock_timestamp, true);
                    lock_table.add_discovered_lock(lg.clone(), txn.to_intent(str_to_key(key_str)));

                    // enqueue a READ request onto the discovered lock
                    let (read_request, _) =
                        create_test_read_request(key_str, lock_timestamp.advance_by(1));
                    let (should_wait, read_lg) = lock_table.scan_and_enqueue(&read_request);
                    assert!(should_wait);

                    let test_lock_state = TestLockState {
                        queued_writers: Vec::from([]),
                        waiting_readers: Vec::from([get_guard_id(lg), get_guard_id(read_lg)]),
                        lock_holder: Some(txn_id),
                        reservation: None,
                    };
                    assert_lock_state(&lock_table, str_to_key(key_str), test_lock_state);
                }

                #[test]
                fn read_request_with_smaller_timestamp_than_lock_holder() {
                    let key_str = "foo";
                    let lock_table = LockTable::new();

                    // add discovered lock
                    let lock_timestamp = Timestamp::new(2, 2);
                    let (txn_id, txn, lg) =
                        create_test_lock_table_guard_with_timestamp(lock_timestamp, false);
                    lock_table.add_discovered_lock(lg.clone(), txn.to_intent(str_to_key(key_str)));

                    let (read_request, _) =
                        create_test_read_request(key_str, lock_timestamp.decrement_by(1));
                    let (should_wait, _) = lock_table.scan_and_enqueue(&read_request);
                    assert!(!should_wait);

                    let test_lock_state = TestLockState {
                        queued_writers: Vec::from([get_guard_id(lg)]),
                        waiting_readers: Vec::from([]),
                        lock_holder: Some(txn_id),
                        reservation: None,
                    };
                    assert_lock_state(&lock_table, str_to_key(key_str), test_lock_state);
                }
            }
        }

        mod wait_for {}

        mod dequeue {}

        mod update_locks {
            use crate::{
                lock_table::lock_table::{test::create_test_lock_table_guard, LockTable},
                storage::str_to_key,
            };

            #[tokio::test]
            async fn no_queued_writer() {
                let key_str = "foo";
                let lock_table = LockTable::new();
                let (txn_id, txn, lg) = create_test_lock_table_guard(false);
                lock_table.add_discovered_lock(lg, txn.to_intent(str_to_key(key_str)));

                let can_gc_lock = lock_table.update_locks(str_to_key(key_str), txn).await;

                assert!(!can_gc_lock);
            }

            #[tokio::test]
            async fn queued_writer_is_freed() {}
        }
    }

    mod lock_state {
        mod lock_is_free {}

        mod try_active_wait {}
    }

    mod lock_table_guard {
        mod should_wait {}

        mod done_waiting_at_lock {}
    }
}
