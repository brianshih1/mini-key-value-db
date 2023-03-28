use std::{
    collections::HashMap,
    sync::{Arc, RwLock},
};

use tokio::{
    sync::{
        mpsc::{channel, Receiver, Sender},
        Mutex,
    },
    time::{self, Duration},
};
use uuid::Uuid;

use crate::{
    execute::request::{Command, Request, SpanSet},
    storage::{
        txn::{Txn, TxnIntent, TxnMetadata},
        Key,
    },
};

pub struct LockTableGuard {
    // whether the request needs to wait for the guard since it's
    // queued to a conflicting lock
    pub should_wait: RwLock<bool>,

    // TODO: Does this Txn need to be in RwLock? Are we going to mutate it somewhere?
    // (i.e. bump the readTimestmap)
    pub txn: RwLock<Txn>,

    pub sender: Arc<Sender<()>>,

    pub receiver: Mutex<Receiver<()>>,

    pub guard_id: Uuid,

    /**
     * Whether the request associated to the guard is a read or write request
     */
    pub is_read_only: RwLock<bool>,

    pub wait_state: RwLock<WaitingState>,

    /**
     * The keys that this request affects
     */
    pub keys: SpanSet<Key>,
}

#[derive(Debug, Eq, PartialEq, Clone, Copy)]
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

pub type LockStateLink = Arc<LockState>;

pub type LockTableGuardLink = Arc<LockTableGuard>;

/**
 * Invariants:
 * - Reservation and lock_holder cannot both exist at the same time (both not None)
 */
pub struct LockState {
    pub reservation: Arc<RwLock<Option<LockTableGuardLink>>>,

    pub lock_holder: Arc<RwLock<Option<TxnMetadata>>>,

    pub queued_writers: Arc<RwLock<Vec<LockTableGuardLink>>>,

    pub waiting_readers: Arc<RwLock<Vec<LockTableGuardLink>>>,
}

impl LockTable {
    pub fn new() -> Self {
        LockTable {
            locks: RwLock::new(HashMap::new()),
        }
    }

    /**
     * An uncommitted intent (lock_state) was discovered by a lock_table_guard
     * Push the guard onto the queued_writers.
     * Add a discovered lock to lockTable. Add the guard to the queued_writers
     * or waiting_readers depending on if the request is_read_only.
     *
     */
    pub async fn add_discovered_lock(&self, guard: LockTableGuardLink, intent: TxnIntent) {
        let mut locks = self.locks.write().unwrap();

        let lock_state = if locks.contains_key(&intent.key) {
            Arc::clone(locks.get(&intent.key).unwrap())
        } else {
            Arc::new(LockState::new())
        };
        locks.insert(intent.key, lock_state.clone());
        let state = lock_state.as_ref();

        let mut holder = state.lock_holder.write().unwrap();
        if holder.is_none() {
            *holder = Some(intent.txn_meta)
        } else {
            // TODO: What should we do if there's already a holder?
        }
        let mut reservation = state.reservation.write().unwrap();
        if reservation.is_some() {
            *reservation = None
        }

        // push the guard onto the queued_writers or waiting_reads
        let is_read_only = *guard.as_ref().is_read_only.read().unwrap();
        let lg = guard.as_ref();
        lg.update_wait_state(WaitingState::Waiting);
        if is_read_only {
            let mut waiting_readers = state.waiting_readers.write().unwrap();
            waiting_readers.push(guard.clone());
        } else {
            let mut queued_writers = state.queued_writers.write().unwrap();
            queued_writers.push(guard.clone());
        }
    }

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
    pub async fn scan_and_enqueue<'a>(&'a self, request: &Request) -> (bool, LockTableGuardLink) {
        let spans = request.request_union.collect_spans();
        let is_read_only = request.request_union.is_read_only();
        let txn = request.metadata.txn;

        let lock_guard =
            LockTableGuard::new_lock_table_guard_link(txn.clone(), is_read_only, spans.clone());
        for span in spans.iter() {
            let locks = self.locks.read().unwrap();
            let lock = locks.get(&span.start_key);
            if let Some(write_lock_state) = lock {
                let should_wait =
                    write_lock_state.try_active_wait(lock_guard.clone(), is_read_only);
                if should_wait {
                    *lock_guard.as_ref().should_wait.write().unwrap() = true;
                    return (true, lock_guard);
                }
            }
        }
        *lock_guard.as_ref().should_wait.write().unwrap() = false;
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
        let lg = guard.as_ref();
        let mut rx = lg.receiver.lock().await;

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
    pub async fn dequeue(&self, lock_table_guard: LockTableGuardLink) {
        let keys = &lock_table_guard.as_ref().keys;
        for key in keys.iter() {
            let state = self.get_lock_state(&key.start_key);
            if let Some(lock_state) = state {
                lock_state.request_done(lock_table_guard.clone()).await
            }
        }
    }

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
        let lock_state_option = self.get_lock_state(&key);

        if lock_state_option.is_none() {
            return false;
        }
        let lock_state_link = lock_state_option.unwrap().clone();

        let lock_state = lock_state_link.as_ref();
        let holder_option = lock_state.get_holder_txn_meta();
        let reservation = lock_state.reservation.read().unwrap().clone();

        if reservation.is_none() && holder_option.is_none() {
            return true;
        }
        if let Some(holder) = holder_option {
            if holder.txn_id != txn.txn_id {
                return false;
            }
        }
        lock_state.update_holder(None);

        lock_state_link.lock_is_free().await
    }
}

impl LockState {
    pub fn new() -> Self {
        LockState {
            reservation: Arc::new(RwLock::new(None)),
            lock_holder: Arc::new(RwLock::new(None)),
            queued_writers: Arc::new(RwLock::new(Vec::new())),
            waiting_readers: Arc::new(RwLock::new(Vec::new())),
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
        {
            let holder = self.lock_holder.read().unwrap();
            if holder.is_some() {
                panic!("called lock_is_free with holder");
            }

            let reservation = self.reservation.read().unwrap();
            if reservation.is_some() {
                panic!("called lock_is_free with reservation");
            }
        }

        let readers = self
            .waiting_readers
            .read()
            .unwrap()
            .iter()
            .map(|r| r.clone())
            .collect::<Vec<Arc<LockTableGuard>>>();

        for reader in readers.iter() {
            reader.done_waiting_at_lock().await;
        }
        self.clear_readers();

        let first_writer = self.remove_first_writer();
        match first_writer {
            Some(writer) => {
                self.update_reservation(Some(writer.clone()));
                writer.done_waiting_at_lock().await;
            }
            None => {
                return true;
            }
        }

        return false;
    }

    pub fn clear_readers(&self) {
        *self.waiting_readers.write().unwrap() = Vec::new();
    }

    pub fn update_reservation(&self, link: Option<LockTableGuardLink>) {
        *self.reservation.write().unwrap() = link;
    }

    // Returns None if the array is empty
    pub fn remove_first_writer(&self) -> Option<LockTableGuardLink> {
        let mut writers = self.queued_writers.write().unwrap();
        if writers.len() == 0 {
            return None;
        }
        Some(writers.remove(0))
    }

    pub fn register_guard(&self, guard: LockTableGuardLink) {
        let lg = guard.as_ref();
        let txn = lg.txn.read().unwrap();
        let mut lock_holder = self.lock_holder.write().unwrap();
        if lock_holder.is_none() {
            *lock_holder = Some(txn.metadata)
        }

        if self.reservation.read().unwrap().is_some() {
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
        let lg = guard.as_ref();

        let lg_txn = lg.txn.read().unwrap();
        let lock_holder = self.lock_holder.read().unwrap();
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
        *lg.should_wait.write().unwrap() = true;
        true
    }

    /**
     * This function is called when a request is done. The request could be a reserver, or inside queued_writers,
     * or waiting_readers. It removes the request from the lock_state
     */
    pub async fn request_done(&self, guard: LockTableGuardLink) {
        let reservation_option = self.get_reservation();
        if let Some(reservation) = reservation_option {
            if reservation.guard_id == guard.guard_id {
                self.update_reservation(None);
                self.lock_is_free().await;
            }
        }
        let mut queued_writers = self.queued_writers.write().unwrap();
        queued_writers.retain(|g| g.guard_id != guard.guard_id);

        let mut waiting_readers = self.waiting_readers.write().unwrap();
        waiting_readers.retain(|g| g.guard_id != guard.guard_id);
    }

    pub fn get_holder_txn_meta(&self) -> Option<TxnMetadata> {
        let holder = self.lock_holder.read().unwrap();
        holder.and_then(|txn_meta| Some(txn_meta))
    }

    pub fn update_holder(&self, new_holder: Option<TxnMetadata>) {
        let mut holder = self.lock_holder.write().unwrap();
        *holder = new_holder;
    }

    pub fn get_reservation(&self) -> Option<LockTableGuardLink> {
        let reservation = self.reservation.read().unwrap();
        reservation.clone()
    }

    #[cfg(test)]
    pub fn get_queued_writer_ids(&self) -> Vec<Uuid> {
        let writers = self.queued_writers.read().unwrap();
        let ids = writers
            .iter()
            .map(|x| x.as_ref().guard_id)
            .collect::<Vec<Uuid>>();
        ids
    }

    #[cfg(test)]
    pub fn get_waiting_readers_ids(&self) -> Vec<Uuid> {
        let writers = self.waiting_readers.read().unwrap();
        let ids = writers
            .iter()
            .map(|x| x.as_ref().guard_id)
            .collect::<Vec<Uuid>>();
        ids
    }

    #[cfg(test)]
    pub fn get_holder_txn_id(&self) -> Option<Uuid> {
        let holder = self.lock_holder.read().unwrap();
        holder.and_then(|txn_meta| Some(txn_meta.txn_id))
    }
}

impl LockTableGuard {
    pub fn new(txn: Txn, is_read_only: bool, keys: SpanSet<Key>) -> Self {
        let (tx, rx) = channel::<()>(1);
        LockTableGuard {
            should_wait: RwLock::new(false),
            txn: RwLock::new(txn.clone()),
            sender: Arc::new(tx),
            receiver: Mutex::new(rx),
            guard_id: Uuid::new_v4(),
            is_read_only: RwLock::new(is_read_only),
            wait_state: RwLock::new(WaitingState::DoneWaiting),
            keys,
        }
    }

    pub fn new_lock_table_guard_link(
        txn: Txn,
        is_read_only: bool,
        keys: SpanSet<Key>,
    ) -> LockTableGuardLink {
        Arc::new(LockTableGuard::new(txn, is_read_only, keys))
    }

    /**
     * should_wait should be called after scan_and_enqueue - if true, it should release latches
     * and wait until the lock is reserved by the transcation or it timed out
     */
    pub fn should_wait(&self) -> bool {
        *self.should_wait.read().unwrap()
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
        let read_guard = guard_link.as_ref();
        let txn_guard = read_guard.txn.read().unwrap();
        txn_guard.metadata.clone()
    }

    #[cfg(test)]
    pub fn get_guard_id(guard: LockTableGuardLink) -> Uuid {
        let g = guard.as_ref();
        g.guard_id
    }

    #[cfg(test)]
    pub fn get_txn_id(guard: LockTableGuardLink) -> Uuid {
        let g = guard.as_ref();
        let txn_id = g.txn.read().unwrap().txn_id;
        txn_id
    }
}
