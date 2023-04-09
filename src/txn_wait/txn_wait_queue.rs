use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, RwLock},
};

use tokio::{
    sync::{
        mpsc::{channel, Receiver, Sender},
        Mutex,
    },
    task::JoinHandle,
    time::{self, Duration},
};
use uuid::Uuid;

use crate::{
    db::{db::InternalDB, request_queue::ThreadPoolRequest},
    execute::executor::Executor,
};

/**
 * Holds a map from pushee txn ID to list of waitingPush queries.
 */
pub struct TxnWaitQueue {
    txns: TxnMap,
    request_sender: Arc<Sender<ThreadPoolRequest>>,
}

/**
 * Each WaitingPush corresponds to a pusher's attempt to push
 * a pushee.
 */
struct WaitingPush {
    // TODO: Do we need the Txn or smth here?
    dependents: RwLock<HashSet<Uuid>>,
    txn_id: Uuid,
    /**
     * Notifies the receiver that the pushTxn request has succeeded.
     * In other words, the pushee has finalized (aborted or committed)
     */
    sender: Arc<Sender<()>>,
    /**
     * The receiver will receive a message if the pushee's transaction
     * has finalized
     */
    receiver: Mutex<Receiver<()>>,
}

type WaitingPushLink = Arc<WaitingPush>;

struct PendingTxn {
    waiting_pushes: Vec<WaitingPushLink>,
}

type TxnMap = Arc<RwLock<HashMap<Uuid, Arc<RwLock<PendingTxn>>>>>;

impl PendingTxn {
    pub fn new() -> Self {
        PendingTxn {
            waiting_pushes: Vec::new(),
        }
    }
}

impl WaitingPush {
    pub fn new(txn_id: Uuid) -> Self {
        let (tx, rx) = channel::<()>(1);
        WaitingPush {
            dependents: RwLock::new(HashSet::new()),
            txn_id,
            sender: Arc::new(tx),
            receiver: Mutex::new(rx),
        }
    }

    pub fn get_dependents(&self) -> Vec<Uuid> {
        let set = self.dependents.read().unwrap();
        set.iter().map(|a| a.clone()).collect()
    }

    /**
     * uses the sender to notify the receiver that the waitingPush's
     * pushee has been released and the pending push is no longer blocked
     */
    pub async fn release_push(&self) {
        let sender = self.sender.clone();
        sender.send(()).await.unwrap();
    }
}

impl TxnWaitQueue {
    pub fn new(request_sender: Arc<Sender<ThreadPoolRequest>>) -> Self {
        TxnWaitQueue {
            txns: Arc::new(RwLock::new(HashMap::new())),
            request_sender,
        }
    }

    /**
     * Enqueue and push the waitingPush onto the pendingTxn's waiting_pushes.
     */
    fn enqueue_and_push(&self, pusher_txn_id: Uuid, pushee_txn_id: Uuid) -> WaitingPushLink {
        let mut txns = self.txns.write().unwrap();
        let pushee = txns.get(&pushee_txn_id);

        let waiting_push = Arc::new(WaitingPush::new(pusher_txn_id));

        match pushee {
            Some(pending_txn) => {
                pending_txn
                    .write()
                    .unwrap()
                    .waiting_pushes
                    .push(waiting_push.clone());
            }
            None => {
                let mut pending_txn = PendingTxn::new();
                pending_txn.waiting_pushes.push(waiting_push.clone());
                txns.insert(pushee_txn_id, Arc::new(RwLock::new(pending_txn)));
            }
        };
        waiting_push
    }

    fn get_pending_txn(&self, txn_id: Uuid) -> Option<Arc<RwLock<PendingTxn>>> {
        let txns = self.txns.read().unwrap();
        let pushee_option = txns.get(&txn_id).and_then(|pushee| Some(pushee.clone()));
        pushee_option
    }

    /**
     * Finalize_txn is invoked when the transaction has finalized (aborted or committed).
     * It unblocks all pending waiters.
     */
    async fn finalize_txn(&self, txn_id: Uuid) {
        let pushee = self.get_pending_txn(txn_id);
        match pushee {
            Some(pushee) => {
                let waiting_pushes = &pushee.read().unwrap().waiting_pushes;
                for waiting_push in waiting_pushes.iter() {
                    waiting_push.release_push().await;
                }
                self.remove_pushee(txn_id);
            }
            None => {}
        }
    }

    /**
     * Removes the pushee from the txns queue
     */
    fn remove_pushee(&self, pushee_txn_id: Uuid) {
        let mut txns = self.txns.write().unwrap();
        txns.remove(&pushee_txn_id);
    }

    /**
     * Removes the waitingPush from its pushee's waiting_pushes
     */
    fn dequeue(&self, waiting_push: WaitingPush, pushee_txn_id: Uuid) {
        todo!()
    }

    /**
     * This function will first create a waiting_push and add it to the txn waitQueue.
     * It will then wait for the waiting_push's pushee to be finalized (aborted/committed).
     */
    pub async fn wait_for_push(&self, pusher_txn_id: Uuid, pushee_txn_id: Uuid) {
        let waiting_push_link = self.enqueue_and_push(pusher_txn_id, pushee_txn_id);
        let (query_dependents_handle, mut dependents_rx) =
            self.start_query_pusher_txn_dependents(pusher_txn_id);
        let mut rx = waiting_push_link.receiver.lock().await;

        loop {
            tokio::select! {
                Some(dependents) = dependents_rx.recv() => {
                    let is_cycle_detected = dependents.contains(&pushee_txn_id);
                    // check if there is a dependency
                    if is_cycle_detected {

                    // TODO: abort pushee with sender
                        return;
                    }
                println!("Received dependents. No cycles detected yet!");
                }
                Some(_) = rx.recv() => {
                    // ends the loop to query for dependents
                    query_dependents_handle.abort();
                    println!("Pushee has finalized");
                    return;
                }
            }
        }
    }

    fn get_dependents(txn_map: TxnMap, txn_id: Uuid) -> Vec<Uuid> {
        let txns = txn_map.read().unwrap();
        let pending_txn = txns.get(&txn_id);
        match pending_txn {
            Some(pending_txn) => {
                let pending = pending_txn.read().unwrap();
                pending
                    .waiting_pushes
                    .iter()
                    .map(|push| push.get_dependents())
                    .collect::<Vec<Vec<Uuid>>>()
                    .concat()
            }
            None => Vec::new(),
        }
    }

    /**
     * Starts a loop to query a txn's waiting_push's dependents
     */
    fn start_query_pusher_txn_dependents(
        &self,
        txn_id: Uuid,
    ) -> (JoinHandle<()>, Receiver<Vec<Uuid>>) {
        let (tx, rx) = channel::<Vec<Uuid>>(1);
        let txns = self.txns.clone();
        let handle = tokio::spawn(async move {
            let foo = txns.clone();
            loop {
                // TODO: We need a way to terminate the loop
                let dependents = TxnWaitQueue::get_dependents(foo.clone(), txn_id);
                tx.send(dependents).await.unwrap();
                time::sleep(Duration::from_millis(10)).await;
            }
        });
        (handle, rx)
    }
}
