use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, RwLock},
};

use tokio::{
    sync::mpsc::{channel, Receiver},
    task::JoinHandle,
    time::{self, Duration},
};
use uuid::Uuid;

struct WaitingPush {
    // TODO: Do we need the Txn or smth here?
    dependents: RwLock<HashSet<Uuid>>,
    txn_id: Uuid,
}

type WaitingPushLink = Arc<WaitingPush>;

struct PendingTxn {
    waiting_pushes: Vec<WaitingPushLink>,
}

/**
 * Holds a map from pushee txn ID to list of waitingPush queries.
 */
struct TxnWaitQueue {
    txns: TxnMap,
}

type TxnMap = Arc<RwLock<HashMap<Uuid, RwLock<PendingTxn>>>>;

impl PendingTxn {
    pub fn new() -> Self {
        PendingTxn {
            waiting_pushes: Vec::new(),
        }
    }
}

impl WaitingPush {
    pub fn new(txn_id: Uuid) -> Self {
        WaitingPush {
            dependents: RwLock::new(HashSet::new()),
            txn_id,
        }
    }

    pub fn get_dependents(&self) -> Vec<Uuid> {
        let set = self.dependents.read().unwrap();
        set.iter().map(|a| a.clone()).collect()
    }
}

impl TxnWaitQueue {
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
                txns.insert(pushee_txn_id, RwLock::new(pending_txn));
            }
        };
        waiting_push
    }

    /**
     * Removes the pushee from the txns queue
     */
    fn remove_pushee(&self, pushee_txn_id: Uuid) {}

    /**
     * Removes the waitingPush from its pushee's waiting_pushes
     */
    fn dequeue(&self, waiting_push: WaitingPush, pushee_txn_id: Uuid) {
        todo!()
    }

    /**
     * Waits for the waiting_push's pushee to be finalized (aborted/committed)
     */
    fn wait_for_push(&self) {
        // tokio::select! {}
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

    // /**
    //  * Starts a loop to query a txn's waiting_push's dependents
    //  */
    async fn start_query_pusher_txn(&self, txn_id: Uuid) -> (JoinHandle<()>, Receiver<Vec<Uuid>>) {
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
