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
    db::{
        db::InternalDB,
        request_queue::{
            AbortTxnQueueRequest, QueueResponseUnion, TaskQueueRequest, TaskQueueRequestUnion,
        },
    },
    execute::executor::Executor,
    storage::{mvcc::KVStore, txn::TransactionStatus},
};

/**
 * Holds a map from pushee txn ID to list of waitingPush queries.
 */
pub struct TxnWaitQueue {
    pushees: TxnWaitingPushesMap,
    pub request_sender: Arc<Sender<TaskQueueRequest>>,
    pub store: Arc<KVStore>,
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
    pushee_finalized_receiver: Mutex<Receiver<()>>,
}

type WaitingPushLink = Arc<WaitingPush>;

struct PendingTxn {
    waiting_pushes: Vec<WaitingPushLink>,
}

type TxnWaitingPushesMap = Arc<RwLock<HashMap<Uuid, Arc<RwLock<PendingTxn>>>>>;

impl PendingTxn {
    pub fn new() -> Self {
        PendingTxn {
            waiting_pushes: Vec::new(),
        }
    }
}

pub struct PushTxnResponse {}

pub enum WaitForPushError {
    TxnAborted,
    TxnCommitted,
}

impl WaitingPush {
    pub fn new(txn_id: Uuid) -> Self {
        let (tx, rx) = channel::<()>(1);
        WaitingPush {
            dependents: RwLock::new(HashSet::new()),
            txn_id,
            sender: Arc::new(tx),
            pushee_finalized_receiver: Mutex::new(rx),
        }
    }

    pub fn get_dependents(&self) -> Vec<Uuid> {
        let set = self.dependents.read().unwrap();
        println!("Dependents len is: {}", set.len());
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
    pub fn new(request_sender: Arc<Sender<TaskQueueRequest>>, store: Arc<KVStore>) -> Self {
        TxnWaitQueue {
            pushees: Arc::new(RwLock::new(HashMap::new())),
            request_sender,
            store,
        }
    }

    /**
     * Enqueue and push the waitingPush onto the pendingTxn's waiting_pushes.
     */
    fn enqueue_and_push(&self, pusher_txn_id: Uuid, pushee_txn_id: Uuid) -> WaitingPushLink {
        let mut pushees = self.pushees.write().unwrap();
        let pushee = pushees.get(&pushee_txn_id);

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
                println!(
                    "TxnWaitQ - No pending_txn found for pushee: {}. Creating one",
                    pushee_txn_id
                );
                let mut pending_txn = PendingTxn::new();
                pending_txn.waiting_pushes.push(waiting_push.clone());
                pushees.insert(pushee_txn_id, Arc::new(RwLock::new(pending_txn)));
            }
        };
        waiting_push
    }

    fn get_pending_txn(&self, txn_id: Uuid) -> Option<Arc<RwLock<PendingTxn>>> {
        let txns = self.pushees.read().unwrap();
        let pushee_option = txns.get(&txn_id).and_then(|pushee| Some(pushee.clone()));
        pushee_option
    }

    /**
     * Finalize_txn is invoked when the transaction has finalized (aborted or committed).
     * It unblocks all pending waiters.
     */
    pub async fn finalize_txn(&self, txn_id: Uuid) {
        let pushee = self.get_pending_txn(txn_id);
        match pushee {
            Some(pushee) => {
                let waiting_pushes = &pushee.read().unwrap().waiting_pushes.clone();
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
        let mut txns = self.pushees.write().unwrap();
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
    pub async fn wait_for_push(
        &self,
        pusher_txn_id: Uuid,
        pushee_txn_id: Uuid,
    ) -> Result<PushTxnResponse, WaitForPushError> {
        println!(
            "Wait_for_push called. Pusher: {}. Pushee: {}",
            pusher_txn_id, pushee_txn_id
        );
        let waiting_push_link = self.enqueue_and_push(pusher_txn_id, pushee_txn_id);
        let (query_dependents_handle, mut dependents_rx) =
            self.start_query_pusher_txn_dependents(pusher_txn_id, waiting_push_link.clone());
        let mut rx = waiting_push_link.pushee_finalized_receiver.lock().await;
        let mut loop_count = 0;
        loop {
            loop_count += 1;
            if loop_count > 25 {
                panic!(
                    "Exceeded count waiting for push. Pusher: {}. Pushee: {}",
                    pusher_txn_id, pushee_txn_id
                );
            }
            tokio::select! {
                Some((dependents, txn_status)) = dependents_rx.recv() => {
                    // if the pusher's transaciton is finalized, then the push itself failed.
                    match txn_status {
                        TransactionStatus::PENDING => {},
                        TransactionStatus::COMMITTED => {
                            return Err(WaitForPushError::TxnCommitted);
                        },
                        TransactionStatus::ABORTED => {
                            return Err(WaitForPushError::TxnAborted);
                        },
                    }
                    let is_cycle_detected = dependents.contains(&pushee_txn_id);
                    // check if there is a dependency
                    if is_cycle_detected {
                        query_dependents_handle.abort();
                        let (push_txn_sender, mut push_txn_rx) = channel(1);
                        self.request_sender
                            .send(TaskQueueRequest{
                                done_sender: Arc::new(push_txn_sender),
                                request: TaskQueueRequestUnion::AbortTxn(
                                    AbortTxnQueueRequest { txn_id: pushee_txn_id }
                                )
                            })
                            .await
                            .unwrap();
                        let response = push_txn_rx.recv().await.unwrap();
                        match response {
                            QueueResponseUnion::AbortTxn(_) => {
                                println!("Ending waitForPush, finished aborting pushee: {}", pushee_txn_id);
                                return Ok(PushTxnResponse{})
                            },
                        }
                    }
                    println!("Received dependents. No cycles detected yet! Dependents: {:?}", dependents);
                }
                Some(_) = rx.recv() => {
                    // ends the loop to query for dependents
                    query_dependents_handle.abort();
                    println!("Pushee has finalized");
                    return Ok(PushTxnResponse{});
                }
            }
        }
    }

    fn get_transitive_dependents(txn_map: TxnWaitingPushesMap, txn_id: Uuid) -> Vec<Uuid> {
        let txns = txn_map.read().unwrap();
        let pending_txn = txns.get(&txn_id);
        match pending_txn {
            Some(pending_txn) => {
                let pending = pending_txn.read().unwrap();
                pending
                    .waiting_pushes
                    .iter()
                    .map(|push| {
                        let mut vec = push.get_dependents();
                        vec.push(push.txn_id);
                        vec
                    })
                    .collect::<Vec<Vec<Uuid>>>()
                    .concat()
            }
            None => {
                println!("No pushee for {} found when get_dependents", txn_id);
                Vec::new()
            }
        }
    }

    /**
     * Starts a loop to query a txn's waiting_push's dependents
     */
    fn start_query_pusher_txn_dependents(
        &self,
        txn_id: Uuid,
        pusher_waiting_push_link: Arc<WaitingPush>,
    ) -> (JoinHandle<()>, Receiver<(Vec<Uuid>, TransactionStatus)>) {
        let (tx, rx) = channel::<(Vec<Uuid>, TransactionStatus)>(1);
        let txns = self.pushees.clone();
        let store = self.store.clone();
        let handle = tokio::spawn(async move {
            loop {
                // TODO: We need a way to terminate the loop
                let dependents = TxnWaitQueue::get_transitive_dependents(txns.clone(), txn_id);
                let txn_record = store.get_transaction_record(txn_id).unwrap();
                // add dependents to waitingPush's dependents
                pusher_waiting_push_link
                    .as_ref()
                    .dependents
                    .write()
                    .unwrap()
                    .extend(dependents.iter());
                // update waitingPush
                let res = tx.send((dependents, txn_record.status)).await;
                match res {
                    Ok(_) => {}
                    Err(err) => {
                        panic!("Failed to send dependents and txn status: {}", err);
                    }
                }

                time::sleep(Duration::from_millis(10)).await;
            }
        });
        (handle, rx)
    }
}
