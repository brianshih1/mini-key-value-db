use std::{
    sync::{
        Arc,
    },
};

use tokio::{
    spawn,
    sync::{
        mpsc::{Receiver, Sender},
        Mutex,
    },
};
use uuid::Uuid;

use crate::execute::{
    executor::Executor,
    request::{AbortTxnRequest, Request, RequestMetadata, RequestUnion},
};

use super::db::{TxnLink, TxnMap};

#[derive(Debug)]
pub struct TaskQueueRequest {
    pub done_sender: Arc<Sender<QueueResponseUnion>>,
    pub request: TaskQueueRequestUnion,
}

#[derive(Debug)]
pub enum TaskQueueRequestUnion {
    AbortTxn(AbortTxnQueueRequest),
}

#[derive(Debug)]
pub enum QueueResponseUnion {
    AbortTxn(AbortTxnThreadPoolResponse),
}

#[derive(Debug)]
pub struct AbortTxnQueueRequest {
    pub txn_id: Uuid,
}

#[derive(Debug)]
pub struct AbortTxnThreadPoolResponse {}

// The implementation of ThreadPool is based on the ThreadPool
// implementation from The Rust Programing Language (Chpter 20: Building a Multithreaded Web Server)
pub struct TaskQueue {
    executor: Arc<Executor>,
    txns: TxnMap,
    sender: Arc<Sender<TaskQueueRequest>>,
}

impl TaskQueue {
    pub fn new(
        receiver: Receiver<TaskQueueRequest>,
        executor: Arc<Executor>,
        txns: TxnMap,
        sender: Arc<Sender<TaskQueueRequest>>,
    ) -> Self {
        let receiver = Arc::new(Mutex::new(receiver));
        let cloned_executor = executor.clone();
        let cloned_txns = txns.clone();
        spawn(async move {
            loop {
                let mut locked = receiver.lock().await;
                let request = locked.recv().await;
                let txns_cloned = cloned_txns.clone();
                let executor_cloned = cloned_executor.clone();
                spawn(async move {
                    match request {
                        Some(request) => match request.request {
                            TaskQueueRequestUnion::AbortTxn(abort_request) => {
                                let txn_id = abort_request.txn_id;
                                println!("Received an abort request for txn: {}", txn_id);

                                let txn_request = RequestUnion::AbortTxn(AbortTxnRequest {});
                                let txn = TaskQueue::get_txn(txns_cloned.clone(), txn_id);
                                let request_metadata = RequestMetadata { txn };
                                let done_sender = request.done_sender.clone();
                                let request = Request {
                                    metadata: request_metadata,
                                    request_union: txn_request,
                                };
                                let _response = executor_cloned
                                    .execute_request_with_concurrency_retries(request)
                                    .await
                                    .unwrap();
                                done_sender
                                    .send(QueueResponseUnion::AbortTxn(
                                        AbortTxnThreadPoolResponse {},
                                    ))
                                    .await
                                    .unwrap();
                            }
                        },
                        None => {}
                    }
                });
            }
        });

        TaskQueue {
            executor,
            txns,
            sender,
        }
    }

    fn get_txn(txns: TxnMap, txn_id: Uuid) -> TxnLink {
        let txns = txns.read().unwrap();
        let txn = txns.get(&txn_id);
        match txn {
            Some(txn_link) => txn_link.clone(),
            None => {
                panic!("No txn found for {}", txn_id)
            }
        }
    }
}
