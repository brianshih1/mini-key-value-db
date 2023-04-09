use std::{
    sync::{
        mpsc::{self},
        Arc,
    },
    thread,
};

use tokio::{
    runtime::Runtime,
    spawn,
    sync::{
        mpsc::{Receiver, Sender},
        Mutex,
    },
};

use crate::execute::executor::Executor;

use super::db::TxnMap;

#[derive(Debug)]
pub enum ThreadPoolRequest {
    Terminate,
}

// The implementation of ThreadPool is based on the ThreadPool
// implementation from The Rust Programing Language (Chpter 20: Building a Multithreaded Web Server)
pub struct ThreadPool {
    executor: Arc<Executor>,
    txns: TxnMap,
    sender: Arc<Sender<ThreadPoolRequest>>,
}

impl ThreadPool {
    pub fn new(
        receiver: Receiver<ThreadPoolRequest>,
        executor: Arc<Executor>,
        txns: TxnMap,
        sender: Arc<Sender<ThreadPoolRequest>>,
    ) -> Self {
        let receiver = Arc::new(Mutex::new(receiver));
        spawn(async move {
            loop {
                // println!("worker looping!");
                let mut locked = receiver.lock().await;
                let request = locked.recv().await;
                match request {
                    Some(request) => match request {
                        ThreadPoolRequest::Terminate => {
                            println!("Ending worker");
                            break;
                        }
                    },
                    None => {}
                }
            }
        });

        ThreadPool {
            executor,
            txns,
            sender,
        }
    }
}
