use std::{
    sync::{
        mpsc::{self},
        Arc, Mutex,
    },
    thread,
};

use tokio::sync::mpsc::Receiver;

use crate::execute::executor::Executor;

use super::db::TxnMap;

pub enum ThreadPoolRequest {}

// The implementation of ThreadPool is based on the ThreadPool
// implementation from The Rust Programing Language (Chpter 20: Building a Multithreaded Web Server)
pub struct ThreadPool {
    executor: Arc<Executor>,
    txns: TxnMap,
    workers: Vec<Worker>,
}

struct Worker {
    thread: Option<thread::JoinHandle<()>>,
}

impl ThreadPool {
    pub fn new(
        receiver: Arc<Mutex<Receiver<ThreadPoolRequest>>>,
        executor: Arc<Executor>,
        txns: TxnMap,
    ) -> Self {
        // let (sender, rx) = mpsc::channel();

        let num_threads = 5;
        let mut workers = Vec::with_capacity(num_threads);

        for _ in 0..num_threads {
            workers.push(Worker::new(Arc::clone(&receiver)));
        }

        ThreadPool {
            executor,
            txns,
            workers,
        }
    }
}

impl Drop for ThreadPool {
    fn drop(&mut self) {
        for worker in &mut self.workers {
            if let Some(worker) = worker.thread.take() {
                worker.join().unwrap();
            }
        }
    }
}

impl Worker {
    pub fn new(receiver: Arc<Mutex<Receiver<ThreadPoolRequest>>>) -> Self {
        let thread = Some(thread::spawn(move || {
            // TODO: Can't block here
            tokio::runtime::Runtime::new().unwrap().block_on(async {
                loop {
                    let request = receiver.lock().unwrap().recv().await;
                    match request {
                        Some(_) => todo!(),
                        None => todo!(),
                    }
                }
            });
        }));

        Worker { thread }
    }
}
