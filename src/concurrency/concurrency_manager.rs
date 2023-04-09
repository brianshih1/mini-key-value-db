use core::time;
use std::collections::HashSet;
use std::sync::Arc;
use std::thread;

use tokio::sync::mpsc::Sender;

use crate::db::db::{TxnLink, TxnMap};
use crate::db::request_queue::ThreadPoolRequest;
use crate::execute::request::{Command, Request};
use crate::latch_manager::latch_manager::{LatchGuard, LatchManager};
use crate::lock_table::lock_table::{LockTable, LockTableGuardLink, UpdateLock};
use crate::storage::Key;

pub struct ConcurrencyManager {
    latch_manager: LatchManager<Key>,
    pub lock_table: LockTable,
}

pub struct Guard {
    latch_guard: LatchGuard<Key>,

    lock_guard: LockTableGuardLink,
}

impl ConcurrencyManager {
    pub fn new(txns: TxnMap, request_sender: Arc<Sender<ThreadPoolRequest>>) -> Self {
        ConcurrencyManager {
            latch_manager: LatchManager::new(),
            lock_table: LockTable::new(txns, request_sender),
        }
    }

    pub async fn sequence_req(&self, request: &Request) -> Guard {
        let spans_to_acquire = request
            .request_union
            .collect_spans(request.metadata.txn.clone());
        loop {
            println!("Sequence req loop!");
            let latch_guard = self
                .latch_manager
                .acquire_and_wait(spans_to_acquire.clone())
                .await;
            let (should_wait, lock_guard) = self.lock_table.scan_and_enqueue(request).await;
            if should_wait {
                self.latch_manager.release(latch_guard);
                self.lock_table.wait_for(lock_guard).await;
                // restart the loop to re-acquire latches and rescan the lockTable
                continue;
            } else {
                return Guard {
                    latch_guard,
                    lock_guard,
                };
            }
        }
    }

    /**
     * Release latches and dequeues the request from any lock tables.
     */
    pub async fn finish_req(&self, guard: Guard) -> () {
        self.latch_manager.release(guard.latch_guard);
        self.lock_table.dequeue(guard.lock_guard.clone()).await;
    }

    fn get_txn_lock_spans(txn_link: TxnLink) -> Vec<Key> {
        let txn = txn_link.read().unwrap();
        let keys = txn.lock_spans.read().unwrap();
        keys.clone()
        // TODO: Remove clone
    }

    pub async fn update_txn_locks(&self, txn: TxnLink, update_lock: UpdateLock) {
        let keys = ConcurrencyManager::get_txn_lock_spans(txn.clone());

        for key in keys.iter() {
            self.lock_table
                .update_locks(key.clone(), &update_lock)
                .await;
        }
    }
}

mod test {
    use std::thread;

    use tokio::{
        sync::{
            mpsc::{self, channel, Receiver, Sender},
            Mutex,
        },
        time::{self, Duration},
    };

    mod sequence_req {
        use core::time;
        use std::{sync::Arc, thread};

        use crate::{
            concurrency::concurrency_manager::ConcurrencyManager, execute::request::SpansToAcquire,
            latch_manager::latch_interval_btree::Range, storage::str_to_key,
        };

        //     #[test]
        //     fn experiment() {
        //         let concurrency_manager = Arc::new(ConcurrencyManager::new());
        //         let thread1_manager = concurrency_manager.clone();
        //         let handle1 = thread::spawn(move || {
        //             let guard = thread1_manager.sequence_req(SpansToAcquire {
        //                 latch_spans: Vec::from([
        //                     Range {
        //                         start_key: str_to_key("a"),
        //                         end_key: str_to_key("a"),
        //                     },
        //                     Range {
        //                         start_key: str_to_key("b"),
        //                         end_key: str_to_key("b"),
        //                     },
        //                 ]),
        //                 lock_spans: Vec::new(),
        //             });

        //             // thread::sleep(time::Duration::from_micros(15));
        //             // println!("Releasing first guard");
        //             thread1_manager.release_guard(guard);
        //         });

        //         let thread2_manager = concurrency_manager.clone();

        //         let handle2 = thread::spawn(move || {
        //             let guard2 = thread2_manager.clone().sequence_req(
        //                 SpansToAcquire {
        //                     latch_spans: Vec::from([
        //                         Range {
        //                             start_key: str_to_key("a"),
        //                             end_key: str_to_key("a"),
        //                         },
        //                         Range {
        //                             start_key: str_to_key("b"),
        //                             end_key: str_to_key("b"),
        //                         },
        //                     ]),
        //                     lock_spans: Vec::new(),
        //                 },
        //                 "SECOND",
        //             );
        //             // println!("Releasing second guard");

        //             thread2_manager.release_guard(guard2);
        //         });

        //         handle1.join().unwrap();
        //         handle2.join().unwrap();
        //     }
    }

    struct TestGuard {
        sender: Sender<u32>,
        receiver: Receiver<u32>,
    }

    #[tokio::test]
    async fn test_select() {
        let (tx, mut rx) = channel::<u32>(1);

        let sleep = time::sleep(Duration::from_millis(1000));
        tokio::pin!(sleep);

        tokio::spawn(async move {
            println!("sending!");
            tx.send(12).await.unwrap();
        });

        tokio::select! {
            Some(ctrl) = rx.recv() => {
                println!("Control is: {}", ctrl);
            }
            _ = &mut sleep, if !sleep.is_elapsed() => {
                println!("operation timed out");
            }
        };
    }

    #[tokio::test]
    async fn test_async_loop() {
        // let (tx, mut rx) = channel::<u32>(1);

        tokio::spawn(async move {
            loop {
                println!("Loop");
                let sleep = time::sleep(Duration::from_millis(10));
                tokio::pin!(sleep);

                tokio::select! {
                    _ = &mut sleep, if !sleep.is_elapsed() => {
                        println!("operation timed out");
                    }
                };
            }
        });
    }
}
