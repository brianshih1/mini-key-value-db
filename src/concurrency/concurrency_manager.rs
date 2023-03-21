use core::time;
use std::collections::HashSet;
use std::thread;

use crate::latch_manager::latch_interval_btree::{BTree, Range};
use crate::latch_manager::latch_manager::{LatchGuard, LatchManager};
use crate::lock_table::lock_table::LockTable;
use crate::storage::Key;
use crate::{execute::request::SpansToAcquire, latch_manager::latch_interval_btree::NodeKey};
use tokio::sync;

pub struct ConcurrencyManager {
    latch_manager: LatchManager<Key>,
    pub lock_table: LockTable,
}

pub struct Guard {
    latch_guard: LatchGuard<Key>,
}

impl ConcurrencyManager {
    pub fn new() -> Self {
        ConcurrencyManager {
            latch_manager: LatchManager::new(),
            lock_table: LockTable::new(),
        }
    }

    // TODO: Instead of looping, can we have a hashmap where
    // key is duplicate keys and we add a lock guard or something for that key
    // and we wait on that to be released. When delete, we have a callback that
    // removes it
    pub async fn sequence_req(&self, spans_to_acquire: SpansToAcquire) -> Guard {
        loop {
            let latch_guard = self
                .latch_manager
                .acquire_and_wait(spans_to_acquire.latch_spans.clone())
                .await;
            let lock_guard = self.lock_table.scan_and_enqueue();
            if lock_guard.should_wait() {
                self.latch_manager.release(latch_guard);
                self.lock_table.wait_for(lock_guard);
            }
        }
    }

    pub fn release_guard(&self, guard: Guard) -> () {
        todo!()
    }
}

mod Test {
    use std::thread;

    use tokio::{
        sync::{
            mpsc::{self, channel, Receiver, Sender},
            Mutex,
        },
        time::{self, Duration},
    };

    mod SequenceReq {
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
    // #[test]
    // fn learn_channel() {
    //     let (tx, rx) = channel::<u32>(1);
    //     let guard = TestGuard {
    //         sender: tx,
    //         receiver: rx,
    //     };
    //     let tx1 = mpsc::Sender::clone(&guard.sender);
    //     thread::spawn(move || {
    //         println!("sending!");
    //         tx1.send(12).await;
    //     });
    //     guard.receiver.recv().unwrap();
    //     println!("foo");
    // }

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
}
