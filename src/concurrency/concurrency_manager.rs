use core::time;
use std::collections::HashSet;
use std::thread;

use crate::latch_manager::latch_interval_btree::{BTree, Range};
use crate::storage::Key;
use crate::{execute::request::SpansToAcquire, latch_manager::latch_interval_btree::NodeKey};
use tokio::sync;

pub struct ConcurrencyManager {
    latch_tree: BTree<Key>,
}

pub struct Guard {
    spansets: SpansToAcquire,
}

impl ConcurrencyManager {
    pub fn new() -> Self {
        ConcurrencyManager {
            latch_tree: BTree::new(3),
        }
    }

    // TODO: Instead of looping, can we have a hashmap where
    // key is duplicate keys and we add a lock guard or something for that key
    // and we wait on that to be released. When delete, we have a callback that
    // removes it
    pub fn sequence_req(&self, spans_to_acquire: SpansToAcquire, str: &str) -> Guard {
        // TODO: Randomnize
        let delay = time::Duration::from_micros(1);
        let total_latch_count = spans_to_acquire.latch_spans.len();
        let mut acquired_set: HashSet<usize> = HashSet::new();
        loop {
            for (pos, range) in spans_to_acquire.latch_spans.iter().enumerate() {
                // if !acquired_set.contains(&pos) {
                //     let did_insert = self.latch_tree.insert(Range {
                //         start_key: range.start_key.clone(),
                //         end_key: range.end_key.clone(),
                //     });
                //     if did_insert.is_ok() {
                //         acquired_set.insert(pos);
                //     }
                // }
            }

            if acquired_set.len() == total_latch_count {
                break;
            }
            let latches = &spans_to_acquire.latch_spans;
            // release all acquired to prevent deadlock
            let mut collected = Vec::new();
            for idx in acquired_set.iter() {
                collected.push(latches[*idx].clone())
            }
            let guard = Guard {
                spansets: SpansToAcquire {
                    latch_spans: collected.clone(),
                    lock_spans: collected.clone(),
                },
            };
            self.release_guard(guard);
            println!("retrying sequencing {}", str);
            // TODO: This is definitely not the way, just needed to do this to get the
            // MVP working
            thread::sleep(delay);
        }
        Guard {
            spansets: SpansToAcquire {
                latch_spans: spans_to_acquire.latch_spans.clone(),
                lock_spans: spans_to_acquire.lock_spans.clone(),
            },
        }
    }

    pub fn release_guard(&self, guard: Guard) -> () {
        println!("Releasing guard");
        for range in guard.spansets.latch_spans.iter() {
            self.latch_tree.delete(range.start_key.clone());
        }
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

        #[test]
        fn experiment() {
            let concurrency_manager = Arc::new(ConcurrencyManager::new());
            let thread1_manager = concurrency_manager.clone();
            let handle1 = thread::spawn(move || {
                let guard = thread1_manager.sequence_req(
                    SpansToAcquire {
                        latch_spans: Vec::from([
                            Range {
                                start_key: str_to_key("a"),
                                end_key: str_to_key("a"),
                            },
                            Range {
                                start_key: str_to_key("b"),
                                end_key: str_to_key("b"),
                            },
                        ]),
                        lock_spans: Vec::new(),
                    },
                    "FIRST",
                );

                // thread::sleep(time::Duration::from_micros(15));
                // println!("Releasing first guard");
                thread1_manager.release_guard(guard);
            });

            let thread2_manager = concurrency_manager.clone();

            let handle2 = thread::spawn(move || {
                let guard2 = thread2_manager.clone().sequence_req(
                    SpansToAcquire {
                        latch_spans: Vec::from([
                            Range {
                                start_key: str_to_key("a"),
                                end_key: str_to_key("a"),
                            },
                            Range {
                                start_key: str_to_key("b"),
                                end_key: str_to_key("b"),
                            },
                        ]),
                        lock_spans: Vec::new(),
                    },
                    "SECOND",
                );
                // println!("Releasing second guard");

                thread2_manager.release_guard(guard2);
            });

            handle1.join().unwrap();
            handle2.join().unwrap();
        }
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
