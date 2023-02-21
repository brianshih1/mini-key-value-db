use core::time;
use std::collections::HashSet;
use std::thread;

use crate::latch_manager::latch_interval_btree::{BTree, Range};
use crate::{execute::request::SpansToAcquire, latch_manager::latch_interval_btree::NodeKey};

pub struct ConcurrencyManager<K: NodeKey> {
    latch_tree: BTree<K>,
}

pub struct Guard<K: NodeKey> {
    spansets: SpansToAcquire<K>,
}

impl<K: NodeKey> ConcurrencyManager<K> {
    pub fn new() -> Self {
        ConcurrencyManager {
            latch_tree: BTree::new(3),
        }
    }

    // TODO: Instead of looping, can we have a hashmap where
    // key is duplicate keys and we add a lock guard or something for that key
    // and we wait on that to be released. When delete, we have a callback that
    // removes it
    pub fn sequence_req(&self, spans_to_acquire: SpansToAcquire<K>) -> Guard<K> {
        let delay = time::Duration::from_micros(1);
        let total_latch_count = spans_to_acquire.latch_spans.len();
        let mut acquired_set: HashSet<usize> = HashSet::new();
        loop {
            for (pos, range) in spans_to_acquire.latch_spans.iter().enumerate() {
                if !acquired_set.contains(&pos) {
                    let did_insert = self.latch_tree.insert(Range {
                        start_key: range.start_key.clone(),
                        end_key: range.end_key.clone(),
                    });
                    if did_insert.is_ok() {
                        acquired_set.insert(pos);
                    }
                }
            }

            if acquired_set.len() == total_latch_count {
                break;
            }
            println!("retrying sequencing");
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

    pub fn release_guard(&self, guard: Guard<K>) -> () {
        for range in guard.spansets.latch_spans.iter() {
            self.latch_tree.delete(range.start_key.clone());
        }
    }
}

mod Test {
    mod SequenceReq {
        use core::time;
        use std::{sync::Arc, thread};

        use crate::{
            concurrency::concurrency_manager::ConcurrencyManager, execute::request::SpansToAcquire,
            latch_manager::latch_interval_btree::Range,
        };

        #[test]
        fn experiment() {
            let concurrency_manager = Arc::new(ConcurrencyManager::<i32>::new());
            let thread1_manager = concurrency_manager.clone();
            let handle1 = thread::spawn(move || {
                let guard = thread1_manager.sequence_req(SpansToAcquire {
                    latch_spans: Vec::from([
                        Range {
                            start_key: 3,
                            end_key: 3,
                        },
                        Range {
                            start_key: 5,
                            end_key: 5,
                        },
                    ]),
                    lock_spans: Vec::new(),
                });

                thread::sleep(time::Duration::from_micros(15));
                thread1_manager.release_guard(guard);
            });

            let thread2_manager = concurrency_manager.clone();

            let handle2 = thread::spawn(move || {
                thread2_manager.clone().sequence_req(SpansToAcquire {
                    latch_spans: Vec::from([
                        Range {
                            start_key: 3,
                            end_key: 3,
                        },
                        Range {
                            start_key: 5,
                            end_key: 5,
                        },
                    ]),
                    lock_spans: Vec::new(),
                });
            });

            let thread3_manager = concurrency_manager.clone();

            handle1.join().unwrap();
            handle2.join().unwrap();
        }
    }
}
