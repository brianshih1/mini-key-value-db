use core::time;
use std::collections::HashSet;
use std::thread;

use crate::latch_manager::latch_interval_btree::{BTree, Range};
use crate::{execute::request::SpansToAcquire, latch_manager::latch_interval_btree::NodeKey};

pub struct ConcurrencyManager<K: NodeKey> {
    latch_tree: BTree<K>,
}

pub struct Guard {}

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
    pub fn sequence_req(&self, spans_to_acquire: SpansToAcquire<K>) -> Guard {
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
            println!("HELLO");
            // TODO: This is definitely not the way, just needed to do this to get the
            // MVP working
            thread::sleep(delay);
        }
        Guard {}
    }

    pub fn release_guard(&self, guard: Guard) -> () {}
}

mod Test {
    mod SequenceReq {
        use std::thread;

        use crate::{
            concurrency::concurrency_manager::ConcurrencyManager, execute::request::SpansToAcquire,
            latch_manager::latch_interval_btree::Range,
        };

        #[test]
        fn experiment() {
            let cncr_manager = ConcurrencyManager::<i32>::new();
            // thread::spawn(|| {
            //     let foo = &cncr_manager;
            //     // cncr_manager.sequence_req(SpansToAcquire {
            //     //     latch_spans: Vec::from([
            //     //         Range {
            //     //             start_key: 3,
            //     //             end_key: 3,
            //     //         },
            //     //         Range {
            //     //             start_key: 5,
            //     //             end_key: 5,
            //     //         },
            //     //     ]),
            //     //     lock_spans: Vec::new(),
            //     // });
            // });

            cncr_manager.sequence_req(SpansToAcquire {
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
        }
    }
}
