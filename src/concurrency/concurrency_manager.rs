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
    pub fn sequence_req(&self, spansToAcquire: SpansToAcquire<K>) -> Guard {
        let delay = time::Duration::from_micros(1);
        let totalLatches = spansToAcquire.latch_spans.len();
        let acquiredSet: HashSet<usize> = HashSet::new();
        loop {
            for (pos, range) in spansToAcquire.latch_spans.iter().enumerate() {
                if !acquiredSet.contains(&pos) {
                    self.latch_tree.insert(Range {
                        start_key: range.start_key.clone(),
                        end_key: range.end_key.clone(),
                    });
                }
            }

            if acquiredSet.len() == totalLatches {
                break;
            }
            // TODO: This is definitely not the way, just needed to do this to get the
            // MVP working
            thread::sleep(delay);
        }
        todo!()
    }

    pub fn release_guard(&self, guard: Guard) -> () {}
}
