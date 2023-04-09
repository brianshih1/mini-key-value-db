use std::sync::mpsc::{Receiver, Sender};

use rand::Rng;
use tokio::time::{self, Duration};

use crate::execute::request::SpanSet;

use super::{
    latch_interval_btree::{BTree, LatchKeyGuard, NodeKey, Range},
    spanset::Span,
};

pub struct LatchManager<K: NodeKey> {
    tree: BTree<K>,
}

pub struct LatchGuard<K: NodeKey> {
    pub spans: SpanSet<K>,
}

impl<K: NodeKey> LatchManager<K> {
    pub fn new() -> Self {
        let tree = BTree::new(3);
        LatchManager { tree }
    }

    // We currently don't support key-range locks. We only support single point locks
    pub async fn acquire_and_wait(&self, spans: SpanSet<K>) -> LatchGuard<K> {
        // create a timer and repeat until success
        // loop through the spans, add them, wait until it's released
        loop {
            let mut did_timeout = false;
            let mut acquired_indices = Vec::new();
            'outer: for (idx, span) in spans.iter().enumerate() {
                let mut did_acquire = false;
                loop {
                    let lg = self.tree.insert(Range {
                        start_key: span.start_key.clone(),
                        end_key: span.end_key.clone(),
                    });

                    let duration: u64 = rand::thread_rng().gen_range(50..150);
                    let sleep = time::sleep(Duration::from_millis(duration));
                    tokio::pin!(sleep);

                    if let LatchKeyGuard::NotAcquired(mut wait_guard) = lg {
                        tokio::select! {
                            Some(_) = wait_guard.receiver.recv() => {
                                println!("retry acquire lock")
                            }
                            _ = &mut sleep, if !sleep.is_elapsed() => {
                                println!("operation timed out. Releasing and retrying");
                                did_timeout = true;
                                break;
                            }

                        };
                    } else {
                        did_acquire = true;
                        acquired_indices.push(idx);
                    }
                    if did_acquire {
                        break;
                    }
                }
                if did_timeout {
                    // if there is a timeout, then it is likely related to a deadlock.
                    // We release all acquired latches then retry.
                    // TODO: what if we keep running into deadlocks?
                    for acquired_idx in acquired_indices.iter() {
                        let span = spans.get(*acquired_idx).unwrap();
                        self.tree.delete(span.start_key.clone())
                    }
                    break 'outer;
                }
            }
            if acquired_indices.len() == spans.len() {
                println!("Finished acquiring latch");
                return LatchGuard { spans };
            }
        }
    }

    pub fn release(&self, guard: LatchGuard<K>) -> () {
        for span in guard.spans.iter() {
            self.tree.delete(span.start_key.clone())
        }
    }
}

mod Test {
    mod Acquire {
        use std::sync::Arc;

        use tokio::time::{self, sleep, Duration};

        use crate::latch_manager::{latch_interval_btree::Range, latch_manager::LatchManager};

        #[tokio::test]
        async fn test_select() {
            let lm = Arc::new(LatchManager::<i32>::new());
            let guard = lm
                .acquire_and_wait(Vec::from([
                    Range {
                        start_key: 12,
                        end_key: 12,
                    },
                    Range {
                        start_key: 18,
                        end_key: 18,
                    },
                ]))
                .await;

            let lm2 = Arc::clone(&lm);

            tokio::spawn(async move {
                println!("sleeping!");
                sleep(Duration::from_millis(100)).await;
                println!("releasing!");
                lm2.release(guard)
            });

            lm.acquire_and_wait(Vec::from([
                Range {
                    start_key: 12,
                    end_key: 12,
                },
                Range {
                    start_key: 18,
                    end_key: 18,
                },
            ]))
            .await;
        }
    }
}
