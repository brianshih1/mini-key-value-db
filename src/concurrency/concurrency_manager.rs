use std::sync::Arc;

use anyhow::Result;
use tokio::sync::mpsc::Sender;

use crate::db::db::{TxnLink, TxnMap};
use crate::db::request_queue::TaskQueueRequest;
use crate::execute::request::{Command, Request};
use crate::latch_manager::latch_manager::{LatchGuard, LatchManager};
use crate::lock_table::lock_table::{LockTable, LockTableGuardLink, UpdateLock, WaitForGuardError};
use crate::storage::mvcc::KVStore;
use crate::storage::Key;
use tracing::error;

pub struct ConcurrencyManager {
    latch_manager: LatchManager<Key>,
    pub lock_table: LockTable,
    pub store: Arc<KVStore>,
}

pub struct Guard {
    latch_guard: LatchGuard<Key>,

    lock_guard: LockTableGuardLink,
}

#[derive(Debug)]
pub enum SequenceReqError {
    TxnAborted,
    TxnCommitted,
}

impl ConcurrencyManager {
    pub fn new(
        txns: TxnMap,
        request_sender: Arc<Sender<TaskQueueRequest>>,
        store: Arc<KVStore>,
    ) -> Self {
        ConcurrencyManager {
            latch_manager: LatchManager::new(),
            lock_table: LockTable::new(txns, request_sender, store.clone()),
            store,
        }
    }

    pub async fn sequence_req(&self, request: &Request) -> Result<Guard, SequenceReqError> {
        let spans_to_acquire = request
            .request_union
            .collect_spans(request.metadata.txn.clone());
        loop {
            let latch_guard = self.latch_manager.acquire(spans_to_acquire.clone()).await;
            let (should_wait, lock_guard) = self.lock_table.scan_and_enqueue(request).await;
            if should_wait {
                self.latch_manager.release(latch_guard);
                let wait_res = self.lock_table.wait_for(lock_guard).await;
                if let Err(err) = wait_res {
                    match err {
                        WaitForGuardError::TxnAborted => return Err(SequenceReqError::TxnAborted),
                        WaitForGuardError::TxnCommitted => {
                            return Err(SequenceReqError::TxnCommitted)
                        }
                    }
                };

                // restart the loop to re-acquire latches and rescan the lockTable
                continue;
            } else {
                return Ok(Guard {
                    latch_guard,
                    lock_guard,
                });
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
    }

    pub async fn update_txn_locks(&self, txn: TxnLink, update_lock: UpdateLock) {
        let keys = ConcurrencyManager::get_txn_lock_spans(txn.clone());

        for key in keys.iter() {
            println!("Updating lock for key: {:?}", key);
            self.lock_table
                .update_locks(key.clone(), &update_lock)
                .await;
        }
    }
}

#[cfg(test)]
mod test {
    use std::time::Duration;

    use tokio::{sync::mpsc::channel, time};

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
