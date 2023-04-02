#[cfg(test)]
mod executor {
    use std::{
        collections::HashMap,
        sync::{Arc, RwLock},
    };

    use crate::{
        db::db::{TxnLink, TxnMap},
        execute::executor::Executor,
        hlc::timestamp::Timestamp,
        lock_table::lock_table_test::test::{
            create_test_lock_table_guard, create_test_txn_with_timestamp,
        },
        storage::str_to_key,
    };

    pub fn add_txn_to_txn_map(txns: TxnMap, txn: TxnLink) {
        let mut txns = txns.write().unwrap();
        let txn_id = txn.read().unwrap().txn_id;
        txns.insert(txn_id, txn);
    }

    pub fn create_test_txn(txns: TxnMap, timestamp: Timestamp) -> TxnLink {
        let txn = create_test_txn_with_timestamp(timestamp);
        let txn_link = Arc::new(RwLock::new(txn));
        add_txn_to_txn_map(txns.clone(), txn_link.clone());
        txn_link
    }

    #[tokio::test]
    async fn test() {
        let txns = Arc::new(RwLock::new(HashMap::new()));
        let executor = Executor::new("./tmp/data", txns.clone());
        let txn_link = create_test_txn(txns, Timestamp::new(1, 1));
        let key = str_to_key("foo");
        let (_, lg_txn_link, lg) = create_test_lock_table_guard(false, Vec::from([key.clone()]));

        // execute read request
        // then execute write request and see if the timestamp is bumped
    }
}
