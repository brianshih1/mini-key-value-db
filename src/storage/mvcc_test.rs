#[cfg(test)]
mod tests {
    use uuid::Uuid;

    use crate::{
        hlc::timestamp::Timestamp,
        storage::{
            mvcc::KVStore,
            txn::{TransactionStatus, TxnMetadata, TxnRecord},
        },
    };

    #[test]
    fn create_pending_transaction_record() -> () {
        let kv_store = KVStore::new("./tmp/data");
        let transaction_id = Uuid::new_v4();
        let write_timestamp = Timestamp {
            wall_time: 0,
            logical_time: 0,
        };
        kv_store.create_pending_transaction_record(transaction_id, write_timestamp);
        let transaction_record = kv_store.get_transaction_record(&transaction_id).unwrap();
        assert_eq!(
            transaction_record,
            TxnRecord {
                status: TransactionStatus::PENDING,
                metadata: TxnMetadata {
                    txn_id: transaction_id.clone(),
                    write_timestamp
                }
            }
        )
    }

    mod mvcc_put {
        use uuid::Uuid;

        use crate::{
            hlc::timestamp::Timestamp,
            storage::{
                mvcc::KVStore,
                mvcc_iterator::IterOptions,
                mvcc_key::MVCCKey,
                str_to_key,
                txn::{Txn, TxnMetadata},
            },
            WRITE_INTENT_ERROR,
        };

        #[test]
        fn put_with_transaction() {
            let kv_store = KVStore::new("./tmp/data");
            let key = "foo";
            let txn1_id = Uuid::new_v4();

            let timestamp = Timestamp {
                wall_time: 10,
                logical_time: 12,
            };
            let txn = Txn::new_link(txn1_id, timestamp.to_owned());
            kv_store
                .mvcc_put(str_to_key(key), None, Some(txn.clone()), 12)
                .unwrap();
            let mut it = kv_store
                .storage
                .new_mvcc_iterator(IterOptions { prefix: true });
            assert_eq!(it.current_key(), MVCCKey::create_intent_key_with_str(key));
            let is_valid = it.next();
            assert_eq!(is_valid, false);
        }

        #[test]
        fn write_intent_error() {
            let kv_store = KVStore::new("./tmp/data");
            let key = "foo";
            let txn1_id = Uuid::new_v4();

            let timestamp = Timestamp {
                wall_time: 10,
                logical_time: 12,
            };
            let txn = Txn::new_link(txn1_id, timestamp);

            kv_store.create_pending_transaction_record(txn1_id, timestamp.to_owned());
            let current_keys = kv_store.collect_all_mvcc_kvs();

            kv_store
                .mvcc_put(str_to_key(key), None, Some(txn.clone()), 12)
                .unwrap();

            let txn2_id = Uuid::new_v4();

            let second_txn = Txn::new_link(
                txn2_id,
                Timestamp {
                    wall_time: 12,
                    logical_time: 14,
                },
            );

            let res = kv_store.mvcc_put(str_to_key(key), None, Some(second_txn.clone()), 12);
            assert!(res.is_err());
        }
    }

    mod mvcc_get {
        use uuid::Uuid;

        use crate::{
            hlc::timestamp::Timestamp,
            storage::{
                mvcc::{KVStore, MVCCGetParams},
                mvcc_key::MVCCKey,
                serialized_to_value, str_to_key,
                txn::{Txn, TxnIntent, TxnMetadata},
            },
        };

        #[test]
        fn get_key_with_multiple_timestamps() {
            let kv_store = KVStore::new("./tmp/data");
            let read_timestamp = Timestamp::new(10, 10);

            let key1 = str_to_key("apple");
            let key1_timestamp1 = read_timestamp.decrement_by(3); // 7
            let key1_timestamp2 = read_timestamp.decrement_by(1); // 9
            let key1_timestamp3 = read_timestamp.advance_by(2); // 12

            kv_store
                .storage
                .put_serialized_with_mvcc_key(&MVCCKey::new(key1.clone(), key1_timestamp1), 10)
                .unwrap();

            let most_recent_key = MVCCKey::new(key1.clone(), key1_timestamp2);
            let most_recent_value = 11;
            kv_store
                .storage
                .put_serialized_with_mvcc_key(&most_recent_key.to_owned(), most_recent_value)
                .unwrap();

            kv_store
                .storage
                .put_serialized_with_mvcc_key(&MVCCKey::new(key1.clone(), key1_timestamp3), 12)
                .unwrap();

            let key2 = "banana";
            let key2_timestamp = read_timestamp.decrement_by(1);
            kv_store
                .storage
                .put_serialized_with_mvcc_key(&MVCCKey::new(str_to_key(key2), key2_timestamp), 10)
                .unwrap();

            let res = kv_store.mvcc_get(&key1, read_timestamp, MVCCGetParams { transaction: None });

            assert!(res.intent.is_none());
            assert_eq!(
                res.value,
                Some((
                    most_recent_key.to_owned(),
                    serialized_to_value(most_recent_value)
                ))
            );
        }

        #[test]
        fn get_intent() {
            let kv_store = KVStore::new("./tmp/data");
            let key = "foo";
            let txn1_id = Uuid::new_v4();

            let timestamp = Timestamp {
                wall_time: 10,
                logical_time: 12,
            };
            let txn = Txn::new_link(txn1_id, timestamp);

            kv_store.create_pending_transaction_record(txn1_id, timestamp);

            kv_store
                .mvcc_put(str_to_key(key), None, Some(txn.clone()), 12)
                .unwrap();

            let res = kv_store.mvcc_get(
                &str_to_key(key),
                timestamp.advance_by(2),
                MVCCGetParams { transaction: None },
            );

            assert_eq!(
                res.intent,
                Some((
                    TxnIntent {
                        txn_meta: TxnMetadata {
                            txn_id: txn1_id,
                            write_timestamp: timestamp,
                        },
                        key: str_to_key(key),
                    },
                    serialized_to_value(12)
                ))
            )
        }
    }

    mod mvcc_resolve_intent {
        use uuid::Uuid;

        use crate::{
            hlc::timestamp::Timestamp,
            storage::{
                mvcc::{serialize, KVStore, MVCCGetParams},
                mvcc_key::{create_intent_key, MVCCKey},
                str_to_key,
                txn::{Txn, TxnMetadata, UncommittedValue},
                Value,
            },
        };

        #[test]
        fn resolve_intent_with_higher_timestamp() {
            let kv_store = KVStore::new("./tmp/dataa");
            let uncommitted_timestamp = Timestamp::new(10, 10);

            let key = str_to_key("apple");

            let txn_id = Uuid::new_v4();
            let txn = Txn::new_link(txn_id, uncommitted_timestamp);
            let put_value = 1000;

            kv_store
                .mvcc_put(
                    key.clone(),
                    Some(uncommitted_timestamp),
                    Some(txn.clone()),
                    put_value,
                )
                .unwrap();

            let uncommitted_value_option = kv_store
                .storage
                .get_serialized_with_mvcc_key::<UncommittedValue>(&create_intent_key(&key))
                .unwrap();

            assert!(uncommitted_value_option.is_some());

            let commit_timestamp = Timestamp::new(12, 0);

            kv_store.mvcc_resolve_intent(key.clone(), commit_timestamp, txn_id);

            let uncommitted_value_option_2 = kv_store
                .storage
                .get_serialized_with_mvcc_key::<UncommittedValue>(&create_intent_key(&key))
                .unwrap();
            assert!(uncommitted_value_option_2.is_none());

            let resolved_value = kv_store
                .storage
                .get_serialized_with_mvcc_key::<i32>(&MVCCKey {
                    key: key.clone(),
                    timestamp: commit_timestamp,
                })
                .unwrap();
            assert_eq!(resolved_value, Some(put_value));
        }

        #[test]
        fn resolve_intent_owned_by_another_txn() {
            let kv_store = KVStore::new("./tmp/dataa");
            let uncommitted_timestamp = Timestamp::new(10, 10);

            let key = str_to_key("apple");

            let txn_id = Uuid::new_v4();
            let txn = Txn::new_link(txn_id, uncommitted_timestamp);
            let put_value = 1000;

            kv_store
                .mvcc_put(
                    key.clone(),
                    Some(uncommitted_timestamp),
                    Some(txn),
                    put_value,
                )
                .unwrap();

            let uncommitted_value_option = kv_store
                .storage
                .get_serialized_with_mvcc_key::<UncommittedValue>(&create_intent_key(&key))
                .unwrap();

            assert!(uncommitted_value_option.is_some());

            let commit_timestamp = Timestamp::new(12, 0);

            let txn_2_id = Uuid::new_v4();

            kv_store.mvcc_resolve_intent(key.clone(), commit_timestamp, txn_2_id);

            // uncommitted intent not deleted as it's owned by another txn
            let uncommitted_value_option_2 = kv_store
                .storage
                .get_serialized_with_mvcc_key::<UncommittedValue>(&create_intent_key(&key))
                .unwrap();
            assert!(uncommitted_value_option_2.is_some());
        }
    }
}
