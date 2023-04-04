#[cfg(test)]
mod tests {
    #[cfg(test)]
    mod get_current_key {
        use uuid::Uuid;

        use crate::{
            hlc::timestamp::Timestamp,
            storage::{
                mvcc::KVStore,
                mvcc_iterator::{IterOptions, MVCCIterator},
                mvcc_key::MVCCKey,
                mvcc_scanner::MVCCScanner,
                storage::Storage,
                str_to_key,
                txn::{Txn, TxnIntent},
            },
        };

        #[test]
        fn no_intent_and_no_end_key() {
            let storage = Storage::new_cleaned("./tmp/test");
            let key = "foo";
            let mvcc_key_1 = MVCCKey::new(
                str_to_key(key),
                Timestamp {
                    logical_time: 1,
                    wall_time: 1,
                },
            );
            storage
                .put_serialized_with_mvcc_key(&mvcc_key_1, 10)
                .unwrap();

            let mvcc_key_2 = MVCCKey::new(
                str_to_key(key),
                Timestamp {
                    logical_time: 2,
                    wall_time: 2,
                },
            );
            storage
                .put_serialized_with_mvcc_key(&mvcc_key_2, 1)
                .unwrap();

            let iterator = MVCCIterator::new(&storage, IterOptions { prefix: true });
            let scanner_timestamp = Timestamp {
                logical_time: 3,
                wall_time: 3,
            };
            let mut scanner = MVCCScanner::new(
                iterator,
                key.as_bytes().to_vec(),
                None,
                scanner_timestamp,
                5,
                None,
            );
            scanner.get_current_key();
            assert_eq!(scanner.results.len(), 1);
            assert_eq!(scanner.found_intents.len(), 0);
        }

        #[test]
        fn intent_found() {
            let kv_store = KVStore::new("./tmp/data");
            let timestamp = Timestamp::new(12, 0);
            let txn_id = Uuid::new_v4();
            let txn = Txn::new_link(txn_id, timestamp);
            let key = "foo";
            kv_store
                .mvcc_put(str_to_key(key), Some(timestamp), Some(txn.clone()), 12)
                .unwrap();

            let iterator = MVCCIterator::new(&kv_store.storage, IterOptions { prefix: true });
            let scanner_timestamp = Timestamp {
                logical_time: 3,
                wall_time: 3,
            };
            let mut scanner = MVCCScanner::new(
                iterator,
                key.as_bytes().to_vec(),
                None,
                scanner_timestamp,
                5,
                None,
            );
            scanner.scan();
            assert_eq!(scanner.results.len(), 0);
            assert_eq!(scanner.found_intents.len(), 1);
            assert_eq!(
                scanner.found_intents[0].0,
                TxnIntent {
                    txn_meta: txn.read().unwrap().to_txn_metadata(),
                    key: str_to_key(key)
                }
            );
        }
    }

    #[cfg(test)]
    mod advance_to_next_key {
        use crate::{
            hlc::timestamp::Timestamp,
            storage::{
                mvcc::KVStore,
                mvcc_iterator::{IterOptions, MVCCIterator},
                mvcc_key::MVCCKey,
                mvcc_scanner::MVCCScanner,
                str_to_key,
            },
        };

        #[test]
        fn advances_to_next_key() {
            let kv_store = KVStore::new("./tmp/data");
            let first_key = "apple";
            let first_key_timestamp1 = Timestamp::new(2, 3);
            let first_key_timestamp2 = Timestamp::new(3, 0);
            kv_store
                .mvcc_put(str_to_key(first_key), Some(first_key_timestamp1), None, 12)
                .unwrap();
            kv_store
                .mvcc_put(str_to_key(first_key), Some(first_key_timestamp2), None, 13)
                .unwrap();

            let second_key = "banana";
            let second_key_timestamp = Timestamp::new(2, 3);
            kv_store
                .mvcc_put(str_to_key(second_key), Some(second_key_timestamp), None, 13)
                .unwrap();

            let iterator = MVCCIterator::new(&kv_store.storage, IterOptions { prefix: true });
            let scanner_timestamp = Timestamp {
                logical_time: 3,
                wall_time: 3,
            };
            let mut scanner = MVCCScanner::new(
                iterator,
                first_key.as_bytes().to_vec(),
                None,
                scanner_timestamp,
                5,
                None,
            );
            scanner.advance_to_next_key();
            assert_eq!(
                scanner.it.current_key(),
                MVCCKey {
                    key: second_key.as_bytes().to_vec(),
                    timestamp: second_key_timestamp
                }
            )
        }

        #[test]
        fn there_is_no_next_key() {
            let kv_store = KVStore::new("./tmp/data");
            let iterator = MVCCIterator::new(&kv_store.storage, IterOptions { prefix: true });
            let scanner_timestamp = Timestamp {
                logical_time: 3,
                wall_time: 3,
            };
            let mut scanner = MVCCScanner::new(
                iterator,
                "foo".as_bytes().to_vec(),
                None,
                scanner_timestamp,
                5,
                None,
            );
            scanner.advance_to_next_key();
        }
    }

    #[cfg(test)]
    mod scan {
        use uuid::Uuid;

        use crate::{
            hlc::timestamp::Timestamp,
            storage::{
                mvcc::KVStore,
                mvcc_iterator::{IterOptions, MVCCIterator},
                mvcc_key::MVCCKey,
                mvcc_scanner::MVCCScanner,
                serialized_to_value, str_to_key,
                txn::Txn,
            },
        };

        #[test]
        fn multiple_timestamps_for_same_keys() {
            let kv_store = KVStore::new("./tmp/data");

            let scan_timestamp = Timestamp::new(12, 3);

            let key1 = "apple";
            let first_key_timestamp1 = scan_timestamp.decrement_by(2);
            let first_key_timestamp2 = scan_timestamp.advance_by(3);
            kv_store
                .mvcc_put(str_to_key(key1), Some(first_key_timestamp1), None, 12)
                .unwrap();

            kv_store
                .mvcc_put(str_to_key(key1), Some(first_key_timestamp2), None, 13)
                .unwrap();

            let key2 = "banana";
            let second_key_timestamp1 = scan_timestamp.decrement_by(1);
            let second_key_timestamp2 = scan_timestamp.advance_by(10);
            kv_store
                .mvcc_put(str_to_key(key2), Some(second_key_timestamp1), None, 12)
                .unwrap();
            kv_store
                .mvcc_put(str_to_key(key2), Some(second_key_timestamp2), None, 13)
                .unwrap();

            let key3 = "cherry";
            let third_key_timestamp = scan_timestamp.decrement_by(10);
            kv_store
                .mvcc_put(str_to_key(key3), Some(third_key_timestamp), None, 12)
                .unwrap();

            let iterator = MVCCIterator::new(&kv_store.storage, IterOptions { prefix: true });
            let mut scanner = MVCCScanner::new(
                iterator,
                str_to_key(key1),
                Some(str_to_key(key2)),
                scan_timestamp,
                5,
                None,
            );
            scanner.scan();
            assert_eq!(scanner.results.len(), 2);
            let mut vec = Vec::new();
            vec.push((
                MVCCKey {
                    key: str_to_key(key1),
                    timestamp: first_key_timestamp1,
                },
                serialized_to_value(12),
            ));
            vec.push((
                MVCCKey {
                    key: str_to_key(key2),
                    timestamp: second_key_timestamp1,
                },
                serialized_to_value(12),
            ));

            assert_eq!(scanner.results, vec);
            assert_eq!(scanner.found_intents.len(), 0);
        }

        #[test]
        fn multiple_intents() {
            let kv_store = KVStore::new("./tmp/data");
            let txn_id = Uuid::new_v4();
            let transaction_timestamp = Timestamp::new(12, 0);
            let txn = Txn::new_link(txn_id, transaction_timestamp);
            let key1 = "apple";
            kv_store
                .mvcc_put(
                    str_to_key(key1),
                    Some(transaction_timestamp),
                    Some(txn.clone()),
                    12,
                )
                .unwrap();

            let key2 = "banana";
            kv_store
                .mvcc_put(
                    str_to_key(key2),
                    Some(transaction_timestamp),
                    Some(txn.clone()),
                    "world",
                )
                .unwrap();

            let key3 = "cherry";
            kv_store
                .mvcc_put(
                    str_to_key(key3),
                    Some(transaction_timestamp),
                    Some(txn.clone()),
                    "hello",
                )
                .unwrap();

            let iterator = MVCCIterator::new(&kv_store.storage, IterOptions { prefix: true });
            let scanner_timestamp = Timestamp {
                logical_time: 3,
                wall_time: 3,
            };
            let mut scanner = MVCCScanner::new(
                iterator,
                str_to_key(key1),
                Some(str_to_key(key2)),
                scanner_timestamp,
                5,
                None,
            );
            scanner.scan();
            assert_eq!(scanner.found_intents.len(), 2);
            let mut vec = Vec::new();

            vec.push((
                txn.read().unwrap().to_intent(str_to_key(key1)),
                serialized_to_value(12),
            ));
            vec.push((
                txn.read().unwrap().to_intent(str_to_key(key2)),
                serialized_to_value("world"),
            ));
            assert_eq!(scanner.found_intents, vec);
        }
    }
}
