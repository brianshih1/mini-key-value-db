use rocksdb::DBIterator;

use crate::hlc::timestamp::Timestamp;

use super::{
    mvcc_iterator::MVCCIterator,
    mvcc_key::{create_intent_key, MVCCKey},
    txn::{Txn, TxnIntent, TxnMetadata, UncommittedValue},
    Key, Value,
};

pub struct MVCCScanner<'a> {
    it: MVCCIterator<'a>,

    pub transaction: Option<&'a Txn>,

    // TODO: lockTable

    // start of scan (doesn't contain MVCC timestamp)
    pub start_key: Key,

    // end of the scan (doesn't contain MVCC timestamp)
    pub end_key: Option<Key>,

    // Timestamp that MVCCScan/MVCCGet was called
    pub timestamp: Timestamp,

    pub found_intents: Vec<TxnIntent>,

    // max number of tuples to add to the results
    pub max_records_count: usize,

    /**
     * CockroachDB stores it as: <valueLen:Uint32><keyLen:Uint32><Key><Value>
     * https://github.com/cockroachdb/cockroach/blob/c21c90f93219b857858518d25a8bc061444d573c/pkg/storage/pebble_mvcc_scanner.go#L148
     *
     * For now, I'll just store a vec of KV tuples (unoptimized version for the MVP)
     */
    pub results: Vec<(MVCCKey, Value)>,
    // TODO: failOnMoreRecent if we want to allow things like locked scans. But not for now.
}

impl<'a> MVCCScanner<'a> {
    pub fn new(
        it: MVCCIterator<'a>,
        start_key: Key,
        end_key: Option<Key>,
        timestamp: Timestamp,
        max_records_count: usize,
        transaction: Option<&'a Txn>,
    ) -> Self {
        MVCCScanner {
            it,
            start_key: start_key,
            end_key: end_key,
            timestamp,
            found_intents: Vec::new(),
            results: Vec::new(),
            max_records_count,
            transaction,
        }
    }

    pub fn scan(&mut self) -> () {
        // intent key will always be sorted before other MVCC keys
        let start_base = create_intent_key(&self.start_key);
        self.it.seek_ge(&start_base);
        loop {
            if self.results.len() == self.max_records_count {
                return;
            }
            if !self.it.valid() {
                return;
            }
            match &self.end_key {
                Some(end_key) => {
                    if &self.it.current_key().key > end_key {
                        return;
                    }
                }
                None => {
                    // if there is no end_key, then the end_key defaults to start_key
                    if self.it.current_key().key > self.start_key {
                        return;
                    }
                }
            }
            self.get_current_key();
            self.advance_to_next_key();
            // advance to next one
        }
    }

    /**
     * Gets the most recent key that is less than the read_timestamp and adds it to the result set.
     *
     * If there is a transaction and it notices an intent, it adds the intent.
     *
     * Attempts to add the current key to the result set. If it notices an intent,
     * it adds the intent. This function is not responsible for checking the start and end key or advances.
     * It just tries to add the current key to the result set.
     *
     * Returns whether a record was added to the result set for the current key
     *
     */
    pub fn get_current_key(&mut self) -> bool {
        let current_key = self.it.current_key();
        if current_key.is_intent_key() {
            let current_value = self.it.current_value_serialized::<UncommittedValue>();

            if let Some(scanner_transaction) = self.transaction {
                if current_value.txn_metadata.txn_id == scanner_transaction.txn_id {
                    // TODO: Resolve based on epoch
                } else {
                    self.found_intents.push(TxnIntent {
                        txn_meta: current_value.txn_metadata,
                        key: current_key.key.clone(),
                    });
                }
            } else {
                self.found_intents.push(TxnIntent {
                    txn_meta: current_value.txn_metadata,
                    key: current_key.key.clone(),
                });
            }

            return false;
        } else {
            let key_timestamp = current_key.timestamp;

            if self.timestamp > key_timestamp {
                // the scanner's timestamp is greater, so just add
                self.results
                    .push((self.it.current_key(), self.it.current_value()));
                return true;
            } else if self.timestamp < key_timestamp {
                // seek to older version
                return self.seek_older_version(current_key.key.to_owned(), self.timestamp);
            } else {
                // the scanner's timestamp is sufficient (equal), so just add
                self.results
                    .push((self.it.current_key(), self.it.current_value()));
                return true;
            }
        }
    }

    /**
     * Try to add the key <= the provided timestamp and add it to the result set.
     * Return true if added.
     */
    fn seek_older_version(&mut self, key: Key, timestamp: Timestamp) -> bool {
        let mvcc_key = MVCCKey {
            key: key.to_owned(),
            timestamp,
        };
        let is_valid = self.it.seek_ge(&mvcc_key);
        if is_valid {
            let current_key = self.it.current_key();
            if current_key.key == key {
                self.results.push((current_key, self.it.current_value()));
                return true;
            } else {
                return false;
            }
        } else {
            return false;
        }
    }

    pub fn advance_to_next_key(&mut self) -> () {
        if !self.it.valid() {
            return;
        }
        let current_key = self.it.current_key();

        loop {
            self.it.next();
            if !self.it.valid() {
                return;
            }
            let next_key = self.it.current_key();
            if current_key.key != next_key.key {
                break;
            } else {
                continue;
            }
        }
    }
}

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

        use super::scan;

        #[test]
        fn no_intent_and_no_end_key() {
            let mut storage = Storage::new_cleaned("./tmp/test");
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
            let mut kv_store = KVStore::new("./tmp/data");
            let timestamp = Timestamp::new(12, 0);
            let txn_id = Uuid::new_v4();
            let transaction = Txn::new(txn_id, timestamp.to_owned(), timestamp.to_owned());
            let key = "foo";
            kv_store
                .mvcc_put(str_to_key(key), Some(timestamp), Some(&transaction), 12)
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
                scanner.found_intents[0],
                TxnIntent {
                    txn_meta: transaction.metadata,
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
            let mut kv_store = KVStore::new("./tmp/data");
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
                boxed_byte_to_byte_vec,
                mvcc::KVStore,
                mvcc_iterator::{IterOptions, MVCCIterator},
                mvcc_key::MVCCKey,
                mvcc_scanner::MVCCScanner,
                serialized_to_value, str_to_key,
                txn::{Txn, TxnIntent},
            },
        };

        #[test]
        fn multiple_timestamps_for_same_keys() {
            let mut kv_store = KVStore::new("./tmp/data");

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
            let first = vec.get(0);
            let (k, v) = scanner.results.get(0).unwrap();
            assert_eq!(scanner.results, vec);
            assert_eq!(scanner.found_intents.len(), 0);
        }

        #[test]
        fn multiple_intents() {
            let mut kv_store = KVStore::new("./tmp/data");
            let txn_id = Uuid::new_v4();
            let transaction_timestamp = Timestamp::new(12, 0);
            let transaction = Txn::new(
                txn_id,
                transaction_timestamp.to_owned(),
                transaction_timestamp.to_owned(),
            );
            let key1 = "apple";
            kv_store
                .mvcc_put(
                    str_to_key(key1),
                    Some(transaction_timestamp),
                    Some(&transaction),
                    12,
                )
                .unwrap();

            let key2 = "banana";
            kv_store
                .mvcc_put(
                    str_to_key(key2),
                    Some(transaction_timestamp),
                    Some(&transaction),
                    "world",
                )
                .unwrap();

            let key3 = "cherry";
            kv_store
                .mvcc_put(
                    str_to_key(key3),
                    Some(transaction_timestamp),
                    Some(&transaction),
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
            TxnIntent {
                txn_meta: transaction.metadata,
                key: str_to_key(key1),
            };
            vec.push(TxnIntent {
                txn_meta: transaction.metadata,
                key: str_to_key(key1),
            });
            vec.push(TxnIntent {
                txn_meta: transaction.metadata,
                key: str_to_key(key2),
            });
            assert_eq!(scanner.found_intents, vec);
        }
    }
}
