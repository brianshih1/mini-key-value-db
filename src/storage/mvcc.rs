use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{hlc::timestamp::Timestamp, StorageError, StorageResult, WRITE_INTENT_ERROR};

use super::{
    mvcc_iterator::{IterOptions, MVCCIterator},
    mvcc_key::{create_intent_key, decode_mvcc_key, encode_mvcc_key, MVCCKey},
    mvcc_scanner::MVCCScanner,
    serialized_to_value,
    storage::Storage,
    str_to_key,
    txn::{
        Transaction, TransactionMetadata, TransactionRecord, TransactionStatus, UncommittedValue,
    },
    Key, Value,
};

pub struct KVStore {
    pub storage: Storage,
}

pub struct MVCCScanParams {
    max_result_count: usize,
    transaction: Option<Transaction>,
}

pub struct MVCCGetParams {
    pub transaction: Option<Transaction>,
}

#[derive(Debug, Eq, PartialEq, PartialOrd, Ord, Clone, Serialize, Deserialize)]
pub struct WriteIntentError {
    pub intents: Vec<(Key, TransactionMetadata)>,
}

impl KVStore {
    // path example: "./tmp/data";
    pub fn new(path: &str) -> Self {
        KVStore {
            storage: Storage::new_cleaned(path),
        }
    }

    /**
     * mvcc_get returns the most recent value less than the timestamp provided for the key.
     * If it runs into an uncommited value, it returns a WriteIntentError
     */
    pub fn mvcc_get(
        &self,
        key: Key,
        timestamp: Timestamp,
        params: MVCCGetParams,
    ) -> Result<Option<(MVCCKey, Value)>, WriteIntentError> {
        // implement mvcc_get as a scan with 1 element max and start_key = end_key
        let scan_params = MVCCScanParams {
            max_result_count: 1,
            transaction: params.transaction,
        };
        let res = self.mvcc_scan(key.to_owned(), key.to_owned(), timestamp, scan_params);
        match res {
            Ok(vec) => {
                if vec.len() == 0 {
                    return Ok(None);
                }
                let kv = vec.get(0).unwrap().to_owned();
                return Ok(Some(kv));
            }
            Err(intent_error) => {
                return Err(intent_error);
            }
        }
    }

    pub fn mvcc_scan(
        &self,
        start_key: Key,
        end_key: Key,
        timestamp: Timestamp,
        scan_params: MVCCScanParams,
    ) -> Result<Vec<(MVCCKey, Value)>, WriteIntentError> {
        let iterator = MVCCIterator::new(&self.storage, IterOptions { prefix: true });
        let mut scanner = MVCCScanner::new(
            iterator,
            start_key,
            Some(end_key),
            timestamp,
            scan_params.max_result_count,
            scan_params.transaction,
        );
        scanner.scan();
        if scanner.found_intents.len() > 0 {
            return Err(WriteIntentError {
                intents: scanner.found_intents,
            });
        } else {
            return Ok(scanner.results);
        }
    }

    /**
     * MVCCPut puts a new timestamped value for the key/value, as well as an
     * intent if a transaction is provided.
     *
     * Before writing, MVCCPut must verify that there are no uncommitted intent
     * for the same key.
     *
     * This function returns an error if failed. Otherwise, return the MVCCKey of the value stored
     *
     * When transaction is provided, it will dictate the timestamp, not the timestamp parameter.
     * The intent will be written by txn.metadata.writeTimestamp. Reads are performed at txn.readTimestamp.
     */
    pub fn mvcc_put<T: Serialize>(
        &mut self,
        key: &str,
        timestamp: Option<Timestamp>,
        txn: Option<&Transaction>,
        value: T,
    ) -> StorageResult<MVCCKey> {
        let intent = self.mvcc_get_uncommited_value(&key.as_bytes().to_vec());

        match intent {
            Some((intent, transaction_record)) => match &txn {
                Some(put_txn) => {
                    // this means we're overwriting our own transaction
                    // TODO: epoch - transaction retries
                    if intent.transaction_id == put_txn.transaction_id {
                    } else {
                        match transaction_record.status {
                            TransactionStatus::PENDING => {
                                return Err(StorageError::new(
                                    WRITE_INTENT_ERROR.to_owned(),
                                    "found pending transaction".to_owned(),
                                ))
                            }
                            TransactionStatus::COMMITTED => todo!(),
                            TransactionStatus::ABORTED => todo!(), // clean up
                        }
                    }
                }
                None => {}
            },
            None => {}
        }

        // TODO: Storage::new_iterator(...)

        let (read_timestamp, write_timestamp) = match txn {
            Some(transaction) => (
                transaction.read_timestamp,
                transaction.metadata.write_timestamp,
            ),
            None => (timestamp.unwrap().to_owned(), timestamp.unwrap().to_owned()),
        };

        let version_key = MVCCKey::new(key, write_timestamp.to_owned());

        if let Some(transaction) = txn {
            self.storage
                .put_serialized_with_mvcc_key(
                    &create_intent_key(&str_to_key(key)),
                    UncommittedValue {
                        value: serialized_to_value(value), // is this correct?
                        txn_metadata: TransactionMetadata {
                            transaction_id: transaction.transaction_id,
                            write_timestamp: write_timestamp.to_owned(),
                        },
                    },
                )
                .unwrap();
        } else {
            self.storage
                .put_serialized_with_mvcc_key(&version_key, value)
                .unwrap()
        }

        Ok(version_key)
    }

    pub fn mvcc_get_uncommited_value(
        &self,
        key: &Key,
    ) -> Option<(TransactionMetadata, TransactionRecord)> {
        let options = IterOptions { prefix: true };
        let mut it = self.storage.new_mvcc_iterator(options);
        let intent_key = create_intent_key(key);
        // TODO: range keys - if there are no range keys we could probably have
        // just done storage.get(...)
        let seek_res = it.seek_ge(&intent_key);
        if seek_res {
            let curr_key = it.current_key();
            if curr_key.is_intent_key() && &curr_key.key == key {
                let metadata = it
                    .current_value_serialized::<UncommittedValue>()
                    .txn_metadata;
                let transaction_id = metadata.transaction_id;
                let transaction_record = self.get_transaction_record(&transaction_id).unwrap();
                return Some((metadata, transaction_record));
            }
        }
        None
    }

    pub fn create_pending_transaction_record(&mut self, transaction_id: &Uuid) -> () {
        self.put_transaction_record(
            transaction_id,
            TransactionRecord {
                status: TransactionStatus::PENDING,
            },
        )
    }

    pub fn get_transaction_record(&self, transaction_id: &Uuid) -> Option<TransactionRecord> {
        self.storage.get_transaction_record(transaction_id)
    }

    // This can be used to create or overwrite transaction record.
    pub fn put_transaction_record(&mut self, transaction_id: &Uuid, record: TransactionRecord) {
        self.storage
            .put_transaction_record(&transaction_id, record)
            .unwrap();
    }

    // Debugger method to help collect all MVCCKey-Value pairs
    pub fn collect_all_mvcc_kvs(&mut self) -> Vec<MVCCKey> {
        let mut vec = Vec::new();
        let mut it = MVCCIterator::new(&self.storage, IterOptions { prefix: false });
        loop {
            if it.valid() {
                let raw_key = it.current_raw_key();
                let curr_key = decode_mvcc_key(&Vec::from(raw_key.as_ref()));
                match curr_key {
                    Some(key) => vec.push(key),
                    None => {}
                }
            } else {
                break;
            }
            it.next();
        }
        vec
    }
}

#[cfg(test)]
mod tests {
    use uuid::Uuid;

    use crate::storage::txn::{TransactionRecord, TransactionStatus};

    use super::KVStore;

    #[test]
    fn create_pending_transaction_record() -> () {
        let mut kv_store = KVStore::new("./tmp/data");
        let transaction_id = Uuid::new_v4();

        kv_store.create_pending_transaction_record(&transaction_id);
        let transaction_record = kv_store.get_transaction_record(&transaction_id).unwrap();
        assert_eq!(
            transaction_record,
            TransactionRecord {
                status: TransactionStatus::PENDING
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
                txn::{Transaction, TransactionMetadata},
            },
            WRITE_INTENT_ERROR,
        };

        #[test]
        fn put_with_transaction() {
            let mut kv_store = KVStore::new("./tmp/data");
            let key = "foo";
            let txn1_id = Uuid::new_v4();

            let timestamp = Timestamp {
                wall_time: 10,
                logical_time: 12,
            };
            let transaction = Transaction::new(txn1_id, timestamp.to_owned(), timestamp.to_owned());
            kv_store
                .mvcc_put(key, None, Some(&transaction), 12)
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
            let mut kv_store = KVStore::new("./tmp/data");
            let key = "foo";
            let txn1_id = Uuid::new_v4();

            let timestamp = Timestamp {
                wall_time: 10,
                logical_time: 12,
            };
            let transaction = Transaction::new(txn1_id, timestamp.to_owned(), timestamp.to_owned());

            kv_store.create_pending_transaction_record(&txn1_id);
            let current_keys = kv_store.collect_all_mvcc_kvs();

            kv_store
                .mvcc_put(key, None, Some(&transaction), 12)
                .unwrap();

            let txn2_id = Uuid::new_v4();

            let second_transaction = Transaction::new(
                txn2_id,
                Timestamp {
                    wall_time: 12,
                    logical_time: 14,
                },
                Timestamp {
                    wall_time: 12,
                    logical_time: 14,
                },
            );

            let res = kv_store.mvcc_put(key, None, Some(&second_transaction), 12);

            let err = res.map_err(|e| e.message_id);
            assert_eq!(err, Err(WRITE_INTENT_ERROR.to_owned()));
        }
    }

    mod mvcc_get {
        use crate::{
            hlc::timestamp::Timestamp,
            storage::{
                mvcc::{KVStore, MVCCGetParams},
                mvcc_key::MVCCKey,
                serialized_to_value, str_to_key, Value,
            },
        };

        #[test]
        fn get_key_with_multiple_timestamps() {
            let mut kv_store = KVStore::new("./tmp/data");
            let read_timestamp = Timestamp::new(10, 10);

            let key1 = "apple";
            let key1_timestamp1 = read_timestamp.decrement_by(3); // 7
            let key1_timestamp2 = read_timestamp.decrement_by(1); // 9
            let key1_timestamp3 = read_timestamp.advance_by(2); // 12

            kv_store
                .storage
                .put_serialized_with_mvcc_key(&MVCCKey::new(&key1, key1_timestamp1), 10)
                .unwrap();

            let most_recent_key = MVCCKey::new(&key1, key1_timestamp2);
            let most_recent_value = 11;
            kv_store
                .storage
                .put_serialized_with_mvcc_key(&most_recent_key.to_owned(), most_recent_value)
                .unwrap();

            kv_store
                .storage
                .put_serialized_with_mvcc_key(&MVCCKey::new(&key1, key1_timestamp3), 12)
                .unwrap();

            let key2 = "banana";
            let key2_timestamp = read_timestamp.decrement_by(1);
            kv_store
                .storage
                .put_serialized_with_mvcc_key(&MVCCKey::new(&key2, key2_timestamp), 10)
                .unwrap();

            let res = kv_store.mvcc_get(
                str_to_key(key1),
                read_timestamp,
                MVCCGetParams { transaction: None },
            );
            assert_eq!(
                res,
                Ok(Some((
                    most_recent_key.to_owned(),
                    serialized_to_value(most_recent_value)
                )))
            );
        }
    }
}
