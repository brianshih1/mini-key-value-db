use rand::distributions::{Alphanumeric, DistString};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{db::db::TxnLink, hlc::timestamp::Timestamp};

use super::{
    mvcc_iterator::{IterOptions, MVCCIterator},
    mvcc_key::{create_intent_key, decode_mvcc_key, MVCCKey},
    mvcc_scanner::MVCCScanner,
    storage::Storage,
    txn::{TransactionStatus, TxnIntent, TxnMetadata, TxnRecord, UncommittedValue},
    Key, Value,
};

pub struct KVStore {
    pub storage: Storage,
}

pub struct MVCCScanParams {
    max_result_count: usize,
    transaction: Option<TxnLink>,
}

pub struct MVCCGetParams {
    pub transaction: Option<TxnLink>,
}

#[derive(Debug, Eq, PartialEq, PartialOrd, Ord, Clone, Serialize, Deserialize)]
pub struct WriteIntentError {
    pub intent: TxnIntent,
}

pub struct MVCCGetResult {
    pub value: Option<(MVCCKey, Value)>,
    pub intent: Option<(TxnIntent, Value)>,
}

pub struct MVCCScanResult {
    results: Vec<(MVCCKey, Value)>,
    intents: Vec<(TxnIntent, Value)>,
}

pub fn serialize<T: Serialize>(value: T) -> Value {
    let str = serde_json::to_string(&value).unwrap();
    str.into_bytes()
}

impl KVStore {
    // path example: "./tmp/data";
    pub fn new_cleaned(path: &str) -> Self {
        KVStore {
            storage: Storage::new_cleaned(path),
        }
    }

    pub fn new_random_path() -> Self {
        let string = Alphanumeric.sample_string(&mut rand::thread_rng(), 16);

        KVStore::new_cleaned(&format!("./tmp/{}", string))
    }

    pub fn new(path: &str) -> Self {
        KVStore {
            storage: Storage::new(path),
        }
    }

    /**
     * mvcc_get returns the most recent value less than the timestamp provided for the key.
     * If it runs into an uncommited value, it returns a WriteIntentError
     */
    pub fn mvcc_get<'a>(
        &self,
        key: &'a Key,
        timestamp: Timestamp,
        params: MVCCGetParams,
    ) -> MVCCGetResult {
        // implement mvcc_get as a scan with 1 element max and start_key = end_key
        let scan_params = MVCCScanParams {
            max_result_count: 1,
            transaction: params.transaction,
        };
        let res = self.mvcc_scan(key.to_owned(), key.to_owned(), timestamp, scan_params);
        let results = res.results;
        let intents = res.intents;
        MVCCGetResult {
            value: if results.len() > 0 {
                Some(results.first().unwrap().to_owned())
            } else {
                None
            },
            intent: if intents.len() > 0 {
                Some(intents.first().unwrap().clone())
            } else {
                None
            },
        }
    }

    pub fn mvcc_delete(&self, key: MVCCKey) {
        self.storage.delete_mvcc(&key);
    }

    pub fn mvcc_scan(
        &self,
        start_key: Key,
        end_key: Key,
        timestamp: Timestamp,
        scan_params: MVCCScanParams,
    ) -> MVCCScanResult {
        let iterator = MVCCIterator::new(&self.storage, IterOptions { prefix: true });
        let mut scanner = MVCCScanner::new(
            iterator,
            start_key,
            Some(end_key),
            timestamp.clone(),
            scan_params.max_result_count,
            scan_params.transaction,
        );
        scanner.scan();
        MVCCScanResult {
            results: scanner.results,
            intents: scanner.found_intents,
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
        &self,
        key: Key,
        timestamp: Option<Timestamp>,
        txn: Option<TxnLink>,
        value: T,
    ) -> Result<MVCCKey, WriteIntentError> {
        self.mvcc_put_raw(key, timestamp, txn, serialize(value))
    }

    pub fn mvcc_put_raw(
        &self,
        key: Key,
        timestamp: Option<Timestamp>,
        txn: Option<TxnLink>,
        value: Value,
    ) -> Result<MVCCKey, WriteIntentError> {
        let intent = self.mvcc_get_uncommited_value(&key);

        match intent {
            Some((metadata, transaction_record)) => match &txn {
                Some(put_txn) => {
                    // this means we're overwriting our own transaction
                    // TODO: epoch - transaction retries
                    if metadata.txn_id == put_txn.read().unwrap().txn_id {
                    } else {
                        match transaction_record.status {
                            TransactionStatus::PENDING => {
                                return Err(WriteIntentError {
                                    intent: TxnIntent {
                                        txn_meta: metadata,
                                        key,
                                    },
                                })
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

        let (_, write_timestamp) = match txn {
            Some(ref transaction) => (
                transaction.read().unwrap().read_timestamp,
                // TODO: reading multiple times is inefficient, move to helper function
                transaction.read().unwrap().write_timestamp,
            ),
            None => (timestamp.unwrap(), timestamp.unwrap()),
        };

        let version_key = MVCCKey::new(key.clone(), write_timestamp);

        if let Some(ref transaction) = txn {
            self.storage
                .put_serialized_with_mvcc_key(
                    &create_intent_key(&key),
                    UncommittedValue {
                        value: value.clone(), // is this correct?
                        txn_metadata: TxnMetadata {
                            txn_id: transaction.read().unwrap().txn_id,
                            write_timestamp: write_timestamp,
                        },
                    },
                )
                .unwrap();
        } else {
            self.storage
                .put_raw_with_mvcc_key(&version_key, value.clone())
                .unwrap()
        }

        Ok(version_key)
    }

    pub fn mvcc_get_uncommited_value(&self, key: &Key) -> Option<(TxnMetadata, TxnRecord)> {
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
                let transaction_id = metadata.txn_id;
                let transaction_record = self.get_transaction_record(transaction_id).unwrap();
                return Some((metadata, transaction_record));
            }
        }
        None
    }

    pub fn create_pending_transaction_record(
        &self,
        txn_id: Uuid,
        write_timestamp: Timestamp,
    ) -> () {
        let record = TxnRecord {
            status: TransactionStatus::PENDING,
            metadata: TxnMetadata {
                txn_id: txn_id.clone(),
                write_timestamp: write_timestamp,
            },
        };
        self.put_transaction_record(txn_id, &record)
    }

    pub fn get_transaction_record(&self, transaction_id: Uuid) -> Option<TxnRecord> {
        self.storage.get_transaction_record(transaction_id)
    }

    // This can be used to create or overwrite transaction record.
    pub fn put_transaction_record(&self, transaction_id: Uuid, record: &TxnRecord) {
        self.storage
            .put_transaction_record(&transaction_id, record)
            .unwrap();
    }

    pub fn commit_transaction_record(&self, transaction_id: Uuid, write_timestamp: Timestamp) {
        let record = TxnRecord {
            status: TransactionStatus::COMMITTED,
            metadata: TxnMetadata {
                txn_id: transaction_id.clone(),
                write_timestamp: write_timestamp,
            },
        };
        self.put_transaction_record(transaction_id, &record)
    }

    // TODO: Do we really need a write_timestamp here?
    pub fn update_transaction_record_to_abort(
        &self,
        transaction_id: Uuid,
        write_timestamp: Timestamp,
    ) {
        // TODO: Delete all uncomitted intents from DB
        self.put_transaction_record(
            transaction_id,
            &TxnRecord {
                status: TransactionStatus::ABORTED,
                metadata: TxnMetadata {
                    txn_id: transaction_id.clone(),
                    write_timestamp: write_timestamp,
                },
            },
        )
    }

    pub fn get_uncommitted_value(&self, key: &MVCCKey) -> Option<UncommittedValue> {
        self.storage
            .get_serialized_with_mvcc_key::<UncommittedValue>(key)
            .unwrap()
    }

    pub fn mvcc_resolve_intent(&self, key: Key, commit_timestamp: Timestamp, txn_id: Uuid) {
        let intent_key = MVCCKey::create_intent_key(&key);

        if let Some(uncommitted_value) = self.get_uncommitted_value(&intent_key) {
            if uncommitted_value.txn_metadata.txn_id != txn_id {
                println!("MVCC intent owned by another transaction. Failed to resolve");
                return;
            }
            self.storage.delete_mvcc(&intent_key);
            let value = uncommitted_value.value;

            self.storage
                .put_raw_with_mvcc_key(&MVCCKey::new(key, commit_timestamp), value)
                .unwrap();
        }
    }

    // Debugger method to help collect all MVCCKey-Value pairs
    pub fn collect_all_mvcc_kvs(&self) -> Vec<MVCCKey> {
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
