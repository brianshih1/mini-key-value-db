use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{hlc::timestamp::Timestamp, StorageError, StorageResult, WRITE_INTENT_ERROR};

use super::{
    mvcc_iterator::IterOptions,
    mvcc_key::{create_intent_key, MVCCKey},
    storage::Storage,
    txn::{Transaction, TransactionMetadata, TransactionRecord, TransactionStatus},
    Key, Value,
};

struct KVStore {
    storage: Storage,
}

impl KVStore {
    // path example: "./tmp/data";
    pub fn new(path: &str) -> Self {
        KVStore {
            storage: Storage::new_cleaned(path),
        }
    }

    pub fn mvcc_get(&self) {}

    /**
     * Returns an error if failed. Otherwise, return the MVCCKey of the value stored
     *
     * When transaction is provided, it will dictate the timestamp, not the timestamp parameter.
     * The intent will be written by txn.metadata.writeTimestamp. Reads are performed at txn.readTimestamp.
     */
    pub fn mvcc_put(
        &mut self,
        key: &str,
        timestamp: &Timestamp,
        txn: Option<Transaction>,
        value: Value,
    ) -> StorageResult<MVCCKey> {
        let intent = self.mvcc_get_intent(&key.as_bytes().to_vec());

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
            None => (timestamp.to_owned(), timestamp.to_owned()),
        };

        let version_key = MVCCKey::new(key, write_timestamp.to_owned());
        self.storage.put_mvcc_serialized(&version_key, value);
        Ok(version_key)
    }

    pub fn mvcc_get_intent(&self, key: &Key) -> Option<(TransactionMetadata, TransactionRecord)> {
        let options = IterOptions { prefix: true };
        let mut it = self.storage.new_iterator(options);
        let intent_key = create_intent_key(key);
        // TODO: range keys - if there are no range keys we could probably have
        // just done storage.get(...)
        let seek_res = it.seek_ge(&intent_key);
        if seek_res {
            let curr_key = it.current_key();
            if curr_key.is_intent_key() && &curr_key.key == key {
                let metadata = it.current_value_serialized::<TransactionMetadata>();
                let transaction_id = metadata.transaction_id;
                let transaction_record = self.get_transaction_record(&transaction_id);
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

    pub fn get_transaction_record(&self, transaction_id: &Uuid) -> TransactionRecord {
        self.storage
            .get_serialized::<TransactionRecord>(&transaction_id.to_string())
            .unwrap()
    }

    // This can be used to create or overwrite transaction record.
    pub fn put_transaction_record(&mut self, transaction_id: &Uuid, record: TransactionRecord) {
        self.storage
            .put_serialized(&transaction_id.to_string(), record);
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
        let transaction_record = kv_store.get_transaction_record(&transaction_id);
        assert_eq!(
            transaction_record,
            TransactionRecord {
                status: TransactionStatus::PENDING
            }
        )
    }

    mod mvcc_put {
        use uuid::Uuid;

        #[test]
        fn write_intent_error() {
            let txn1_id = Uuid::new_v4();
            // let txn =
        }
    }
}
