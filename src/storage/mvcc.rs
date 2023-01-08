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
            Some((intent, transaction_record)) => match txn {
                Some(put_txn) => {
                    // this means we're overwriting our own transaction
                    // TODO: epoch - transaction retries
                    if intent.transaction_id == put_txn.transaction_id {
                    } else {
                        match transaction_record.status {
                            TransactionStatus::PENDING => {
                                return Err(StorageError::new(
                                    WRITE_INTENT_ERROR,
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
        let mut read_timestamp = timestamp;
        let mut write_timestamp = timestamp;
        if txn.is_some() {
            let txn = txn.unwrap();
            // should we ensure read_timestamp = timestamp?
            read_timestamp = &txn.read_timestamp;
            write_timestamp = &txn.metadata.write_timestamp;
        }

        let version_key = MVCCKey::new(key, write_timestamp.to_owned());
        self.storage.put_mvcc_serialized(&version_key, value);
        todo!()
    }

    pub fn mvcc_get_intent(
        &mut self,
        key: &Key,
    ) -> Option<(TransactionMetadata, TransactionRecord)> {
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

    pub fn get_transaction_record(&mut self, transaction_id: &Uuid) -> TransactionRecord {
        self.storage
            .get_serialized::<TransactionRecord>(&transaction_id.to_string())
            .unwrap()
    }
}

#[cfg(test)]
mod tests {
    use serde::{Deserialize, Serialize};

    use super::TransactionMetadata;

    #[derive(Debug, Serialize, Deserialize)]
    struct Test {
        foo: bool,
    }

    #[test]
    fn test() {
        let mvcc = Test { foo: true };
        let str = serde_json::to_string(&mvcc).unwrap();
        let meta = serde_json::from_str::<Test>(&str).unwrap();
        let huh = "";

        let test_bool = true;
        let bool_str = serde_json::to_string(&test_bool).unwrap();
        let meta = serde_json::from_str::<bool>(&bool_str).unwrap();
        // println!("value: {:?}", meta);

        let vec = serde_json::to_vec(&test_bool).unwrap();
        let back = serde_json::from_slice::<bool>(&vec).unwrap();
        // println!("value: {:?}", back);

        let str = "foo";
        let vec = serde_json::to_vec(&str).unwrap();
        let back = serde_json::from_slice::<&str>(&vec).unwrap();
        println!("value: {:?}", back);
        // serde_json::from_slice(value)
    }
}
