use std::sync::{Arc, RwLock};

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{db::db::TxnLink, hlc::timestamp::Timestamp};

use super::{Key, Value};

#[derive(Serialize, Deserialize, Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct TxnMetadata {
    pub txn_id: Uuid,
    pub write_timestamp: Timestamp,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct TxnIntent {
    pub txn_meta: TxnMetadata,
    pub key: Key,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct UncommittedValue {
    pub value: Value,
    pub txn_metadata: TxnMetadata,
}

#[derive(Debug)]
pub struct Txn {
    pub txn_id: Uuid,
    pub metadata: TxnMetadata,
    // All reads are performed on this read_timestamp
    // Writes are performed on metadata.write_timestamp.
    // If the write runs into timestamp oracle, then the write timestamp will be bumped.
    pub read_timestamp: Timestamp,

    /**
     * List of keys that the tranaction has acquired locks.
     * Theses should be resolved when the txn finalizes.
     */
    pub lock_spans: RwLock<Vec<Key>>,
}

impl Txn {
    pub fn new(transaction_id: Uuid, timestamp: Timestamp) -> Self {
        Txn {
            txn_id: transaction_id,
            metadata: TxnMetadata {
                txn_id: transaction_id.to_owned(),
                write_timestamp: timestamp,
            },
            read_timestamp: timestamp,
            lock_spans: RwLock::new(Vec::new()),
        }
    }

    pub fn new_link(transaction_id: Uuid, timestamp: Timestamp) -> TxnLink {
        Arc::new(RwLock::new(Txn::new(transaction_id, timestamp)))
    }

    pub fn to_intent(&self, key: Key) -> TxnIntent {
        TxnIntent {
            txn_meta: self.metadata.clone(),
            key,
        }
    }

    pub fn append_lock_span(&self, key: Key) {
        let mut lock_spans = self.lock_spans.write().unwrap();
        lock_spans.push(key);
    }
}

#[derive(Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Debug)]
// What's stored in the database
pub struct TxnRecord {
    pub status: TransactionStatus,
    pub metadata: TxnMetadata,
}

#[derive(Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Debug)]
pub enum TransactionStatus {
    PENDING,
    COMMITTED,
    ABORTED,
}

impl TxnIntent {
    pub fn new(txn_id: Uuid, write_timestamp: Timestamp, key: Key) -> Self {
        TxnIntent {
            txn_meta: TxnMetadata {
                txn_id: txn_id,
                write_timestamp: write_timestamp,
            },
            key: key,
        }
    }
}
