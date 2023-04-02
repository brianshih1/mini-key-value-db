use std::{collections::HashMap, default};

use uuid::Uuid;

use crate::{
    db::db::TxnLink,
    hlc::timestamp::Timestamp,
    latch_manager::latch_interval_btree::{NodeKey, Range},
    storage::{
        mvcc::{KVStore, MVCCGetParams, WriteIntentError},
        mvcc_key::MVCCKey,
        str_to_key,
        txn::{Txn, TxnIntent, TxnMetadata},
        Key, Value,
    },
    StorageResult,
};

/**
 * Request is the input to SequenceReq. The struct contains all the information necessary
 * to sequence a request (to provide concurrent isolation for storage)
 */
pub struct Request {
    pub metadata: RequestMetadata,
    pub request_union: RequestUnion,
}

pub type ExecuteResult = Result<ResponseUnion, ExecuteError>;

pub enum ExecuteError {
    WriteIntentError(WriteIntentErrorData),
}

pub struct WriteIntentErrorData {
    intent: TxnIntent,
}

pub enum ResponseUnion {
    BeginTransaction(BeginTxnResponse),
    CommitTxn(CommitTxnResponse),
    AbortTxn(AbortTxnResponse),
    Get(GetResponse),
    Put(PutResponse),
}

pub enum RequestUnion {
    BeginTxn(BeginTxnRequest),
    CommitTxn(CommitTxnRequest),
    AbortTxn(AbortTxnRequest),
    Get(GetRequest),
    Put(PutRequest),
    // TODO: ConditionalPut
    // TODO: Scan
}

// TODO: Does this need a timestamp in there?
pub type SpanSet<K: NodeKey> = Vec<Range<K>>;

#[derive(Debug, Clone)]
pub struct SpansToAcquire {
    pub latch_spans: SpanSet<Key>,
    pub lock_spans: SpanSet<Key>,
}

pub trait Command {
    fn is_read_only(&self) -> bool;

    fn collect_spans(&self) -> SpanSet<Key>;

    fn execute(&self, header: &RequestMetadata, writer: &KVStore) -> ExecuteResult;
}

// Similar to CRDB's RequestHeader
pub struct RequestMetadata {
    /**
     * For now, assume every request is part of a transaction.
     * In the future, we can support non-transactional reads.
     */
    pub txn: TxnLink,
}

pub struct BeginTxnRequest {
    pub txn_id: Uuid,
}

pub struct BeginTxnResponse {}

impl Command for BeginTxnRequest {
    fn is_read_only(&self) -> bool {
        true
    }

    fn collect_spans(&self) -> SpanSet<Key> {
        Vec::new()
    }

    fn execute(&self, header: &RequestMetadata, writer: &KVStore) -> ExecuteResult {
        let txn = header.txn.read().unwrap();
        let write_timestamp = txn.write_timestamp;
        writer.create_pending_transaction_record(&self.txn_id, write_timestamp);
        Ok(ResponseUnion::BeginTransaction(BeginTxnResponse {}))
    }
}

pub struct AbortTxnRequest {}

pub struct AbortTxnResponse {}

impl Command for AbortTxnRequest {
    fn is_read_only(&self) -> bool {
        todo!()
    }

    fn collect_spans(&self) -> SpanSet<Key> {
        todo!()
    }

    fn execute(&self, header: &RequestMetadata, writer: &KVStore) -> ExecuteResult {
        let txn = header.txn.read().unwrap();
        let write_timestamp = txn.write_timestamp;
        writer.abort_transaction(&txn.txn_id, write_timestamp);
        Ok(ResponseUnion::AbortTxn(AbortTxnResponse {}))
    }
}

pub struct CommitTxnRequest {}

pub struct CommitTxnResponse {}

impl Command for CommitTxnRequest {
    fn is_read_only(&self) -> bool {
        true
    }

    fn collect_spans(&self) -> SpanSet<Key> {
        Vec::new()
    }

    fn execute(&self, header: &RequestMetadata, writer: &KVStore) -> ExecuteResult {
        let txn = header.txn.read().unwrap();
        let write_timestamp = txn.write_timestamp;

        writer.commit_transaction(&txn.txn_id, write_timestamp);
        Ok(ResponseUnion::CommitTxn(CommitTxnResponse {}))
    }
}

pub struct GetRequest {
    pub key: Key,
}

pub struct GetResponse {
    pub value: (MVCCKey, Value),
}

impl Command for GetRequest {
    fn is_read_only(&self) -> bool {
        true
    }

    fn collect_spans(&self) -> SpanSet<Key> {
        Vec::from([Range {
            start_key: self.key.clone(),
            end_key: self.key.clone(),
        }])
    }

    fn execute(&self, header: &RequestMetadata, writer: &KVStore) -> ExecuteResult {
        let txn = header.txn.read().unwrap();
        let result = writer.mvcc_get(
            &self.key,
            &txn.read_timestamp,
            MVCCGetParams {
                transaction: Some(&txn),
            },
        );

        match result.intent {
            Some(intent) => Err(ExecuteError::WriteIntentError(WriteIntentErrorData {
                intent: intent.clone(),
            })),
            None => Ok(ResponseUnion::Get(GetResponse {
                value: result.value.unwrap(),
            })),
        }
    }
}

pub struct PutRequest {
    pub key: Key,
    pub value: Value,
}

pub struct PutResponse {}

impl<'a> Command for PutRequest {
    fn is_read_only(&self) -> bool {
        false
    }

    fn collect_spans(&self) -> SpanSet<Key> {
        Vec::from([Range {
            start_key: self.key.clone(),
            end_key: self.key.clone(),
        }])
    }

    fn execute(&self, header: &RequestMetadata, writer: &KVStore) -> ExecuteResult {
        let txn = header.txn.read().unwrap();
        let res = writer.mvcc_put(
            self.key.clone(),
            Some(txn.write_timestamp),
            Some(&txn),
            self.value.clone(),
        );
        match res {
            Ok(_) => {
                // update the txn's lock spans to account for the intent being written
                txn.append_lock_span(self.key.clone());
                Ok(ResponseUnion::Put(PutResponse {}))
            }
            Err(err) => Err(ExecuteError::WriteIntentError(WriteIntentErrorData {
                intent: err.intent,
            })),
        }
    }
}

impl Command for RequestUnion {
    fn is_read_only(&self) -> bool {
        match self {
            RequestUnion::BeginTxn(command) => command.is_read_only(),
            RequestUnion::CommitTxn(command) => command.is_read_only(),
            RequestUnion::Get(command) => command.is_read_only(),
            RequestUnion::Put(command) => command.is_read_only(),
            RequestUnion::AbortTxn(command) => command.is_read_only(),
        }
    }

    fn collect_spans(&self) -> SpanSet<Key> {
        match self {
            RequestUnion::BeginTxn(command) => command.collect_spans(),
            RequestUnion::CommitTxn(command) => command.collect_spans(),
            RequestUnion::Get(command) => command.collect_spans(),
            RequestUnion::Put(command) => command.collect_spans(),
            RequestUnion::AbortTxn(command) => command.collect_spans(),
        }
    }

    fn execute(&self, header: &RequestMetadata, writer: &KVStore) -> ExecuteResult {
        match self {
            RequestUnion::BeginTxn(command) => command.execute(header, writer),
            RequestUnion::CommitTxn(command) => todo!(),
            RequestUnion::AbortTxn(command) => todo!(),
            RequestUnion::Get(command) => todo!(),
            RequestUnion::Put(command) => todo!(),
        }
    }
}
