use std::{collections::HashMap, default};

use uuid::Uuid;

use crate::{
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
pub struct Request<'a> {
    pub metadata: RequestMetadata<'a>,
    pub request_union: RequestUnion,
}

pub struct ExecuteResult {
    pub response: ResponseUnion,
    pub error: Option<ExecuteError>,
}

pub enum ExecuteError {
    WriteIntentError(WriteIntentErrorData),
}

pub struct WriteIntentErrorData {
    intent: TxnIntent,
}

pub enum ResponseUnion {
    BeginTransaction(BeginTransactionResponse),
    EndTransaction(EndTransactionResponse),
    AbortTransaction(AbortTransactionResponse),
    Get(GetResponse),
    Put(PutResponse),
}

pub enum RequestUnion {
    BeginTransaction(BeginTransactionRequest),
    EndTransaction(EndTransactionRequest),
    AbortTransaction(AbortTransactionRequest),
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
pub struct RequestMetadata<'a> {
    /**
     * The timestamp the request should evaluate at.
     * Should be set to Txn.ReadTimestamp if Txn is non-nil
     */
    pub timestamp: Timestamp,
    /**
     * For now, assume every request is part of a transaction.
     * In the future, we can support non-transactional reads.
     */
    pub txn: &'a Txn,
}

pub struct BeginTransactionRequest {
    txn_id: Uuid,
}

pub struct BeginTransactionResponse {}

impl Command for BeginTransactionRequest {
    fn is_read_only(&self) -> bool {
        true
    }

    fn collect_spans(&self) -> SpanSet<Key> {
        Vec::new()
    }

    fn execute(&self, header: &RequestMetadata, writer: &KVStore) -> ExecuteResult {
        let write_timestamp = header.txn.metadata.write_timestamp.clone();
        writer.create_pending_transaction_record(&self.txn_id, write_timestamp);
        ExecuteResult {
            response: ResponseUnion::BeginTransaction(BeginTransactionResponse {}),
            error: None,
        }
    }
}

pub struct AbortTransactionRequest {}

pub struct AbortTransactionResponse {}

impl Command for AbortTransactionRequest {
    fn is_read_only(&self) -> bool {
        todo!()
    }

    fn collect_spans(&self) -> SpanSet<Key> {
        todo!()
    }

    fn execute(&self, header: &RequestMetadata, mut writer: &KVStore) -> ExecuteResult {
        let txn = header.txn;
        let write_timestamp = header.txn.metadata.write_timestamp.clone();
        writer.abort_transaction(&txn.txn_id, write_timestamp);
        ExecuteResult {
            response: ResponseUnion::AbortTransaction(AbortTransactionResponse {}),
            error: None,
        }
    }
}

pub struct EndTransactionRequest {}

pub struct EndTransactionResponse {}

impl Command for EndTransactionRequest {
    fn is_read_only(&self) -> bool {
        true
    }

    fn collect_spans(&self) -> SpanSet<Key> {
        Vec::new()
    }

    fn execute(&self, header: &RequestMetadata, mut writer: &KVStore) -> ExecuteResult {
        let txn = header.txn;
        let write_timestamp = header.txn.metadata.write_timestamp.clone();

        writer.commit_transaction(&txn.txn_id, write_timestamp.clone());
        ExecuteResult {
            response: ResponseUnion::EndTransaction(EndTransactionResponse {}),
            error: None,
        }
    }
}

pub struct GetRequest {
    key: Key,
}

pub struct GetResponse {
    pub value: Option<(MVCCKey, Value)>,
    pub intent: Option<TxnIntent>,
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
        let result = writer.mvcc_get(
            &self.key,
            &header.timestamp,
            MVCCGetParams {
                transaction: Some(header.txn),
            },
        );

        // TODO: Remove clone
        let error = result.intent.clone().and_then(|intent| {
            Some(ExecuteError::WriteIntentError(WriteIntentErrorData {
                intent: intent.clone(), // TODO: Remove this clone
            }))
        });

        ExecuteResult {
            response: ResponseUnion::Get(GetResponse {
                value: result.value,
                intent: result.intent.clone(),
            }),
            error,
        }
    }
}

pub struct PutRequest {
    key: &'static str,
    value: Value,
}

pub struct PutResponse {}

impl<'a> Command for PutRequest {
    fn is_read_only(&self) -> bool {
        false
    }

    fn collect_spans(&self) -> SpanSet<Key> {
        Vec::from([Range {
            start_key: str_to_key(self.key),
            end_key: str_to_key(self.key),
        }])
    }

    fn execute(&self, header: &RequestMetadata, writer: &KVStore) -> ExecuteResult {
        let res = writer.mvcc_put(
            self.key,
            Some(header.timestamp.clone()),
            Some(header.txn),
            self.value.clone(),
        ); // TODO: Remove value.clone()
        let error = match res {
            Ok(_) => None,
            Err(err) => Some(ExecuteError::WriteIntentError(WriteIntentErrorData {
                intent: err.intent,
            })),
        };

        ExecuteResult {
            response: ResponseUnion::Put(PutResponse {}),
            error,
        }
    }
}

impl Command for RequestUnion {
    fn is_read_only(&self) -> bool {
        match self {
            RequestUnion::BeginTransaction(command) => command.is_read_only(),
            RequestUnion::EndTransaction(command) => command.is_read_only(),
            RequestUnion::Get(command) => command.is_read_only(),
            RequestUnion::Put(command) => command.is_read_only(),
            RequestUnion::AbortTransaction(command) => command.is_read_only(),
        }
    }

    fn collect_spans(&self) -> SpanSet<Key> {
        match self {
            RequestUnion::BeginTransaction(command) => command.collect_spans(),
            RequestUnion::EndTransaction(command) => command.collect_spans(),
            RequestUnion::Get(command) => command.collect_spans(),
            RequestUnion::Put(command) => command.collect_spans(),
            RequestUnion::AbortTransaction(command) => command.collect_spans(),
        }
    }

    fn execute(&self, header: &RequestMetadata, writer: &KVStore) -> ExecuteResult {
        match self {
            RequestUnion::BeginTransaction(command) => command.execute(header, writer),
            RequestUnion::EndTransaction(command) => todo!(),
            RequestUnion::AbortTransaction(command) => todo!(),
            RequestUnion::Get(command) => todo!(),
            RequestUnion::Put(command) => todo!(),
        }
    }
}
