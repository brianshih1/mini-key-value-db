use std::{collections::HashMap, default};

use uuid::Uuid;

use crate::{
    hlc::timestamp::Timestamp,
    latch_manager::latch_interval_btree::{NodeKey, Range},
    storage::{
        mvcc::{KVStore, MVCCGetParams, WriteIntentError},
        mvcc_key::MVCCKey,
        str_to_key,
        txn::{Transaction, TransactionMetadata},
        Key, Value,
    },
    StorageResult,
};

/**
 * Request is the input to SequenceReq. The struct contains all the information necessary
 * to sequence a request (to provide concurrent isolation for storage)
 */
pub struct Request<'a> {
    request_header: RequestHeader<'a>,
    request_union: RequestUnion,
}

pub struct ExecuteResult {
    response: ResponseUnion,
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

    fn execute(&self, header: &RequestHeader, writer: &mut KVStore) -> ExecuteResult;
}

pub struct RequestHeader<'a> {
    // TODO: Timestamp
    // TODO: Transaction
    /**
     * The timestamp the request should evaluate at.
     * Should be set to Txn.ReadTimestamp if Txn is non-nil
     */
    timestamp: Timestamp,
    /**
     * The optional transaction that sent the request.
     * Non-transactional requests do not acquire locks
     */
    txn: Option<&'a Transaction>,
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

    fn execute(&self, header: &RequestHeader, mut writer: &mut KVStore) -> ExecuteResult {
        writer.create_pending_transaction_record(&self.txn_id);
        ExecuteResult {
            response: ResponseUnion::BeginTransaction(BeginTransactionResponse {}),
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

    fn execute(&self, header: &RequestHeader, mut writer: &mut KVStore) -> ExecuteResult {
        let txn = header.txn.unwrap();
        writer.abort_transaction(&txn.transaction_id);
        ExecuteResult {
            response: ResponseUnion::AbortTransaction(AbortTransactionResponse {}),
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

    fn execute(&self, header: &RequestHeader, mut writer: &mut KVStore) -> ExecuteResult {
        let txn = header.txn.unwrap();
        writer.commit_transaction(&txn.transaction_id);
        ExecuteResult {
            response: ResponseUnion::EndTransaction(EndTransactionResponse {}),
        }
    }
}

pub struct GetRequest {
    key: Key,
}

pub struct GetResponse {
    pub value: Option<(MVCCKey, Value)>,
    pub intent: Option<TransactionMetadata>,
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

    fn execute(&self, header: &RequestHeader, writer: &mut KVStore) -> ExecuteResult {
        let result = writer.mvcc_get(
            &self.key,
            &header.timestamp,
            MVCCGetParams {
                transaction: header.txn,
            },
        );

        ExecuteResult {
            response: ResponseUnion::Get(GetResponse {
                value: result.value,
                intent: result.intent,
            }),
        }
    }
}

pub struct PutRequest {
    key: &'static str,
    value: Value,
}

pub enum PutResponse {
    Success(),
    WriteIntentError(WriteIntentError),
}

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

    fn execute(&self, header: &RequestHeader, mut writer: &mut KVStore) -> ExecuteResult {
        let res = writer.mvcc_put(
            self.key,
            Some(header.timestamp.clone()),
            header.txn.and_then(|txn| Some(txn)),
            self.value.clone(),
        ); // TODO: Remove value.clone()

        ExecuteResult {
            response: ResponseUnion::Put(match res {
                Ok(_) => PutResponse::Success(),
                Err(err) => PutResponse::WriteIntentError(err),
            }),
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

    fn execute(&self, header: &RequestHeader, writer: &mut KVStore) -> ExecuteResult {
        match self {
            RequestUnion::BeginTransaction(command) => command.execute(header, writer),
            RequestUnion::EndTransaction(command) => todo!(),
            RequestUnion::AbortTransaction(command) => todo!(),
            RequestUnion::Get(command) => todo!(),
            RequestUnion::Put(command) => todo!(),
        }
    }
}