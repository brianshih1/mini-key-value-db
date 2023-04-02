use std::{collections::HashMap, default};

use async_trait::async_trait;
use uuid::Uuid;

use crate::{
    db::db::TxnLink,
    hlc::timestamp::Timestamp,
    latch_manager::latch_interval_btree::{NodeKey, Range},
    lock_table::lock_table::{CommitUpdateLock, UpdateLock},
    storage::{
        mvcc::{KVStore, MVCCGetParams, WriteIntentError},
        mvcc_key::MVCCKey,
        str_to_key,
        txn::{Txn, TxnIntent, TxnMetadata},
        Key, Value,
    },
    StorageResult,
};

use super::executor::Executor;

/**
 * Request is the input to SequenceReq. The struct contains all the information necessary
 * to sequence a request (to provide concurrent isolation for storage)
 */
pub struct Request {
    pub metadata: RequestMetadata,
    pub request_union: RequestUnion,
}

pub type ResponseResult = Result<ResponseUnion, ResponseError>;

pub enum ResponseError {
    WriteIntentError(WriteIntentErrorData),
    ReadRefreshError,
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

#[async_trait]
pub trait Command {
    fn is_read_only(&self) -> bool;

    /**
     * The spans that the request touches. This will be used for latches and locks.
     *
     * TODO: Should we separate latch spans from lock spans. Specifically, for read refresh,
     * do we need to scan_and_enqueue?
     */
    fn collect_spans(&self, txn_link: TxnLink) -> SpanSet<Key>;

    async fn execute(&self, header: &RequestMetadata, writer: &Executor) -> ResponseResult;
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

#[async_trait]
impl Command for BeginTxnRequest {
    fn is_read_only(&self) -> bool {
        true
    }

    fn collect_spans(&self, _: TxnLink) -> SpanSet<Key> {
        Vec::new()
    }

    async fn execute(&self, header: &RequestMetadata, executor: &Executor) -> ResponseResult {
        let txn = header.txn.read().unwrap();
        let write_timestamp = txn.write_timestamp;
        executor
            .store
            .create_pending_transaction_record(self.txn_id, write_timestamp);
        Ok(ResponseUnion::BeginTransaction(BeginTxnResponse {}))
    }
}

pub struct AbortTxnRequest {}

pub struct AbortTxnResponse {}

#[async_trait]
impl Command for AbortTxnRequest {
    fn is_read_only(&self) -> bool {
        todo!()
    }

    fn collect_spans(&self, _: TxnLink) -> SpanSet<Key> {
        todo!()
    }

    async fn execute(&self, header: &RequestMetadata, executor: &Executor) -> ResponseResult {
        let txn = header.txn.read().unwrap();
        let write_timestamp = txn.write_timestamp;
        executor
            .store
            .abort_transaction(txn.txn_id, write_timestamp);
        Ok(ResponseUnion::AbortTxn(AbortTxnResponse {}))
    }
}

pub struct CommitTxnRequest {}

pub struct CommitTxnResponse {}

#[async_trait]
impl Command for CommitTxnRequest {
    fn is_read_only(&self) -> bool {
        true
    }

    fn collect_spans(&self, txn: TxnLink) -> SpanSet<Key> {
        let txn = txn.read().unwrap();
        let read_set = txn.read_set.read().unwrap();
        read_set
            .iter()
            .map(|k| Range {
                start_key: k.clone(),
                end_key: k.clone(),
            })
            .collect::<SpanSet<Key>>()
    }

    async fn execute(&self, header: &RequestMetadata, executor: &Executor) -> ResponseResult {
        // read refresh
        let did_refresh = executor.refresh_read_timestamp(header.txn.clone());
        if !did_refresh {
            return Err(ResponseError::ReadRefreshError);
        }
        let (txn_id, write_timestamp) = self.get_txn_id_timestamp(header.txn.clone());
        executor
            .store
            .commit_transaction_record(txn_id, write_timestamp);

        //  Resolve intents so blocked requests won't run into uncommitted intents
        executor.resolve_intents_for_txn(header.txn.clone());
        executor
            .concr_manager
            .update_txn_locks(
                header.txn.clone(),
                UpdateLock::Commit(CommitUpdateLock {
                    txn_id: txn_id,
                    commit_timestamp: write_timestamp,
                }),
            )
            .await;
        //   The order that we resolve intents or update the lockTable doesn't have to be strictly
        //   synchronous because if we updated the lockTable but haven't resolved the intent,
        //   the subsequent request will run into an uncommitted record and will add it to the lockTable
        //   again. And it will then timeout and push the transaction to either resolve the intent
        //   or detect that the uncommitted intent is already cleaned up

        Ok(ResponseUnion::CommitTxn(CommitTxnResponse {}))
    }
}

impl CommitTxnRequest {
    pub fn get_txn_id_timestamp(&self, txn: TxnLink) -> (Uuid, Timestamp) {
        todo!()
    }
}

pub struct GetRequest {
    pub key: Key,
}

pub struct GetResponse {
    pub value: (MVCCKey, Value),
}

#[async_trait]
impl Command for GetRequest {
    fn is_read_only(&self) -> bool {
        true
    }

    fn collect_spans(&self, _: TxnLink) -> SpanSet<Key> {
        Vec::from([Range {
            start_key: self.key.clone(),
            end_key: self.key.clone(),
        }])
    }

    async fn execute(&self, header: &RequestMetadata, executor: &Executor) -> ResponseResult {
        let read_timestamp = header.txn.read().unwrap().read_timestamp;
        let result = executor.store.mvcc_get(
            &self.key,
            read_timestamp,
            MVCCGetParams {
                transaction: Some(header.txn.clone()),
            },
        );

        match result.intent {
            Some(intent) => Err(ResponseError::WriteIntentError(WriteIntentErrorData {
                intent: intent.clone(),
            })),
            None => {
                header
                    .txn
                    .read()
                    .unwrap()
                    .append_read_sets(self.key.clone());
                return Ok(ResponseUnion::Get(GetResponse {
                    value: result.value.unwrap(),
                }));
            }
        }
    }
}

pub struct PutRequest {
    pub key: Key,
    pub value: Value,
}

pub struct PutResponse {}

#[async_trait]
impl<'a> Command for PutRequest {
    fn is_read_only(&self) -> bool {
        false
    }

    fn collect_spans(&self, _: TxnLink) -> SpanSet<Key> {
        Vec::from([Range {
            start_key: self.key.clone(),
            end_key: self.key.clone(),
        }])
    }

    async fn execute(&self, header: &RequestMetadata, executor: &Executor) -> ResponseResult {
        let txn = header.txn.read().unwrap();
        let res = executor.store.mvcc_put(
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
            Err(err) => Err(ResponseError::WriteIntentError(WriteIntentErrorData {
                intent: err.intent,
            })),
        }
    }
}

#[async_trait]
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

    fn collect_spans(&self, txn_link: TxnLink) -> SpanSet<Key> {
        match self {
            RequestUnion::BeginTxn(command) => command.collect_spans(txn_link),
            RequestUnion::CommitTxn(command) => command.collect_spans(txn_link),
            RequestUnion::Get(command) => command.collect_spans(txn_link),
            RequestUnion::Put(command) => command.collect_spans(txn_link),
            RequestUnion::AbortTxn(command) => command.collect_spans(txn_link),
        }
    }

    async fn execute(&self, header: &RequestMetadata, executor: &Executor) -> ResponseResult {
        match self {
            RequestUnion::BeginTxn(command) => command.execute(header, executor).await,
            RequestUnion::CommitTxn(command) => command.execute(header, executor).await,
            RequestUnion::AbortTxn(command) => command.execute(header, executor).await,
            RequestUnion::Get(command) => command.execute(header, executor).await,
            RequestUnion::Put(command) => command.execute(header, executor).await,
        }
    }
}
