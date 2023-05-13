use std::{
    collections::{HashMap, HashSet},
    default,
};

use async_trait::async_trait;
use rand::prelude::Distribution;
use uuid::Uuid;

use crate::{
    db::db::TxnLink,
    hlc::timestamp::Timestamp,
    latch_manager::latch_interval_btree::{NodeKey, Range},
    lock_table::lock_table::{AbortUpdateLock, CommitUpdateLock, UpdateLock},
    storage::{
        mvcc::{KVStore, MVCCGetParams, WriteIntentError},
        mvcc_key::{create_intent_key, MVCCKey},
        str_to_key,
        txn::{TransactionStatus, Txn, TxnIntent, TxnMetadata},
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
    TxnAbortedError,
}

pub struct WriteIntentErrorData {
    pub intent: TxnIntent,
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

pub fn dedupe_spanset(v: &mut SpanSet<Key>) {
    // note the Copy constraint
    let mut uniques = HashSet::<Key>::new();
    v.retain(|e| uniques.insert((*e.start_key).to_vec()));
}

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
        true
    }

    fn collect_spans(&self, txn: TxnLink) -> SpanSet<Key> {
        let txn = txn.read().unwrap();
        let read_set = txn.read_set.read().unwrap();
        let mut lock_spans = (&*txn.lock_spans.read().unwrap())
            .iter()
            .map(|k| Range {
                start_key: k.clone(),
                end_key: k.clone(),
            })
            .collect::<SpanSet<Key>>();

        let mut read_spans = read_set
            .iter()
            .map(|k| Range {
                start_key: k.clone(),
                end_key: k.clone(),
            })
            .collect::<SpanSet<Key>>();
        lock_spans.append(&mut read_spans);
        dedupe_spanset(&mut lock_spans);
        lock_spans
    }

    async fn execute(&self, header: &RequestMetadata, executor: &Executor) -> ResponseResult {
        let (txn_id, write_timestamp) = self.get_txn_id_write_timestamp(header.txn.clone());

        let record_option = executor.store.get_transaction_record(txn_id);
        if let Some(record) = record_option {
            if let TransactionStatus::COMMITTED = record.status {
                // TODO: return an error instead
                return Ok(ResponseUnion::AbortTxn(AbortTxnResponse {}));
            }
        }

        // finalize the transaction record
        executor
            .store
            .update_transaction_record_to_abort(txn_id, write_timestamp);

        // Remove all uncomitted intents from DB
        for key in self.get_lock_keys(header.txn.clone()).iter() {
            executor.store.mvcc_delete(create_intent_key(key));
        }

        // update the lock table to release all uncommitted intent holders
        executor
            .concr_manager
            .update_txn_locks(
                header.txn.clone(),
                UpdateLock::Abort(AbortUpdateLock { txn_id: txn_id }),
            )
            .await;

        // release any pushers blocked by the transaction
        executor
            .concr_manager
            .lock_table
            .txn_wait_queue
            .finalize_txn(txn_id)
            .await;
        Ok(ResponseUnion::AbortTxn(AbortTxnResponse {}))
    }
}

impl AbortTxnRequest {
    pub fn get_txn_id_write_timestamp(&self, txn: TxnLink) -> (Uuid, Timestamp) {
        let txn = txn.read().unwrap();
        (txn.txn_id, txn.write_timestamp)
    }

    pub fn get_lock_keys(&self, txn: TxnLink) -> Vec<Key> {
        txn.read().unwrap().lock_spans.read().unwrap().clone()
    }
}

pub struct CommitTxnRequest {}

pub struct CommitTxnResponse {
    pub commit_timestamp: Timestamp,
}

#[async_trait]
impl Command for CommitTxnRequest {
    fn is_read_only(&self) -> bool {
        // TODO: Would this bump the timestamp oracle...?
        true
    }

    fn collect_spans(&self, txn: TxnLink) -> SpanSet<Key> {
        let txn = txn.read().unwrap();
        let read_set = txn.read_set.read().unwrap();
        let mut lock_spans = (&*txn.lock_spans.read().unwrap())
            .iter()
            .map(|k| Range {
                start_key: k.clone(),
                end_key: k.clone(),
            })
            .collect::<SpanSet<Key>>();

        let mut read_spans = read_set
            .iter()
            .map(|k| Range {
                start_key: k.clone(),
                end_key: k.clone(),
            })
            .collect::<SpanSet<Key>>();
        lock_spans.append(&mut read_spans);
        dedupe_spanset(&mut lock_spans);
        lock_spans
    }

    async fn execute(&self, header: &RequestMetadata, executor: &Executor) -> ResponseResult {
        let (txn_id, write_timestamp) = self.get_txn_id_write_timestamp(header.txn.clone());
        let record_option = executor.store.get_transaction_record(txn_id);
        if let Some(record) = record_option {
            if let TransactionStatus::ABORTED = record.status {
                executor
                    .concr_manager
                    .update_txn_locks(
                        header.txn.clone(),
                        UpdateLock::Abort(AbortUpdateLock { txn_id: txn_id }),
                    )
                    .await;

                return Err(ResponseError::TxnAbortedError);
            }
        }

        // read refresh
        let did_refresh = executor.refresh_read_timestamp(header.txn.clone());
        if !did_refresh {
            return Err(ResponseError::ReadRefreshError);
        }

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

        Ok(ResponseUnion::CommitTxn(CommitTxnResponse {
            commit_timestamp: write_timestamp,
        }))
    }
}

impl CommitTxnRequest {
    pub fn get_txn_id_write_timestamp(&self, txn: TxnLink) -> (Uuid, Timestamp) {
        let txn = txn.read().unwrap();
        (txn.txn_id, txn.write_timestamp)
    }
}

pub struct GetRequest {
    pub key: Key,
}

pub struct GetResponse {
    pub value: Option<(MVCCKey, Value)>,
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
        // TODO: Don't need two reads
        let read_timestamp = header.txn.read().unwrap().read_timestamp;
        let txn_id = header.txn.read().unwrap().txn_id;
        let result = executor.store.mvcc_get(
            &self.key,
            read_timestamp,
            MVCCGetParams {
                transaction: Some(header.txn.clone()),
            },
        );
        let mut value = result.value;
        if let Some((ref intent, uncommitted_value)) = result.intent {
            if txn_id == intent.txn_meta.txn_id {
                // this means the txn is reading its own uncommitted write
                value = Some((create_intent_key(&self.key.clone()), uncommitted_value))
            } else if intent.txn_meta.write_timestamp <= read_timestamp {
                return Err(ResponseError::WriteIntentError(WriteIntentErrorData {
                    intent: intent.clone(),
                }));
            }
        }

        header
            .txn
            .read()
            .unwrap()
            .append_read_sets(self.key.clone());
        return Ok(ResponseUnion::Get(GetResponse { value }));
    }
}

pub struct PutRequest {
    pub key: Key,
    pub value: Value,
}

pub struct PutResponse {}

#[async_trait]
impl Command for PutRequest {
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
        let res = executor.store.mvcc_put_raw(
            self.key.clone(),
            None, // TODO: Allow put without transaction
            Some(header.txn.clone()),
            self.value.clone(),
        );

        match res {
            Ok(_) => {
                // add to lockTable
                executor
                    .concr_manager
                    .lock_table
                    .acquire_lock(self.key.clone(), header.txn.clone())
                    .await;
                // update the txn's lock spans to account for the intent being written
                header
                    .txn
                    .read()
                    .unwrap()
                    .append_lock_span(self.key.clone());
                println!(
                    "finished executing write for txn {} with key {:?}",
                    header.txn.read().unwrap().txn_id,
                    self.key.clone()
                );
                Ok(ResponseUnion::Put(PutResponse {}))
            }
            Err(err) => Err(ResponseError::WriteIntentError(WriteIntentErrorData {
                intent: err.intent,
            })),
        }
    }
}

impl RequestUnion {
    pub fn get_type_string(&self) -> &str {
        match self {
            RequestUnion::BeginTxn(_) => "begin txn",
            RequestUnion::CommitTxn(_) => "commit txn",
            RequestUnion::Get(_) => "get",
            RequestUnion::Put(_) => "put",
            RequestUnion::AbortTxn(_) => "abort",
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
