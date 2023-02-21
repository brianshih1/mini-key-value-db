use std::collections::HashMap;

use crate::{
    latch_manager::latch_interval_btree::{NodeKey, Range},
    storage::{mvcc::KVStore, mvcc_key::MVCCKey, Key, Value},
    StorageResult,
};

/**
 * Request is the input to SequenceReq. The struct contains all the information necessary
 * to sequence a request (to provide concurrent isolation for storage)
 */
pub struct Request {
    request_header: RequestHeader,
    request_union: RequestUnion,
}

pub enum RequestUnion {
    BeginTransaction(BeginTransactionRequest),
    EndTransaction(EndTransactionRequest),
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

    fn execute(&self, writer: KVStore) -> StorageResult<()>;
}

pub struct RequestHeader {
    // TODO: Timestamp
    // TODO: Transaction
}

pub struct BeginTransactionRequest {}

impl Command for BeginTransactionRequest {
    fn is_read_only(&self) -> bool {
        true
    }

    fn collect_spans(&self) -> SpanSet<Key> {
        Vec::new()
    }

    fn execute(&self, writer: KVStore) -> StorageResult<()> {
        todo!()
    }
}

pub struct EndTransactionRequest {}

impl Command for EndTransactionRequest {
    fn is_read_only(&self) -> bool {
        true
    }

    fn collect_spans(&self) -> SpanSet<Key> {
        Vec::new()
    }

    fn execute(&self, writer: KVStore) -> StorageResult<()> {
        todo!()
    }
}

pub struct GetRequest {
    key: Key,
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

    fn execute(&self, writer: KVStore) -> StorageResult<()> {
        todo!()
    }
}

pub struct PutRequest {
    key: Key,
    value: Value,
}

impl Command for PutRequest {
    fn is_read_only(&self) -> bool {
        false
    }

    fn collect_spans(&self) -> SpanSet<Key> {
        Vec::from([Range {
            start_key: self.key.clone(),
            end_key: self.key.clone(),
        }])
    }

    fn execute(&self, writer: KVStore) -> StorageResult<()> {
        todo!()
    }
}

impl Command for RequestUnion {
    fn is_read_only(&self) -> bool {
        match self {
            RequestUnion::BeginTransaction(command) => command.is_read_only(),
            RequestUnion::EndTransaction(command) => command.is_read_only(),
            RequestUnion::Get(command) => command.is_read_only(),
            RequestUnion::Put(command) => command.is_read_only(),
        }
    }

    fn collect_spans(&self) -> SpanSet<Key> {
        match self {
            RequestUnion::BeginTransaction(command) => command.collect_spans(),
            RequestUnion::EndTransaction(command) => command.collect_spans(),
            RequestUnion::Get(command) => command.collect_spans(),
            RequestUnion::Put(command) => command.collect_spans(),
        }
    }

    fn execute(&self, writer: KVStore) -> StorageResult<()> {
        todo!()
    }
}
