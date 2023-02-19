use std::collections::HashMap;

pub enum Request {
    BeginTransaction(BeginTransactionRequest),
    EndTransaction(EndTransactionRequest),
    Get(GetRequest),
    Put(PutRequest),
}

pub trait Command {
    fn is_read_only(&self) -> bool;

    fn collect_spans(&self) -> ();
}

pub struct RequestHeader {}

pub struct BeginTransactionRequest {}

impl Command for BeginTransactionRequest {
    fn is_read_only(&self) -> bool {
        true
    }

    fn collect_spans(&self) -> () {
        todo!()
    }
}

pub struct EndTransactionRequest {}

impl Command for EndTransactionRequest {
    fn is_read_only(&self) -> bool {
        todo!()
    }

    fn collect_spans(&self) -> () {
        todo!()
    }
}

pub struct GetRequest {}

impl Command for GetRequest {
    fn is_read_only(&self) -> bool {
        todo!()
    }

    fn collect_spans(&self) -> () {
        todo!()
    }
}

pub struct PutRequest {}

impl Command for PutRequest {
    fn is_read_only(&self) -> bool {
        todo!()
    }

    fn collect_spans(&self) -> () {
        todo!()
    }
}

impl Command for Request {
    fn is_read_only(&self) -> bool {
        match self {
            Request::BeginTransaction(command) => command.is_read_only(),
            Request::EndTransaction(command) => command.is_read_only(),
            Request::Get(command) => command.is_read_only(),
            Request::Put(command) => command.is_read_only(),
        }
    }

    fn collect_spans(&self) -> () {
        match self {
            Request::BeginTransaction(command) => command.collect_spans(),
            Request::EndTransaction(command) => command.collect_spans(),
            Request::Get(command) => command.collect_spans(),
            Request::Put(command) => command.collect_spans(),
        }
    }
}
