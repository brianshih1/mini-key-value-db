# Motivation

_Richard Feynman - “What I cannot create, I do not understand”_

As someone without a background in databases, I’ve always been curious about the inner workings of transactions in OLTP databases such as PostgreSQL and CockroachDB. To unveil the mystery, I studied CockroachDB's transactional layer extensively and built a toy transactional key-value database based on it. I reimplemented techniques and algorithms used by CockroachDB's transactional layer such as MVCC, write intents, read refresh, and others.

Here is a quick summary of the toy database I built:

- key-value database
- thread-safe
- uses RocksDB as the storage engine
- written in Rust (my first time learning Rust!)
- support transactions
-  SSI (serializable Snapshot Isolation)
- uses MVCC (multi-version concurrency control)
- uses pessimistic write locks and optimistic reads (reads are lock-free)

In this blog series, I will cover the theory behind and the implementation of my toy database. The source code is available on GitHub [here](https://github.com/brianshih1/little-key-value-db)
