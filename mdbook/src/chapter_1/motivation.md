# Motivation

_Richard Feynman - “What I cannot create, I do not understand”_

As someone without a background in databases, I’ve always been curious about the inner workings of transactions in databases such as PostgreSQL and CockroachDB. To unveil the mystery, I studied CockroachDB's transactional layer extensively and built a toy transactional key-value database. I reimplemented techniques and algorithms used by CockroachDB such as MVCC, write intents, read refresh, etc.

Here is a quick summary of the database I built:

- key-value database
- thread-safe
- uses RocksDB as the storage engine
- written in Rust (my first time learning Rust!)
- support transactions
-  SSI (serializable Snapshot Isolation)
- uses MVCC (multi-version concurrency control)
- uses pessimistic write locks and optimistic reads (reads are lock-free)

In this blog series, I will first go over the techniques I learned from CockroachDB then perform a deep dive into my implementation. I will also provide references to CockroachDB's repo in case you would like to dig into their codebase.

The source code is available on GitHub (PRs/suggestions welcome) [here](https://github.com/brianshih1/little-key-value-db)
