# Motivation

_Richard Feynman - “What I cannot create, I do not understand”_

As someone without a background in databases, I’ve always been curious about the inner workings of transactions in databases such as PostgreSQL and CockroachDB. To better understand this, I decided to build my own transactional key-value database.

Rather than trying to reinvent the wheel, I studied CockroachDB extensively and followed its architecture as closely as possible. For those of you who aren't familiar with what CockroachDB is, it is a distributed SQL database built on top of a transactional key-value store. By following the architecture of CockroachDB's key-value store, I was able to learn in a more structured manner and study its design patterns.

Here is a quick summary of the database I built:

- key-value database
- thread-safe
- uses RocksDB as the storage engine
- written in Rust (my first time learning Rust!)
- support transactions
-  SSI (serializable Snapshot Isolation)
- uses MVCC (multi-version concurrency control)
- uses pessimistic write locks and optimistic reads (reads are lock-free)

My motivation for writing this blog series is to solidify my understanding of the project. In this blog series, I will explain the theory behind the database and dive into the implementation details. I will also provide references and summaries for CockroachDB's repo.

The source code is available on GitHub (PRs/suggestions welcome) [here](https://github.com/brianshih1/little-key-value-db)
