# Motivation

_Richard Feynman - “What I cannot create, I do not understand”_

As someone without a background in databases, I’ve always been curious about the inner workings of transactions in databases such as PostgreSQL and CockroachDB. To better understand this, I decided to build my own transactional key-value database.

Rather than trying to reinvent the wheel, I studied CockroachDB extensively and followed its architecture as closely as possible. For those of you who aren't familiar with what CockroachDB is, it is a distributed SQL database built on top of a transactional key-value store. By following the architecture of CockroachDB's key-value store, I was able to learn in a more structured manner and study its design patterns.

Here is a quick summary of the database I built:

- key-value database
- thread-safe
- uses RocksDB as the storage engine (so I can focus on the transactional layer)
- written in Rust (my first time learning Rust!)
- support transactions that are serializable
- uses MVCC (multi-version concurrency control)
- uses pessimistic write locks and optimistic reads (reads are lock-free)

In this blog series, I will explain the concepts used in my database and dive into the implementation detail/algorithms. Since I studied CockroachDB's codebase so closely, I will also provide the links that point to specific files/lines in CockroachDB's repo when I talk about the different concepts/implementation detail.

I want to emphasize that:

- this blog series is not a tutorial! I’m a beginner to both databases and Rust. Instead, I'm writing this blog series to solidify my understanding of the concepts I learned. I also want to share what I learned with other people who are interested in database transactions but aren't sure where to begin.
- My goal was to learn and implement database concepts. So I did not take my time to write the cleanest and the most performant code.

The source code is available on GitHub (PRs/suggestions welcome) [here](https://github.com/brianshih1/little-key-value-db)
