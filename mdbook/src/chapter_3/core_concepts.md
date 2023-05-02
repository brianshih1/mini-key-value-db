# Core Concepts

Atomicity and isolation are two important traits of transactional databases.

- Atomicity: all of the database operations in a transaction are executed as a single unit (either all of them or none of them are performed).
- Isolation: transactions don’t see intermediate changes of other transactions.

To guarantee atomicity and isolation, I borrowed many concepts from CockroachDB’s architecture, which they outlined in [their research paper](https://www.cockroachlabs.com/guides/thank-you/?pdf=/pdf/cockroachdb-the-resilient-geo-distributed-sql-database-sigmod-2020.pdf). The relevant sections include sections 3.2  (Atomicity Guarantees), section 3.3 (Concurrency Control), and section 3.24 (Read Refreshes).

In general, here are some of the core techniques I used to implement transactions

- MVCC
- Write Intents
- Pessimistic writes and optimistic reads
- Read refreshes

Other parts of the blog series will cover topics like the internals of concurrency control, deadlock detection, and hybrid-logical clock.

