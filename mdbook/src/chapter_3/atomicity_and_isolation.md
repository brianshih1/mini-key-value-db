# CockroachDB & ACID

The goal of this project is to learn how to implement transactions in a database. First, let's look at what a transaction is.

A database transaction is a logical unit of execution in a database defined by certain properties. ACID is an acronym used to describe these properties: Atomicity, Consistency, Isolation, and Durability. 

- **Atomicity**: all of the database operations in a transaction are executed as a single unit (either all of them or none of them are performed).
- **Consistency**: transactions affect data in allowed and expected ways, satisfying all defined rules, including constraints, cascades, triggers, etc.
- **Isolation**: transactions don’t see intermediate changes of other transactions. CockroachDB supports the strictest transaction isolation level: *serializable isolation*. While this isolation allows transactions to run concurrently, it creates the illusion that transactions are run in serial order.
- **Durability**: once the transaction completes, changes to the database are persisted even in the case of system failures.

How does CockroachDB (CRDB) satisfy ACID?

- **Atomicity**: CRDB's [research paper](https://www.cockroachlabs.com/guides/thank-you/?pdf=/pdf/cockroachdb-the-resilient-geo-distributed-sql-database-sigmod-2020.pdf) outlines how it achieves transaction atomicity in Section 3.2. An atomic transaction is achieved with an abstraction called the *Write Intent*, which we will cover later.
- **Consistency**: As stated in this [CRDB blog](https://www.cockroachlabs.com/blog/db-consistency-isolation-terminology/), CRDB supports a consistency level they call *strong partition*. CRDB [uses Japsen Testing](https://www.cockroachlabs.com/blog/cockroachdb-beta-passes-jepsen-testing/) to test for correctness and consistency. Consistency is a complex topic that I will not dive into for this project. 
- **Isolation**: CRDB's [research paper](https://www.cockroachlabs.com/guides/thank-you/?pdf=/pdf/cockroachdb-the-resilient-geo-distributed-sql-database-sigmod-2020.pdf) outlines how it achieves serializable isolation in Sections 3.3 and 3.4. To achieve serializable isolation, each transaction performs its read and write at a commit timestamp. This results in the total ordering of transactions. However, since transactions can be executed concurrently, the commit timestamp needs to be adjusted when it runs into conflicts. This will be covered later.
- **Durability**: CRDB guarantees durability via the Raft Consensus Algorithm which guarantees that every acknowledged write has been persisted by the majority of replicas.

In this project, I focused on reimplementing algorithms that CRDB used in the transactional layer to guarantee Atomicity and Isolation in its transactions.

Now, let's take a look at one of the building blocks of CRDB's transactional layer - write intents!

