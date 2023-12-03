# MVCC & Write Intents

Before we look at what write intents are, we need to understand what MVCC (multiversion concurrency control) is.

MVCC is a popular transaction management scheme in DBMSs. The overall idea behind MVCC is that the database stores multiple versions for each record. There is no standard way to implement MVCC and each design choice has its tradeoffs. In this section, we will look at CockroachDB's variation of MVCC.

In CockroachDB, when a record is updated or added, a new entry in the database is added instead of overwriting the original entry. Each record has a unique timestamp. Database *reads* are performed at a certain timestamp. The database returns the most up-to-date record less than or equal to the specified timestamp.

In the example below, there are 3 different versions of the “Apple” key, each with a different timestamp. The read is performed at timestamp 15. The database returns the result at timestamp 10 since that is the record with the largest timestamp less than 15.

<img src="../images/mvcc.png" width="65%">

### Write Intent

A single transaction may perform multiple writes. Before the transaction is committed, the uncommitted writes must not be read by other transactions. To address this problem, CRDB introduces write intents.

A write intent is a record stored in the MVCC database to represent uncommitted writes. It is given an INTENT timestamp that distinguishes it from normal timestamps. Each key must only have at most one write intent.

<img src="../images/write_intent.png" width="55%">

In the example above, there is an uncommitted write with an INTENT timestamp for the "Apple" key. The table stores additional metadata for the write intent, including the transaction ID and the write timestamp.

### Transaction Record

A transaction may create multiple write intents for different keys. The visibility of the uncommitted intent for a transaction must be flipped atomically. CRDB introduces *transaction records* to address this.

The database stores a transaction record for each transaction in a separate database namespace. The key of the transaction records is the transaction ID and the value contains the status and timestamp for each transaction.

When the transaction is committed, the transaction record is marked as committed. All the write intents are readable by other transactions at this moment.

<img src="../images/txn_record.png" width="50%">

When a reader runs into an intent, it uses the `txn_id` on the write intent to find the intent's transation record. If the transaction record is COMMITTED, the reader considers the intent as a regular value. Otherwise, if the record is PENDING, then the reader is blocked until the intent is finalized (committed or aborted). 

Each transaction performs its reads and writes at the commit timestamp, which is the timestamp on the transaction's transaction record. Since each timestamp in the database is guaranteed to be unique, each committed transaction has a unique commit timestamp. This guarantees serial execution order of transactions.

However, since transactions can be executed concurrently, conflicts between transactions may require adjustments of the commit timestamp. Let's look at the possible conflicts in the next page.

