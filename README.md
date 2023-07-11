# Mini Key-Value DB

Mini-key-value-db is my toy transactional key value database for learning purposes. The focus of this project is on the transactional layer of the database and not the storage engine (I used RocksDB in this project).

I wrote a [blog](https://brianshih1.github.io/mini-key-value-db/) to deep dive into some of the core concepts I explored in this side project.

### Summary of the Database

- key-value database
- thread-safe
- uses RocksDB as the storage engine
- written in Rust (my first time learning Rust!)
- support transactions
- SSI (serializable Snapshot Isolation)
- uses MVCC (multi-version concurrency control)
- uses pessimistic write locks and optimistic reads (reads are lock-free)

### Database API

- new: `(path: &str, initial_time: Timestamp) → DB`
  - opens a database with the given path and specifies the time of the database
- begin_txn: `() -> Uuid`
  - starts a transaction and retrieves a txn ID
- write: `(key: &str, value: T, txn_id: Uuid) -> Result<ResponseUnion, ExecuteError>`
  - creates an uncommitted write in the database with the key and value associated with the txn ID
- read: `(key: &str, txn_id: Uuid) -> Option<T>`
  - Returns the most updated value for a given key for a txn ID. The method will return uncommitted writes from the same txn ID. Returns `None` if no value exists for the key
- read_without_txn: `(key: &str, timestamp: Timestamp) -> Option<T>`
  - TODO - not supported yet
- abort_txn: `(txn_id: Uuid) -> ()`
  - aborts the transaction
- commit_txn: `(txn_id: Uuid) -> CommitTxnResult`
  - commits the transaction
- run_txn: `f: impl FnOnce(Arc<TxnContext>) -> ()`
  - creates a transaction and performs the lambda function in the transaction’s context. When the function goes out of scope, the transaction commits (or aborts if there is an error)

### Examples

In this example, we begin a transaction and perform a read and write before committing the result.

```rust
let db = DB::new("./tmp/data", Timestamp::new(10))
let txn1 = db.begin_txn().await;
let value = db.read::<i32>("foo", txn1).await.unwrap();
if value == "bar" {
    db.write("baz", 20, txn1).await.unwrap();
}
let commit_result = db.commit_txn(txn1).await;
```

In this example, we use `run_txn` to run a transaction.

```rust
db.run_txn(|txn_context| async move {
        let value = txn_context.read::<i32>("foo").await;
        if value == "bar" {
        txn_context.write("foo", 12).await.unwrap();
    }
})
```

In this example, we show how the database instance can be shared across threads using Arc.

```rust
let db = Arc::new(DB::new("./tmp/data", Timestamp::new(10)));

let db_1 = Arc::clone(db);
let key1 = "foo";
let key2 = "bar";
let task_1 = tokio::spawn(async move {
    db_1.run_txn(|txn_context| async move {
        txn_context.write(key1, 1).await.unwrap();
                txn_context.write(key2, 10).await.unwrap();
    })
    .await
});

let db_2 = Arc::clone(db);
let task_2 = tokio::spawn(async move {
    db_2.run_txn(|txn_context| async move {
        txn_context.write(key1, 2).await.unwrap();
                txn_context.write(key2, 20).await.unwrap();
    })
    .await;
});
tokio::try_join!(task_1, task_2).unwrap();
```
