# Database API

Let’s first talk about the API of the toy database. The database’s API consists of the following methods:

- set_time
- begin_txn
- write
- read
- read_without_txn
- abort_txn
- commit_txn
- run_txn

Here is an example of using the database:

```rust
let db = DB::new("./tmp/data", Timestamp::new(10))
let txn1 = db.begin_txn().await;
let value = db.read::<i32>("foo", txn1).await.unwrap();
if value == "bar" {
	db.write("baz", 20, txn1).await.unwrap();
}
let commit_result = db.commit_txn(txn1).await;
```

In the code snippet above, we created a database by providing a path to specify where to store the records. We then began a transaction, performed a write and a read, then committed the transaction.

An alternative way to perform transactions is with the `run_txn` method. In the snippet below, the `run_txn` function automatically begins a transaction and commits the transaction at the end of the function scope. It would also abort the transaction if the inner function panics.

```rust
db.run_txn(|txn_context| async move {
		let value = txn_context.read::<i32>("foo").await;
		if value == "bar" {
	    txn_context.write("foo", 12).await.unwrap();
    }
})
```

For more examples, feel free to check out the [unit tests](https://github.com/brianshih1/little-key-value-db/blob/master/src/db/db_test.rs) I wrote for my database.

### Thread-safe

The database is thread-safe. If you wrap the database instance around an `Arc`, you can safely use it across different threads. For example:

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

In the example above, the serializability of the database guarantees that either all of `task1` is executed first or all of `task2` is executed first.

### Time

The database is powered by a Hybrid Logical Clock (which we will cover later). The developer must manually increment the physical time with the set_time function. But it can also be swapped out with an implementation that uses the system’s time instead.
