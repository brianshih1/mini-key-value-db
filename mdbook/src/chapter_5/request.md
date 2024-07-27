# Executing the Request

During the lifetime of a request, the request can finally be executed after the concurrency manager calls `sequence_req`.  In this section, we finally get to look at how the requests are executed. 

These are the request types of the database:

- BeginTxn
- CommitTxn
- AbortTxn
- Get
- Put

Each command needs to implement an [execute](https://github.com/brianshih1/little-key-value-db/blob/66f355d1a03c488c4f0aee5b8dc66796398bb4de/src/execute/request.rs#L93) method. Here are its implementations:

### BeginTxn

- BeginTxn simply [creates a transaction record](https://github.com/brianshih1/little-key-value-db/blob/66f355d1a03c488c4f0aee5b8dc66796398bb4de/src/execute/request.rs#L124) with the txn’s timestamp.

### CommitTxn

- CommitTxn first [fetches the transaction record](https://github.com/brianshih1/little-key-value-db/blob/66f355d1a03c488c4f0aee5b8dc66796398bb4de/src/execute/request.rs#L254).  If the transaction record is aborted, it [returns an error](https://github.com/brianshih1/little-key-value-db/blob/66f355d1a03c488c4f0aee5b8dc66796398bb4de/src/execute/request.rs#L265).

- Otherwise, it begins committing the transaction. Firstly, it needs to perform a read refresh to advance the read timestamp to the write timestamp. If you need a “refresher” on read refresh, look at [this page](https://brianshih1.github.io/mini-key-value-db/chapter_3/read_refresh.html).

  - If the read refresh is unsuccessful, it [returns a ReadRefreshError](https://github.com/brianshih1/little-key-value-db/blob/66f355d1a03c488c4f0aee5b8dc66796398bb4de/src/execute/request.rs#L271), which would restart the transaction.
  - If read refresh is successful, it [updates the transaction record](https://github.com/brianshih1/little-key-value-db/blob/66f355d1a03c488c4f0aee5b8dc66796398bb4de/src/execute/request.rs#L275) to be committed.

- Next, it [resolves the uncommitted intent](https://github.com/brianshih1/little-key-value-db/blob/66f355d1a03c488c4f0aee5b8dc66796398bb4de/src/execute/request.rs#LL280C18-L280C41), which replaces it with a proper MVCC key value with the transaction’s commit timestamp.

- Next, it [releases the locks](https://github.com/brianshih1/little-key-value-db/blob/66f355d1a03c488c4f0aee5b8dc66796398bb4de/src/execute/request.rs#L281) held by the transaction

- Finally, it [removes itself from the TxnWaitQueue](https://github.com/brianshih1/little-key-value-db/blob/efa45d5873e6536a52e2f08270e693f45ecaaeba/src/execute/request.rs#L195) in case it was queued

### AbortTxn

- AbortTxn first makes sure the transaction record [isn’t committed](https://github.com/brianshih1/little-key-value-db/blob/66f355d1a03c488c4f0aee5b8dc66796398bb4de/src/execute/request.rs#L169)
- Next, it [updates the transaction record to abort](https://github.com/brianshih1/little-key-value-db/blob/66f355d1a03c488c4f0aee5b8dc66796398bb4de/src/execute/request.rs#L176)
- It then [removes all uncommitted intents](https://github.com/brianshih1/little-key-value-db/blob/66f355d1a03c488c4f0aee5b8dc66796398bb4de/src/execute/request.rs#L181) from the MVCC database
- Next, it [releases the locks](https://github.com/brianshih1/little-key-value-db/blob/efa45d5873e6536a52e2f08270e693f45ecaaeba/src/execute/request.rs#L281) held by the transaction
- Finally, it [removes itself from the TxnWaitQueue](https://github.com/brianshih1/little-key-value-db/blob/efa45d5873e6536a52e2f08270e693f45ecaaeba/src/execute/request.rs#L298) in case it was queued

### Get

- [Get](https://github.com/brianshih1/little-key-value-db/blob/efa45d5873e6536a52e2f08270e693f45ecaaeba/src/execute/request.rs#L338) first uses the read timestamp to [perform a mvcc_get](https://github.com/brianshih1/little-key-value-db/blob/efa45d5873e6536a52e2f08270e693f45ecaaeba/src/execute/request.rs#L342) to retrieve the most recent value for the key.
- [If mvcc_get returns an uncommitted intent](https://github.com/brianshih1/little-key-value-db/blob/efa45d5873e6536a52e2f08270e693f45ecaaeba/src/execute/request.rs#L350), it [checks](https://github.com/brianshih1/little-key-value-db/blob/efa45d5873e6536a52e2f08270e693f45ecaaeba/src/execute/request.rs#L351) if the request’s transaction and the intent’s transaction are the same. In that case, the transaction is reading its uncommitted write so it can proceed.
- Otherwise, Get [returns a WriteIntentError](https://github.com/brianshih1/little-key-value-db/blob/efa45d5873e6536a52e2f08270e693f45ecaaeba/src/execute/request.rs#L355)
- Next, the function [adds the key to the read sets to the transaction](https://github.com/brianshih1/little-key-value-db/blob/efa45d5873e6536a52e2f08270e693f45ecaaeba/src/execute/request.rs#L365) so that a read refresh can be performed later if the write timestamp gets bumped.
- Finally, it returns the value.

### Put

- [Put](https://github.com/brianshih1/little-key-value-db/blob/efa45d5873e6536a52e2f08270e693f45ecaaeba/src/execute/request.rs#L390) uses [mvcc_put](https://github.com/brianshih1/little-key-value-db/blob/efa45d5873e6536a52e2f08270e693f45ecaaeba/src/execute/request.rs#L391) to attempt to put an uncommitted intent into the database.
- If mvcc_put is successful, it adds the uncommitted intent to the lock table by [calling acquire_lock](https://github.com/brianshih1/little-key-value-db/blob/efa45d5873e6536a52e2f08270e693f45ecaaeba/src/execute/request.rs#L404)
- Otherwise, this means that an uncommitted intent was detected. Since there can only be one uncommitted intent for each key, the function [returns a WriteIntentError](https://github.com/brianshih1/little-key-value-db/blob/efa45d5873e6536a52e2f08270e693f45ecaaeba/src/execute/request.rs#L419)