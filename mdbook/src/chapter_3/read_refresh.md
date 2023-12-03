# Read Refresh

_Read refresh is a technique covered in section 3.4 of [CockroachDB’s research paper](https://www.cockroachlabs.com/guides/thank-you/?pdf=/pdf/cockroachdb-the-resilient-geo-distributed-sql-database-sigmod-2020.pdf)._

All reads for a transaction are performed at the transaction’s read timestamp. In the last section, we saw that the transaction’s write timestamp can be bumped if it runs into conflicts. Since a transaction commits at the final write timestamp, the read timestamp needs to be advanced when the transaction commits.

However, advancing the read timestamp is not always safe, as in the example below.

<img src="../images/read_refresh.png" width="55%">

In this example, transaction T1 performs a read for key A at timestamp 10. It then performs a write at key B, but it detects a committed write at a higher timestamp, so it advances the timestamp to 16. Finally, it commits. However, this is not safe because it results in a read-write conflict.

When a transaction advances the read timestamp, it must prove that the reads performed at the lower timestamp is still valid at the new timestamp. Suppose the read timestamp is tr and the commit timestamp is tc, the database must prove that the keys read by the transaction have not been updated by another transaction between (tr, tc].

If the database detects that a write has been performed between (tr, tc], the transaction must retry the entire transaction. Otherwise, it is safe to advance its read timestamp to the commit timestamp and commit the transaction. This technique is called *read refresh*.
