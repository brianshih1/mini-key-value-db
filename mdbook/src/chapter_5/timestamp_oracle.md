# Timestamp Oracle

As mentioned earlier, when a transaction performs a write on a key, its write timestamp must be greater than any read timestamps performed on the same key to prevent read-write conflicts. Therefore, the database needs to track the maximum timestamp that key ranges were read from. That is the job of the timestamp oracle. Timestamp oracle is a data structure that stores the maximum timestamp that key ranges were read from.

Timestamp oracle is introduced in Yabandeh’s research paper, [A critique of snapshot isolation](https://dl.acm.org/doi/10.1145/2168836.2168853). In the paper, Yabandeh proves that read-write conflict avoidance is sufficient for serializability. For a more detailed explanation of the timestamp oracle, check out this CockroachDB blog and scroll down to the section `Read-Write Conflicts – Read Timestamp Cache`.

### Timestamp Oracle API

The timestamp oracle’s API consists of two methods: get_max_timestamp and add

**Get_max_timestamp: (start_key, end_key) → Timestamp**

The method returns the max timestamp which overlaps with the interval [start_key, end_key].

**Add: (timestamp, start_key, end_key) → ()**

The method adds the timestamp for the range [start_key, end_key] to the oracle.

### **Red-Black Tree**

The timestamp oracle is implemented with a red-black interval tree. There are plenty of materials online on how to implement a red-black tree, so I won’t go into the details. In summary, red-black tree is a self-balancing binary search tree in which each node is marked as either black or red. The red-black tree has some invariants around the red and black nodes. The start key of the interval is used to order keys in the red-black tree.

[Here](https://github.com/brianshih1/little-key-value-db/blob/f239e62b5d97ff7754ce61e0f8ca02d889fcb4c2/src/llrb/llrb.rs) is my implementation of the red-black tree.

### Implementing the Timestamp Oracle API

**Add**

[Add](https://github.com/brianshih1/little-key-value-db/blob/f239e62b5d97ff7754ce61e0f8ca02d889fcb4c2/src/timestamp_oracle/oracle.rs#L61)’s implementation is really simple. It just inserts the interval into the red-black tree.

**Get_max_timestamp**

First, the function collects all overlap nodes. This is efficient because the timestamp oracle is a Binary Search Tree. Next, the function finds the max timestamp amongst the nodes and returns it.

### CockroachDB’s Implementation

CockroachDB calls the timestamp oracle their timestamp cache. CockroachDB’s interface for the timestamp cache is [here](https://github.com/cockroachdb/cockroach/blob/530100fd39cc722bc324bfb3869a325622258fb3/pkg/kv/kvserver/tscache/cache.go#L53). They have two implementations for the cache: [red-black tree based](https://github.com/cockroachdb/cockroach/blob/530100fd39cc722bc324bfb3869a325622258fb3/pkg/util/interval/llrb_based_interval.go) and [BTree based](https://github.com/cockroachdb/cockroach/blob/530100fd39cc722bc324bfb3869a325622258fb3/pkg/util/interval/btree_based_interval.go).

CockroachDB’s cache is bounded. It evict keys if the cache exceeds a certain size It also maintains a “low water mark” that is used whenever GetMax is called but the interval isn’t in the cache. To decide which keys to exceed, CockroachDB [holds a double-linked list of Entry elements](https://github.com/cockroachdb/cockroach/blob/master/pkg/util/cache/cache.go#L114). The order of entries is based on the [eviction policy](https://github.com/cockroachdb/cockroach/blob/master/pkg/util/cache/cache.go#L31), one of which is LRU and another is FIFO. I didn’t bother to implement this in my MVP.
