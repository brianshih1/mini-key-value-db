use uuid::Uuid;

use crate::{hlc::timestamp::Timestamp, storage::Key};

// Inspired by CockroachDB's https://github.com/cockroachdb/cockroach/blob/master/pkg/kv/kvserver/tscache/cache.go#L31
// which corresponds to the oracle described in Yabandeh's A Critique of Snapshot Isolation
pub trait TimestampOracle {
    /**
     * Adds the timestamp to the oracle. If end is not provided, then the range
     * represents a single point key
     */
    fn add(&mut self, timestamp: Timestamp, start: Key, end: Key, txn_id: Uuid) -> ();

    /**
     * Returns the max timestamp which overlaps with the start-end interval provided.
     * If the max timestamp belongs to a single transaction, the transactionId is returned.
     * Otherwise, if the max is shared by multiple transactions, no transaction ID is returned.
     *
     * If there are no overlap with any transactions, the low water timestmap is returned.
     */
    fn get_max_timestamp(&mut self, start: Key, end: Key) -> (Timestamp, Option<Uuid>);

    /**
     * As explained in Matt Tracy's blog: https://www.cockroachlabs.com/blog/serializable-lockless-distributed-isolation-cockroachdb/,
     * the low water mark is maintained to deal with keys not in the cache.
     * The low water mark is equivalent to the earliest read timestamp of any key that is present in the cache.
     */
    fn get_low_water(&mut self) -> Timestamp;
}
