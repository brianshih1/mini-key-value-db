use std::fmt;

use uuid::Uuid;

use crate::{
    hlc::timestamp::Timestamp,
    interval::interval_tree::IntervalTree,
    llrb::llrb::{NodeKey, NodeValue},
    storage::{Key, Value},
};

#[derive(Clone)]
struct OracleValue {
    pub timestamp: Timestamp,
    pub txn_id: Option<Uuid>,
}

impl NodeKey for Key {}

impl NodeValue for OracleValue {}

// Inspired by CockroachDB's https://github.com/cockroachdb/cockroach/blob/master/pkg/kv/kvserver/tscache/cache.go#L31
// which corresponds to the oracle described in Yabandeh's A Critique of Snapshot Isolation
struct TimestampOracle {
    interval_tree: IntervalTree<Key, OracleValue>,
}

impl TimestampOracle {
    pub fn new() -> Self {
        let interval_tree = IntervalTree::<Key, OracleValue>::new();
        TimestampOracle { interval_tree }
    }

    /**
     * Adds the timestamp to the oracle. If start = end, then the range
     * represents a single point key
     *
     * Start and end are both inclusive
     */
    pub fn add(&mut self, timestamp: Timestamp, start: Key, end: Key, txn_id: Option<Uuid>) -> () {
        self.interval_tree.insert(
            start,
            end,
            OracleValue {
                timestamp: timestamp,
                txn_id,
            },
        )
    }

    /**
     * Returns the max timestamp which overlaps with the start-end interval provided.
     * If the max timestamp belongs to a single transaction, the transactionId is returned.
     * Otherwise, if the max is shared by multiple transactions, no transaction ID is returned.
     *
     * If there are no overlap with any transactions, the low water timestmap is returned.
     *
     * Start and end are both inclusive
     */
    pub fn get_max_timestamp(
        &mut self,
        start: crate::storage::Key,
        end: crate::storage::Key,
    ) -> Option<(Timestamp, Option<Uuid>)> {
        let overlaps = self.interval_tree.get_overlap(start, end);
        let mut max: Option<(Timestamp, Option<Uuid>)> = None;
        for range_value in overlaps.iter() {
            match max {
                Some((curr_timestamp, txn_id)) => {
                    let ord = curr_timestamp.cmp(&range_value.value.timestamp);
                    match ord {
                        std::cmp::Ordering::Less => {
                            max = Some((range_value.value.timestamp, range_value.value.txn_id));
                        }
                        std::cmp::Ordering::Equal => {
                            max = Some((range_value.value.timestamp, None))
                        }
                        std::cmp::Ordering::Greater => {}
                    }
                }
                None => max = Some((range_value.value.timestamp, range_value.value.txn_id)),
            }
        }
        max
    }
}

mod Test {

    mod get_max_timestamp {
        use crate::{
            hlc::timestamp::Timestamp, storage::str_to_key,
            timestamp_oracle::oracle::TimestampOracle,
        };

        #[test]
        fn test_overlap() {
            let mut oracle = TimestampOracle::new();
            oracle.add(
                Timestamp::new(3, 5),
                str_to_key("apple"),
                str_to_key("cat"),
                None,
            );

            oracle.add(
                Timestamp::new(12, 5),
                str_to_key("banana"),
                str_to_key("dog"),
                None,
            );
            let max = oracle.get_max_timestamp(str_to_key("apple"), str_to_key("donkey"));
            assert_eq!(max, Some((Timestamp::new(12, 5), None)));
        }

        #[test]
        fn no_overlap() {
            let mut oracle = TimestampOracle::new();
            oracle.add(
                Timestamp::new(3, 5),
                str_to_key("apple"),
                str_to_key("cat"),
                None,
            );

            oracle.add(
                Timestamp::new(12, 5),
                str_to_key("banana"),
                str_to_key("dog"),
                None,
            );
            let max = oracle.get_max_timestamp(str_to_key("zebra"), str_to_key("zebra"));
            assert_eq!(max, None);
        }
    }
}
