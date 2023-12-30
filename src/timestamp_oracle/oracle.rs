use std::{sync::RwLock};

use uuid::Uuid;

use crate::{
    execute::request::{Command, Request},
    hlc::timestamp::Timestamp,
    interval::interval_tree::IntervalTree,
    latch_manager::latch_interval_btree::Range,
    llrb::llrb::{NodeKey, NodeValue},
    storage::{Key},
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
pub struct TimestampOracle {
    interval_tree: RwLock<IntervalTree<Key, OracleValue>>,
}

impl TimestampOracle {
    pub fn new() -> Self {
        let interval_tree = IntervalTree::<Key, OracleValue>::new();
        TimestampOracle {
            interval_tree: RwLock::new(interval_tree),
        }
    }

    pub fn update_cache(&self, request: &Request) {
        let txn = request.metadata.txn.read().unwrap();
        let txn_id = Some(txn.txn_id);
        let timestamp = txn.read_timestamp;
        // TODO: We already executed this earlier in execute_request_with_concurrency_retries,
        // we should be able to avoid calling collect_spans twice
        let spans = request
            .request_union
            .collect_spans(request.metadata.txn.clone());
        for span in spans.iter() {
            let Range { start_key, end_key } = span;

            // TODO: Remove this clone
            self.add(timestamp, start_key.clone(), end_key.clone(), txn_id)
        }
    }

    /**
     * Adds the timestamp to the oracle. If start = end, then the range
     * represents a single point key
     *
     * Start and end are both inclusive
     */
    pub fn add(&self, timestamp: Timestamp, start: Key, end: Key, txn_id: Option<Uuid>) -> () {
        let mut g = self.interval_tree.write().unwrap();
        g.insert(
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
    pub fn get_max_timestamp(&self, start: Key, end: Key) -> Option<(Timestamp, Option<Uuid>)> {
        let g = self.interval_tree.read().unwrap();
        let overlaps = g.get_overlap(start, end);
        let mut max: Option<(Timestamp, Option<Uuid>)> = None;
        for range_value in overlaps.iter() {
            match max {
                Some((curr_timestamp, _txn_id)) => {
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
