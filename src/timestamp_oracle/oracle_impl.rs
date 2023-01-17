use std::fmt;

use uuid::Uuid;

use crate::{
    hlc::timestamp::Timestamp,
    interval::interval_tree::IntervalTree,
    llrb::llrb::{NodeKey, NodeValue},
    storage::{Key, Value},
};

use super::oracle::TimestampOracle;

#[derive(Clone)]
struct OracleValue {
    timestamp: Timestamp,
    txn_id: Option<Uuid>,
}

impl NodeKey for Key {}

impl NodeValue for OracleValue {}

struct TimestampOracleImpl {
    interval_tree: IntervalTree<Key, OracleValue>,
}

impl TimestampOracle for TimestampOracleImpl {
    fn add(&mut self, timestamp: Timestamp, start: Key, end: Key, txn_id: Uuid) -> () {
        self.interval_tree.insert(
            start,
            end,
            OracleValue {
                timestamp: timestamp,
                txn_id: Some(txn_id),
            },
        )
    }

    /**
     * Inclusive of start & end
     */
    fn get_max_timestamp(
        &mut self,
        start: crate::storage::Key,
        end: crate::storage::Key,
    ) -> (Timestamp, Option<Uuid>) {
        let overlaps = self.interval_tree.get_overlap(start, end);
        todo!()
    }
}
