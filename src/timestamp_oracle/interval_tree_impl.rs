use crate::{hlc::timestamp::Timestamp, storage::Key};

use super::oracle::TimestampOracle;

struct TimestampOracleImpl {}

impl TimestampOracle for TimestampOracleImpl {
    fn add(
        &mut self,
        timestamp: crate::hlc::timestamp::Timestamp,
        start: crate::storage::Key,
        end: crate::storage::Key,
        txn_id: uuid::Uuid,
    ) -> () {
    }

    fn get_max_timestamp(
        &mut self,
        start: crate::storage::Key,
        end: crate::storage::Key,
    ) -> (crate::hlc::timestamp::Timestamp, Option<uuid::Uuid>) {
        todo!()
    }

    fn get_low_water(&mut self) -> crate::hlc::timestamp::Timestamp {
        todo!()
    }
}
