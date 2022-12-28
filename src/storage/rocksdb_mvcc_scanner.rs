use super::data::Key;
use crate::hlc::timestamp::Timestamp;

pub struct RocksMVCCScanner {
    // TODO: lockTable

    // start iteration bound (doesn't contain MVCC timestamp)
    pub start: Key,

    // end iteration bound (doesn't contain MVCC timestamp)
    pub end: Key,

    // Timestamp that MVCCScan/MVCCGet was called
    pub ts: Timestamp,
}
