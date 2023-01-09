use serde::{Deserialize, Serialize};

#[derive(Debug, Eq, PartialOrd, Ord, Clone, Copy, Serialize, Deserialize)]
pub struct Timestamp {
    pub wall_time: u64,
    pub logical_time: u32,
}

impl PartialEq for Timestamp {
    fn eq(&self, other: &Self) -> bool {
        self.logical_time == other.logical_time && self.wall_time == other.wall_time
    }
}

impl Timestamp {
    pub fn advance_by(&self, amount: u64) -> Timestamp {
        Timestamp {
            wall_time: self.wall_time + amount,
            logical_time: 0,
        }
    }

    pub fn decrement_by(&self, amount: u64) -> Timestamp {
        Timestamp {
            wall_time: self.wall_time - amount,
            logical_time: 0,
        }
    }

    pub fn new(wall_time: u64, logical_time: u32) -> Self {
        Timestamp {
            wall_time,
            logical_time,
        }
    }

    pub fn is_intent_timestamp(&self) -> bool {
        self.wall_time == 0 && self.logical_time == 0
    }
}

/**
 * Timestamp used to indicate that it's an intent
 */
pub fn get_intent_timestamp() -> Timestamp {
    Timestamp {
        wall_time: 0,
        logical_time: 0,
    }
}
