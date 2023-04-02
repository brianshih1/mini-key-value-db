use std::cmp::Ordering;

use serde::{Deserialize, Serialize};

#[derive(Debug, Eq, Clone, Copy, Serialize, Deserialize)]
pub struct Timestamp {
    pub wall_time: u64,
    pub logical_time: u32,
}

impl PartialEq for Timestamp {
    fn eq(&self, other: &Self) -> bool {
        self.logical_time == other.logical_time && self.wall_time == other.wall_time
    }
}

impl Ord for Timestamp {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        if self.is_intent_timestamp() && other.is_intent_timestamp() {
            Ordering::Equal
        } else if self.is_intent_timestamp() {
            Ordering::Greater
        } else if other.is_intent_timestamp() {
            Ordering::Less
        } else {
            if self.wall_time > other.wall_time {
                Ordering::Greater
            } else if self.wall_time < other.wall_time {
                Ordering::Less
            } else {
                self.logical_time.cmp(&other.logical_time)
            }
        }
    }
}

impl PartialOrd for Timestamp {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Timestamp {
    pub fn advance_by(&self, amount: u64) -> Timestamp {
        Timestamp {
            wall_time: self.wall_time + amount,
            logical_time: 0,
        }
    }

    pub fn advance_to(&self, timestamp: Timestamp) -> Timestamp {
        if self > &timestamp {
            self.clone()
        } else {
            timestamp
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

    pub fn intent_timestamp() -> Self {
        get_intent_timestamp()
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

#[cfg(test)]
mod test {

    mod advance_to {
        use crate::hlc::timestamp::Timestamp;

        #[test]
        fn advance_to_higher() {
            let first = Timestamp::new(12, 12);
            let second = Timestamp::new(100, 12);
            let advanced = first.advance_to(second);
            assert_eq!(advanced, second);
        }

        #[test]
        fn advance_to_lower_timestamp() {
            let first = Timestamp::new(12, 12);
            let second = Timestamp::new(1, 12);
            let advanced = first.advance_to(second);
            assert_eq!(advanced, first);
        }
    }
    mod compare_timestamp {
        use crate::hlc::timestamp::Timestamp;

        #[test]
        fn compare_intent_timestamp() {
            let first = Timestamp::intent_timestamp();
            let second = Timestamp::new(12, 12);
            let is_first_bigger = first > second;
            assert!(first > second)
        }

        #[test]
        fn compare_same_wall_time() {
            let first = Timestamp::new(12, 10);
            let second = Timestamp::new(12, 12);
            assert!(first < second)
        }

        #[test]
        fn compare_different_walltime() {
            let first = Timestamp::new(13, 10);
            let second = Timestamp::new(12, 12);
            assert!(first > second)
        }
    }
}
