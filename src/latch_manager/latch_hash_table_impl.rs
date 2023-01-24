use std::sync::RwLock;

use super::latch_manager::{LatchGuard, LatchManager};

struct Queue {}

const DEFAULT_CAPACITY: usize = 10;

pub struct HashTableLatchManager {
    slots: Vec<RwLock<Queue>>,
    capacity: usize,
}

impl HashTableLatchManager {
    fn new_default_capacity() -> Self {
        let mut vec = Vec::with_capacity(DEFAULT_CAPACITY);
        (0..DEFAULT_CAPACITY).for_each(|_| vec.push(RwLock::new(Queue {})));
        HashTableLatchManager {
            slots: vec,
            capacity: DEFAULT_CAPACITY,
        }
    }

    fn new(capacity: usize) -> Self {
        let mut vec = Vec::with_capacity(capacity);
        (0..capacity).for_each(|_| vec.push(RwLock::new(Queue {})));
        HashTableLatchManager {
            slots: vec,
            capacity: DEFAULT_CAPACITY,
        }
    }
}

impl LatchManager for HashTableLatchManager {
    fn acquire(&self) -> LatchGuard {
        todo!()
    }

    fn release(&self, guard: LatchGuard) -> () {
        todo!()
    }
}
