use std::sync::mpsc::{Receiver, Sender};

use super::{
    latch_interval_btree::{BTree, NodeKey},
    spanset::Span,
};

pub struct LatchGuard {
    sender: Sender<LatchWaitKind>,
    reciever: Receiver<LatchWaitKind>,
}

pub struct LatchManager<K: NodeKey> {
    tree: BTree<K>,
}

pub enum LatchWaitKind {
    DoneWaiting,
    Error,
}

impl<K: NodeKey> LatchManager<K> {
    // We currently don't support key-range locks. We only support single point locks
    fn acquire(&self, spans: Vec<Span<K>>) -> LatchGuard {
        todo!()
    }

    fn release(&self, guard: LatchGuard) -> () {
        todo!()
    }
}
