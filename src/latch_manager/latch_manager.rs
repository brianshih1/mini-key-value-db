use super::{
    latch_interval_btree::{BTree, NodeKey},
    spanset::Span,
};

pub struct LatchGuard {}

pub struct LatchManager<K: NodeKey> {
    tree: BTree<K>,
}

impl<K: NodeKey> LatchManager<K> {
    // TODO: We currently don't support key-range locks. We only support single point locks
    fn acquire(&self, spans: Vec<Span<K>>) -> LatchGuard {
        todo!()
    }

    fn release(&self, guard: LatchGuard) -> () {
        todo!()
    }
}
