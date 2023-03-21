use super::latch_interval_btree::NodeKey;

pub struct Span<K: NodeKey> {
    pub start_key: K,
    pub end_key: K,
}
