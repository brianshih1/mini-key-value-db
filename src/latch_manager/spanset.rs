use super::latch_interval_btree::NodeKey;

pub struct Span<K: NodeKey> {
    start_key: K,
    end_key: K,
}
