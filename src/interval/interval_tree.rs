use crate::{
    hlc::timestamp::Timestamp,
    llrb::llrb::{NodeKey, NodeValue, RbTree, NIL},
};

// TODO: Eviction policy to make sure IntervalTree is bounded
pub struct IntervalTree<K: NodeKey, V: NodeValue> {
    rbtree: RbTree<K, V>,
}

pub struct RangeValue<K: NodeKey, V> {
    start_key: K,
    end_key: K,
    value: V,
}

pub fn does_range_overlap<K: NodeKey>(
    first_start: &K,
    first_end: &K,
    second_start: &K,
    second_end: &K,
) -> bool {
    if first_start > second_start {
        if second_end < first_start {
            return false;
        } else {
            return true;
        }
    } else if first_start < second_start {
        if first_end >= second_start {
            return true;
        } else {
            return false;
        }
    } else {
        return true;
    }
}

impl<K: NodeKey, V: NodeValue> IntervalTree<K, V> {
    pub fn insert(&mut self, start_key: K, end_key: K, value: V) {
        self.rbtree.insert_node(start_key, end_key, value)
    }

    pub fn get_overlap(&self, start_key: K, end_key: K) -> Vec<RangeValue<K, V>> {
        todo!()
    }

    // Returns true if overlap is done. This prevents visiting nodes that doesn't need to be visited
    pub fn add_overlapping_nodes(
        &self,
        start_key: K,
        end_key: K,
        node: usize,
        overlaps: &mut Vec<RangeValue<K, V>>,
    ) -> bool {
        if node == NIL {
            return false;
        }

        let is_left_done = self.add_overlapping_nodes(
            start_key.to_owned(),
            end_key.to_owned(),
            self.rbtree.nodes[node].left_node,
            overlaps,
        );
        if is_left_done {
            return true;
        }

        if does_range_overlap(
            &start_key,
            &end_key,
            &self.rbtree.nodes[node].start_key,
            &self.rbtree.nodes[node].end_key,
        ) {
            overlaps.push(RangeValue {
                start_key: self.rbtree.nodes[node].start_key.to_owned(),
                end_key: self.rbtree.nodes[node].start_key.to_owned(),
                value: self.rbtree.nodes[node].value.clone(),
            });
        }
        if end_key < self.rbtree.nodes[node].start_key {
            return true;
        }

        return self.add_overlapping_nodes(
            start_key.to_owned(),
            end_key.to_owned(),
            self.rbtree.nodes[node].right_node,
            overlaps,
        );
    }
}
