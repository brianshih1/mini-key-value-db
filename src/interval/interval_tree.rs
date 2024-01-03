use crate::llrb::llrb::{NodeKey, NodeValue, RbTree, NIL};

// TODO: Eviction policy to make sure IntervalTree is bounded
pub struct IntervalTree<K: NodeKey, V: NodeValue> {
    rbtree: RbTree<K, V>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct RangeValue<K: NodeKey, V> {
    pub start_key: K,
    pub end_key: K,
    pub value: V,
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
    pub fn new() -> Self {
        let rbtree = RbTree::<K, V>::new();
        IntervalTree { rbtree }
    }

    pub fn insert(&mut self, start_key: K, end_key: K, value: V) {
        self.rbtree.insert_node(start_key, end_key, value)
    }

    pub fn get_overlap(&self, start_key: K, end_key: K) -> Vec<RangeValue<K, V>> {
        let mut vec = Vec::new();
        self.collect_overlapping_nodes(start_key, end_key, self.rbtree.root, &mut vec);
        vec
    }

    // Returns true if overlap is done. This prevents visiting nodes that doesn't need to be visited
    fn collect_overlapping_nodes(
        &self,
        start_key: K,
        end_key: K,
        node: usize,
        overlaps: &mut Vec<RangeValue<K, V>>,
    ) -> bool {
        if node == NIL {
            return false;
        }

        let is_left_done = self.collect_overlapping_nodes(
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
                end_key: self.rbtree.nodes[node].end_key.to_owned(),
                value: self.rbtree.nodes[node].value.clone(),
            });
        }
        if end_key < self.rbtree.nodes[node].start_key {
            return true;
        }

        return self.collect_overlapping_nodes(
            start_key.to_owned(),
            end_key.to_owned(),
            self.rbtree.nodes[node].right_node,
            overlaps,
        );
    }
}

#[cfg(test)]
mod tests {

    #[cfg(test)]
    mod get_overlap {
        use crate::interval::interval_tree::{IntervalTree, RangeValue};

        #[test]
        fn test_overlap() {
            let mut tree = IntervalTree::<i32, i32>::new();
            tree.insert(0, 5, 12);
            tree.insert(3, 7, 13);
            tree.insert(4, 5, 8);
            tree.insert(11, 2, 5);
            let overlaps = tree.get_overlap(1, 5);
            assert_eq!(
                overlaps,
                Vec::from([
                    RangeValue {
                        start_key: 0,
                        end_key: 5,
                        value: 12
                    },
                    RangeValue {
                        start_key: 3,
                        end_key: 7,
                        value: 13
                    },
                    RangeValue {
                        start_key: 4,
                        end_key: 5,
                        value: 8
                    }
                ])
            );
        }

        #[test]
        fn experiment() {
            // let test = Rc::new(Test { prop1: true });
            // let cell = RefCell::new(test.clone());
            // let cell2 = RefCell::new(test.clone());
            // test.toggle_bool();
        }
    }
}

struct Test {
    prop1: bool,
}

impl Test {
    fn toggle_bool(&mut self) {
        self.prop1 = !self.prop1
    }
}
