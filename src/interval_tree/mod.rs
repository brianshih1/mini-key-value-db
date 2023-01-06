use std::result::Iter;

/**
 * Inefficient implementation of IntervalTree to get an MVP.
 * Not balanced and not optimized for memory
 *
 * Based off of Robert Sedgewick's Interval Search Trees:
 * https://sedgewick.io/wp-content/uploads/2022/04/Algs10-SearchApplications.pdf
 */

struct IntervalTree<K: Copy + Eq + PartialOrd + Ord, V> {
    root_node: TreeNode<K, V>,
}

/**
 * Use start_key as BSG key.
 * Each node stores the max end_key contained in the subtree.
 */
enum TreeNode<K: Copy + Eq + PartialOrd + Ord, V> {
    Node {
        start_key: K,
        end_key: K,
        left_node: Box<TreeNode<K, V>>,
        right_node: Box<TreeNode<K, V>>,
        value: V,
        max_endpoint: K,
        parent_node: Box<TreeNode<K, V>>,
    },
    Empty,
}

impl<K: Copy + Eq + PartialOrd + Ord, V> TreeNode<K, V> {
    fn new_node(start_key: K, end_key: K, value: V) -> TreeNode<K, V> {
        TreeNode::Node {
            start_key: start_key,
            end_key: end_key,
            left_node: Box::new(TreeNode::Empty),
            right_node: Box::new(TreeNode::Empty),
            value: value,
            max_endpoint: end_key,
            parent_node: Box::new(TreeNode::Empty),
        }
    }
}

impl<K: Copy + Eq + PartialOrd + Ord, V> IntervalTree<K, V> {
    pub fn add(&mut self, start: K, end: K, value: V) -> () {
        todo!()
        // let mut node = &self.root_node;
        // match node {
        //     TreeNode::Node {
        //         mut start_key,
        //         end_key,
        //         mut left_node,
        //         right_node,
        //         ..
        //     } => {
        //         if start == start_key {
        //         } else if start > start_key {
        //         } else if start < start_key {
        //         }
        //         left_node = Box::new(TreeNode::new_node(start, end, value));
        //     }
        //     TreeNode::Empty {} => {
        //         self.root_node = TreeNode::new_node(start, end, value);
        //     }
        // }
    }

    pub fn get(&mut self, start: K, end: K) -> Option<V> {
        todo!()
    }

    pub fn intersect(&mut self, start: K, end: K) -> Iter<V> {
        todo!()
    }
}
