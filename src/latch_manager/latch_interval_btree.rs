use std::{
    borrow::Borrow,
    sync::{Arc, Mutex, RwLock, Weak},
};

use tokio::sync::mpsc::{self, Receiver, Sender};

use crate::storage::{mvcc_key::MVCCKey, Key};

#[derive(Debug, Clone)]
pub struct Range<K: NodeKey> {
    pub start_key: K,
    pub end_key: K,
}

pub trait NodeKey: std::fmt::Debug + Clone + Eq + PartialOrd + Ord {}

impl NodeKey for i32 {}
impl NodeKey for MVCCKey {}
impl NodeKey for Key {}

pub type NodeLink<K> = RwLock<Option<LatchNode<K>>>;
pub type WeakNodeLink<K> = RwLock<Option<Weak<RwLock<Node<K>>>>>;
pub type LatchNode<K> = Arc<RwLock<Node<K>>>;

#[derive(Debug)]
pub enum Node<K: NodeKey> {
    Internal(InternalNode<K>),
    Leaf(LeafNode<K>),
}

#[derive(Debug, Clone)]
pub enum Direction {
    Left,
    Right,
}

pub enum LatchKeyGuard {
    Acquired,
    NotAcquired(LatchGuardWait),
}

pub struct LatchGuardWait {
    pub receiver: Receiver<()>,
}

impl<K: NodeKey> Node<K> {
    pub fn as_internal_node(&self) -> &InternalNode<K> {
        match self {
            Node::Internal(ref node) => node,
            Node::Leaf(_) => panic!("Cannot coerce leaf node to internal node"),
        }
    }

    pub fn as_leaf_node(&self) -> &LeafNode<K> {
        match self {
            Node::Internal(_) => panic!("Cannot coerce leaf node to internal node"),
            Node::Leaf(ref node) => node,
        }
    }

    pub fn size(&self) -> usize {
        match self {
            Node::Internal(node) => node.keys.read().unwrap().len(),
            Node::Leaf(node) => node.start_keys.read().unwrap().len(),
        }
    }

    pub fn get_upper(&self) -> Option<K> {
        match self {
            Node::Internal(internal) => {
                let keys = internal.keys.read().unwrap();
                if keys.len() == 0 {
                    None
                } else {
                    Some(keys[keys.len() - 1].clone())
                }
            }
            Node::Leaf(leaf) => {
                let keys = leaf.start_keys.read().unwrap();
                let keys_len = keys.len();
                if keys_len == 0 {
                    None
                } else {
                    Some(keys[keys_len - 1].clone())
                }
            }
        }
    }

    pub fn is_underflow(&self) -> bool {
        match self {
            Node::Internal(internal) => internal.is_underflow(),
            Node::Leaf(leaf) => leaf.is_underflow(),
        }
    }

    // Returns whether a sibling can steal a key from the current node
    pub fn has_spare_key(&self) -> bool {
        match self {
            Node::Internal(internal) => internal.has_spare_key(),
            Node::Leaf(leaf) => leaf.has_spare_key(),
        }
    }

    pub fn get_lower(&self) -> Option<K> {
        match self {
            Node::Internal(internal) => {
                let keys = internal.keys.read().unwrap();
                if keys.len() == 0 {
                    None
                } else {
                    Some(keys[0].clone())
                }
            }
            Node::Leaf(leaf) => {
                let keys = leaf.start_keys.read().unwrap();
                if keys.len() == 0 {
                    None
                } else {
                    Some(keys[0].clone())
                }
            }
        }
    }

    // Just for debugging
    pub fn get_keys(&self) -> Vec<K> {
        match self {
            Node::Internal(internal) => {
                let keys = internal.keys.read().unwrap();
                keys.clone()
            }
            Node::Leaf(leaf) => {
                let keys = leaf.start_keys.read().unwrap();
                keys.clone()
            }
        }
    }

    pub fn update_key_at_index(&self, idx: usize, new_key: K) {
        match self {
            Node::Internal(internal) => {
                let mut keys = internal.keys.write().unwrap();
                keys[idx] = new_key;
            }
            Node::Leaf(_leaf) => {
                panic!("Currently don't support updating key for leaf node")
            }
        }
    }
}

// There's always one more edges than keys
// Order of 3 means each node can only store 2 keys.
#[derive(Debug)]
pub struct InternalNode<K: NodeKey> {
    pub keys: RwLock<Vec<K>>,
    // a key's corresponding left edge will contain nodes with keys stricly less
    // than the key
    pub edges: RwLock<Vec<NodeLink<K>>>,
    pub order: u16,
}

#[derive(Debug)]
pub struct LatchWaiters {
    pub senders: Vec<Mutex<Sender<()>>>,
}

#[derive(Debug)]
pub struct LeafNode<K: NodeKey> {
    pub start_keys: RwLock<Vec<K>>,
    pub end_keys: RwLock<Vec<K>>,
    pub left_ptr: WeakNodeLink<K>,
    pub right_ptr: WeakNodeLink<K>,
    pub order: u16,
    pub waiters: RwLock<Vec<RwLock<LatchWaiters>>>,
}

// impl internal
impl<K: NodeKey> InternalNode<K> {
    pub fn new(capacity: u16) -> Self {
        InternalNode {
            keys: RwLock::new(Vec::new()),
            edges: RwLock::new(Vec::new()),
            order: capacity,
        }
    }

    pub fn has_capacity(&self) -> bool {
        self.keys.read().unwrap().len() < usize::from(self.order)
    }

    pub fn update_key_at_index(&self, idx: usize, new_key: K) {
        let mut write_guard = self.keys.write().unwrap();
        write_guard[idx] = new_key;
    }

    // key is the first key of the node
    // All values in the node will be >= key. Which means it represents
    // the right edge of the key.
    // If the insert index of key K is n, then the corresponding
    // position for the node is n - 1. Note that n will never be 0
    // because insert_node gets called after a split
    pub fn insert_node(&self, node: LatchNode<K>, insert_key: K) -> () {
        // if key is greater than all elements, then the index is length of the keys (push)
        let mut keys_guard = self.keys.write().unwrap();
        let mut insert_idx = keys_guard.len();
        for (pos, k) in keys_guard.iter().enumerate() {
            if &insert_key < k {
                insert_idx = pos;
                break;
            }
        }
        keys_guard.insert(insert_idx, insert_key);
        let mut edges_guard = self.edges.write().unwrap();
        edges_guard.insert(insert_idx + 1, RwLock::new(Some(node)));
    }

    pub fn is_underflow(&self) -> bool {
        let min_nodes = self.order / 2;
        self.keys.read().unwrap().len() < min_nodes.try_into().unwrap()
    }

    // Returns whether a sibling can steal a node from the current node
    pub fn has_spare_key(&self) -> bool {
        let min_nodes = self.order / 2;
        self.keys.read().unwrap().len() > min_nodes.into()
    }

    pub fn remove_largest_key(&self) {}

    pub fn remove_smallest_key(&self) {}

    // Tries to steal nodes from siblings if they have spares.
    // Returns whether or not it successfully stole from sibling
    pub fn steal_from_sibling(&self, parent_node: &InternalNode<K>, edge_idx: usize) -> bool {
        let left_sibling = parent_node.find_child_left_sibling(edge_idx);
        let mut is_stolen = false;
        match left_sibling {
            Some(left_latch) => {
                let left_guard = left_latch.write().unwrap();
                let left_node = left_guard.as_internal_node();
                is_stolen = self.steal_from_left_sibling(left_node, parent_node, edge_idx);
                if is_stolen {
                    return true;
                }
            }
            None => {}
        };
        let right_sibling = parent_node.find_child_right_sibling(edge_idx);
        let mut is_stolen = false;
        match right_sibling {
            Some(right_latch) => {
                let right_guard = right_latch.write().unwrap();
                let right_node = right_guard.as_internal_node();
                is_stolen = self.steal_from_right_sibling(right_node, parent_node, edge_idx);
                if is_stolen {
                    return true;
                }
            }
            None => {}
        };

        is_stolen
    }

    /**
     * Algorithm:
     * - steal the right-most edge from the right sibling
     * - update the parent's split key to the split key of the stolen key
     * - set the new split key for the current internal node to the parent's split key. This is because the
     * parent's split key represents the smallest of that right subtree, so it will be the new split key for the
     * current node.
     */
    pub fn steal_from_left_sibling(
        &self,
        left_sibling: &InternalNode<K>,
        parent_node: &InternalNode<K>,
        edge_idx: usize,
    ) -> bool {
        if !left_sibling.has_spare_key() {
            return false;
        }
        // this will be the new split key for the current node
        let mut parent_keys = parent_node.keys.write().unwrap();
        let parent_split_key = parent_keys[edge_idx - 1].clone();
        let mut left_sibling_edges = left_sibling.edges.write().unwrap();
        let left_size = left_sibling_edges.len();
        let stolen_edge = left_sibling_edges.remove(left_size - 1);

        let mut left_sibling_keys = left_sibling.keys.write().unwrap();
        let left_keys_len = left_sibling_keys.len();
        let stolen_split_key = left_sibling_keys.remove(left_keys_len - 1);
        self.keys.write().unwrap().insert(0, parent_split_key);
        self.edges.write().unwrap().insert(0, stolen_edge);
        parent_keys[edge_idx - 1] = stolen_split_key;
        true
    }

    /**
     * Algorithm:
     * - Steal the left-most edge of the right sibling
     * - for the new split key for the stolen edge, use the split key to the right of the edge from the parent
     * - update the parent’s split key to use the removed split key (left-most) from the right sibling
     */
    pub fn steal_from_right_sibling(
        &self,
        right_sibling: &InternalNode<K>,
        parent_node: &InternalNode<K>,
        edge_idx: usize,
    ) -> bool {
        if !right_sibling.has_spare_key() {
            return false;
        }
        // this will be the new split key for the current node
        let mut parent_keys = parent_node.keys.write().unwrap();
        let parent_split_key = parent_keys[edge_idx].clone();

        let mut right_sibling_edges = right_sibling.edges.write().unwrap();
        let stolen_edge = right_sibling_edges.remove(0);
        // This will become parent's new split key
        let stolen_key = right_sibling.keys.write().unwrap().remove(0);
        self.keys.write().unwrap().push(parent_split_key);
        self.edges.write().unwrap().push(stolen_edge);
        parent_keys[edge_idx] = stolen_key;
        true
    }

    /**
     * Find the left sibling provided the index of the corresponding edge in the parent's node
     */
    pub fn find_child_left_sibling(&self, edge_idx: usize) -> Option<LatchNode<K>> {
        if edge_idx == 0 {
            return None;
        }
        self.edges.write().unwrap()[edge_idx - 1]
            .read()
            .unwrap()
            .clone()
    }

    /**
     * Find the right sibling provided the index of the corresponding edge in the parent's node
     */
    pub fn find_child_right_sibling(&self, edge_idx: usize) -> Option<LatchNode<K>> {
        let edges = self.edges.write().unwrap();
        if edge_idx == edges.len() - 1 {
            return None;
        }
        let x = edges[edge_idx + 1].read().unwrap().clone();
        x
    }

    /**
     * We first try to merge with left sibling if there is one.
     * Otherwise we try to merge with the right node.
     *
     * Algorithm:
     * - Remove the parent split key and put that onto the left_node
     * - Add the keys and edges from right_node to left_node
     * - Remove the edge corresponding to the right_node
     */
    pub fn merge_with_sibling(&self, parent_node: &InternalNode<K>, edge_idx: usize) {
        let left_sibling = parent_node.find_child_left_sibling(edge_idx);
        if let Some(ref left_latch) = left_sibling {
            let left_guard = left_latch.write().unwrap();
            let left_node = left_guard.as_internal_node();
            let parent_split_key = parent_node.keys.write().unwrap().remove(edge_idx - 1);
            let mut left_keys = left_node.keys.write().unwrap();
            left_keys.push(parent_split_key);
            left_keys.append(&mut self.keys.write().unwrap());
            left_node
                .edges
                .write()
                .unwrap()
                .append(&mut self.edges.write().unwrap());

            // removing the edge corresponding to the internal node since it's merged into the left node
            parent_node.edges.write().unwrap().remove(edge_idx);
        } else {
            let right_sibling = parent_node.find_child_right_sibling(edge_idx);
            if let Some(right_latch) = right_sibling {
                let right_guard = right_latch.write().unwrap();
                let right_node = right_guard.as_internal_node();
                // we merge right node into the current node
                let parent_split_key = parent_node.keys.write().unwrap().remove(edge_idx);
                let mut current_keys = self.keys.write().unwrap();
                current_keys.push(parent_split_key);
                current_keys.append(&mut right_node.keys.write().unwrap());
                self.edges
                    .write()
                    .unwrap()
                    .append(&mut right_node.edges.write().unwrap());

                // removing the edge corresponding to the right node since we are merging into the current node
                parent_node.edges.write().unwrap().remove(edge_idx + 1);
            }
        }
    }

    pub fn contains_key(&self, key: &K) -> bool {
        self.keys.read().unwrap().contains(key)
    }

    pub fn deal_with_underflow(&self, parent_node: Option<&InternalNode<K>>, edge_idx: usize) {
        // If there is no parent_node, then the current node is the root. We allow root to underflow.
        if let Some(parent_node) = parent_node {
            if self.is_underflow() {
                let left_sibling = parent_node.find_child_left_sibling(edge_idx);
                let mut is_stolen = false;
                if let Some(ref left_latch) = left_sibling {
                    let left_guard = left_latch.write().unwrap();
                    is_stolen = self.steal_from_left_sibling(
                        left_guard.as_internal_node(),
                        parent_node,
                        edge_idx,
                    );
                }
                if !is_stolen {
                    let right_sibling = parent_node.find_child_right_sibling(edge_idx);
                    if let Some(ref right_latch) = right_sibling {
                        let right_guard = right_latch.write().unwrap();
                        is_stolen = self.steal_from_right_sibling(
                            right_guard.as_internal_node(),
                            parent_node,
                            edge_idx,
                        );
                    }
                }
                if !is_stolen {
                    self.merge_with_sibling(parent_node, edge_idx);
                }
            }
        }
    }

    pub fn split(&self) -> (LatchNode<K>, K) {
        //
        // Suppose we have an internal node:
        // a 0 b 5 c 10 d
        // where numbers represents nodes and letters represent edges.
        // After splitting, we get:
        // left: a 0 b
        // right: e 5 c 10 d
        // The reason for this is that 5 will be pushed up and since
        // node corresponding to b must be less than 5 it must be
        // to the left of the mid key that gets pushed up
        //
        let mid_idx = self.keys.read().unwrap().len() / 2;
        let mut right_keys = self.keys.write().unwrap().split_off(mid_idx);
        let mut right_edges = self.edges.write().unwrap().split_off(mid_idx + 1);
        right_edges.insert(0, RwLock::new(None));
        let right_start = right_keys.remove(0);
        right_edges.remove(0);
        let new_right_node = InternalNode {
            keys: RwLock::new(right_keys),
            edges: RwLock::new(right_edges),
            order: self.order,
        };
        (
            Arc::new(RwLock::new(Node::Internal(new_right_node))),
            right_start,
        )
    }
}

// impl leaf
impl<K: NodeKey> LeafNode<K> {
    pub fn new(capacity: u16) -> Self {
        LeafNode {
            start_keys: RwLock::new(Vec::new()),
            end_keys: RwLock::new(Vec::new()),
            left_ptr: RwLock::new(None),
            right_ptr: RwLock::new(None),
            order: capacity,
            waiters: RwLock::new(Vec::from([RwLock::new(LatchWaiters {
                senders: Vec::new(),
            })])),
        }
    }

    // order 4 means at most 3 keys per node
    pub fn has_capacity(&self) -> bool {
        self.start_keys.read().unwrap().len() < usize::from(self.order)
    }

    pub fn get_lower(&self) -> Option<K> {
        let keys = self.start_keys.read().unwrap();
        if keys.len() == 0 {
            None
        } else {
            Some(keys[0].clone())
        }
    }

    /**
     * Just inserts, doesn't check for overflow and not responsible for splitting.
     */
    pub fn insert_range(&self, range: Range<K>) -> LatchKeyGuard {
        let read_guard = self.start_keys.read();
        let index = read_guard
            .unwrap()
            .iter()
            .position(|r| *r == range.start_key);

        if let Some(found_idx) = index {
            println!("Key: {:?}", &range.start_key);
            let waiters = self.waiters.write().unwrap();
            let mut waiter = waiters[found_idx].write().unwrap();
            let (tx, rx) = mpsc::channel::<()>(1);
            waiter.senders.push(Mutex::new(tx));
            return LatchKeyGuard::NotAcquired(LatchGuardWait { receiver: rx });
        }

        let mut insert_idx = self.start_keys.read().unwrap().len();
        for (pos, k) in self.start_keys.read().unwrap().iter().enumerate() {
            if &range.start_key < k {
                insert_idx = pos;
                break;
            }
        }

        self.start_keys
            .write()
            .unwrap()
            .insert(insert_idx, range.start_key);
        self.end_keys
            .write()
            .unwrap()
            .insert(insert_idx, range.end_key);
        return LatchKeyGuard::Acquired;
    }

    pub fn find_key_idx(&self, key: &K) -> Option<usize> {
        for (idx, k) in self.start_keys.read().unwrap().iter().enumerate() {
            if k == key {
                return Some(idx);
            }
        }
        None
    }

    pub fn find_next_larger_key(&self, key: &K) -> Option<usize> {
        for (idx, k) in self.start_keys.read().unwrap().iter().enumerate() {
            if k > key {
                return Some(idx);
            }
        }
        None
    }

    // Returns true if a key was removed, false if key not found
    pub fn remove_key(&self, key_to_delete: K) -> bool {
        let idx = self.find_key_idx(&key_to_delete);
        match idx {
            Some(idx) => {
                self.start_keys.write().unwrap().remove(idx);
                self.end_keys.write().unwrap().remove(idx);
                true
            }
            None => false,
        }
    }

    pub fn is_underflow(&self) -> bool {
        let min_nodes = self.order / 2;
        self.start_keys.read().unwrap().len() < min_nodes.try_into().unwrap()
    }

    // Returns whether a sibling can steal a node from the current node
    pub fn has_spare_key(&self) -> bool {
        let min_nodes = self.order / 2;
        self.start_keys.read().unwrap().len() > min_nodes.into()
    }

    pub fn get_smallest_key(&self) -> K {
        self.start_keys.read().unwrap().first().unwrap().clone()
    }

    // Returns the stolen key
    pub fn steal_smallest_key(&self) -> Range<K> {
        if !self.has_spare_key() {
            panic!("Cannot steal key from leaf, will underflow")
        }
        let start_key = self.start_keys.write().unwrap().remove(0);
        let end_key = self.end_keys.write().unwrap().remove(0);
        Range { start_key, end_key }
    }

    pub fn get_largest(&self) -> K {
        self.start_keys.read().unwrap().last().unwrap().clone()
    }

    // Returns the stolen key
    pub fn steal_biggest_key(&self) -> Range<K> {
        if !self.has_spare_key() {
            panic!("Cannot steal key from leaf, will underflow")
        }
        let idx = self.start_keys.read().unwrap().len() - 1;
        let start_key = self.start_keys.write().unwrap().remove(idx);
        let end_key = self.end_keys.write().unwrap().remove(idx);
        Range { start_key, end_key }
    }

    /**
     * Returns whether it was stolen from right sibling as well as what to
     * update its ancestor split key to.
     *
     * If any ancestor's key is the deleted key, since the leaf node steals from right sibling,
     * the split key is now the stolen key from the right leaf sibling.
     *
     * Note that the key to update depends on if the edge is to the right or left of the key.
     */
    pub fn steal_from_right_leaf_sibling(
        &self,
        _key_to_delete: &K,
        right_latch_node: LatchNode<K>,
        parent: &InternalNode<K>,
        edge_idx: usize,
        dir: Direction,
    ) -> (bool, Option<K>) {
        let right_guard = right_latch_node.write().unwrap();
        let right_leaf_sibling = right_guard.as_leaf_node();
        if right_leaf_sibling.has_spare_key() {
            let stolen_range = right_leaf_sibling.steal_smallest_key();
            let stolen_key = stolen_range.start_key.clone();
            self.insert_range(stolen_range);

            let key_idx = match dir {
                Direction::Left => edge_idx,
                Direction::Right => edge_idx - 1,
            };
            // Update parent's split key. Since we are stealing from right sibling,
            // the new split_key will be the right sibling's new smallest key
            parent.update_key_at_index(key_idx, right_leaf_sibling.get_lower().unwrap());

            return (true, Some(stolen_key.clone()));
        }
        (false, None)
    }

    pub fn steal_from_left_leaf_sibling(
        &self,
        _key_to_delete: &K,
        left_latch_node: LatchNode<K>,
        parent: &InternalNode<K>,
        edge_idx: usize,
    ) -> bool {
        let left_guard = left_latch_node.read().unwrap();
        let left_leaf_sibling = left_guard.as_leaf_node();
        if left_leaf_sibling.has_spare_key() {
            let stolen_range = left_leaf_sibling.steal_biggest_key();
            let stolen_key = stolen_range.start_key.clone();
            self.insert_range(stolen_range);

            // Update parent's split key. Since we are stealing from left sibling,
            // the new split_key will be the stolen key
            parent.update_key_at_index(edge_idx - 1, stolen_key);
            return true;
        }
        false
    }

    /**
     * - First check if there is a left node. If there is, we assume there is no spare mode since
     * we would've stolen from it if otherwise.
     * - We then merge the left node's start key and left key into the current node.
     * - Then we update the current left node's left ptr to the left ptr's left ptr.
     * - We then update the parent node to remove the split node between the merged nodes.
     *
     * We apply the same to the right node if there is no left node
     */
    pub fn merge_node(&self, parent_node: &InternalNode<K>, edge_idx: usize) {
        let left_sibling = parent_node.find_child_left_sibling(edge_idx);
        match left_sibling {
            Some(left_latch) => {
                let left_guard = left_latch.write().unwrap();
                // merge current node into left node
                let left_node = left_guard.as_leaf_node();
                left_node
                    .start_keys
                    .write()
                    .unwrap()
                    .append(&mut self.start_keys.write().unwrap());
                left_node
                    .end_keys
                    .write()
                    .unwrap()
                    .append(&mut self.end_keys.write().unwrap());
                // edge_idx - 1 | split_key | edge_idx
                // We want to remove edge_idx and split_key (will be edge_idx - 1 in coresponding keys vec)
                let mut parent_edges_guard = parent_node.edges.write().unwrap();
                parent_edges_guard.remove(edge_idx);
                parent_node.keys.write().unwrap().remove(edge_idx - 1);
                *left_node.right_ptr.write().unwrap() = self.right_ptr.write().unwrap().take();
            }
            None => {
                let right_sibling = parent_node.find_child_right_sibling(edge_idx);
                match right_sibling {
                    Some(right_latch) => {
                        let right_guard = right_latch.write().unwrap();
                        // merge right node into current node
                        let right_node = right_guard.as_leaf_node();
                        self.start_keys
                            .write()
                            .unwrap()
                            .append(&mut right_node.start_keys.write().unwrap());
                        self.end_keys
                            .write()
                            .unwrap()
                            .append(&mut right_node.end_keys.write().unwrap());

                        // edge_idx | split_key | edge_idx + 1
                        // We want to remove edge_idx + 1 and split_key (will be edge_idx in coresponding keys vec)
                        parent_node.edges.write().unwrap().remove(edge_idx + 1);
                        parent_node.keys.write().unwrap().remove(edge_idx);
                        *self.right_ptr.write().unwrap() =
                            right_node.right_ptr.write().unwrap().take();
                    }
                    None => {
                        todo!()
                    }
                };
            }
        }
    }

    // given a leaf node and a key for index, and potentially it's right sibling, find the next largest key.
    pub fn find_next_largest_key(
        &self,
        key_to_delete: &K,
        right_sibling_option: &Option<LatchNode<K>>,
    ) -> K {
        let idx = self.find_next_larger_key(key_to_delete);

        match idx {
            Some(idx) => {
                return self.start_keys.read().unwrap()[idx].clone();
            }
            None => {
                // This means that the next biggest key is not in the same leaf node
                let right_leaf_option = right_sibling_option.clone();
                let right_leaf = right_leaf_option.unwrap();
                let right_guard = right_leaf.read().unwrap();
                return right_guard.as_leaf_node().start_keys.read().unwrap()[0].clone();
            }
        }
    }

    /**
     * Returns an optional value the parent's split key should update
     * to if the parent key matches the delete_key
     *
     * Algorithm:
     * - deletes the key from node
     * - if it doesn't underflow, stop. We update the ancestors as following:
     *      if the ancestor uses the deleted key as split key and the deleted key is in the right subtree
     *      then the split key will be the next largest node. Note that if the deleted key
     *      is on the left tree it will not be the split key by definition.
     * - if it underflows, steal from either left or right sibling if they have a node to spare
     * - otherwise, merge node
     */
    pub fn delete_key(
        &self,
        key_to_delete: &K,
        parent_info: Option<(&InternalNode<K>, usize, Direction)>,
    ) -> Option<K> {
        let is_deleted = self.remove_key(key_to_delete.clone());

        if !is_deleted {
            return None;
        }

        // if there is no parent, then the leaf is the only element. We will allow root to underflow
        match parent_info {
            Some((parent_node, edge_idx, dir)) => {
                let right_sibling_option = parent_node.find_child_right_sibling(edge_idx);
                let left_sibling_option = parent_node.find_child_left_sibling(edge_idx);
                if !self.is_underflow() {
                    let next_key = self.find_next_largest_key(key_to_delete, &right_sibling_option);

                    return Some(next_key);
                }
                let mut new_split_key = None;
                let mut is_stolen = false;
                // try to borrow left sibling for a key
                if let Some(left_sibling) = left_sibling_option {
                    is_stolen = self.steal_from_left_leaf_sibling(
                        &key_to_delete,
                        left_sibling,
                        parent_node,
                        edge_idx,
                    );
                }
                // try to borrow right sibling for a key
                if !is_stolen {
                    if let Some(right_sibling) = right_sibling_option {
                        let (did_steal_right_sibling, right_stolen_key) = self
                            .steal_from_right_leaf_sibling(
                                &key_to_delete,
                                right_sibling,
                                parent_node,
                                edge_idx,
                                dir,
                            );
                        new_split_key = right_stolen_key;
                        is_stolen = did_steal_right_sibling;
                    }
                }

                // Can't borrow from either siblings. In this case we merge
                if !is_stolen {
                    self.merge_node(parent_node, edge_idx);
                    return None;
                }
                new_split_key
            }
            None => {
                // if there is no parent, then the leaf is the only element. We will allow root to underflow
                return None;
            }
        }
    }

    /**
     * Allocate a new leaf node and move half keys to the new node.
     * Returns the new node and the smallest key in the new node.
     */
    pub fn split(&self, node: &LatchNode<K>) -> (LatchNode<K>, K) {
        let mid = self.start_keys.read().unwrap().len() / 2;
        let right_start_keys = self.start_keys.write().unwrap().split_off(mid);

        let right_end_keys = self.end_keys.write().unwrap().split_off(mid);
        let right_sibling = self.right_ptr.write().unwrap().take();
        let right_start = right_start_keys[0].clone();

        let new_right_node = LeafNode {
            start_keys: RwLock::new(right_start_keys),
            end_keys: RwLock::new(right_end_keys),
            left_ptr: RwLock::new(Some(Arc::downgrade(node))), // TODO: set the left_sibling to the current leaf node later
            right_ptr: RwLock::new(right_sibling),
            order: self.order,
            waiters: RwLock::new(Vec::from([RwLock::new(LatchWaiters {
                senders: Vec::new(),
            })])),
        };
        let right_latch_node = Arc::new(RwLock::new(Node::Leaf(new_right_node)));
        self.right_ptr
            .write()
            .unwrap()
            .replace(Arc::downgrade(&right_latch_node));
        (right_latch_node, right_start)
    }
}

// Order of 3 means each node can only store 2 keys.
pub struct BTree<K: NodeKey> {
    pub root: NodeLink<K>,
    pub order: u16,
}

impl<K: NodeKey> BTree<K> {
    pub fn new(capacity: u16) -> Self {
        BTree {
            root: RwLock::new(Some(Arc::new(RwLock::new(Node::Leaf(LeafNode::new(
                capacity,
            )))))),
            order: capacity,
        }
    }

    /**
     * First search for which leaf node the new key should go into.
     * If the leaf is not at capacity, insert it.
     * Otherwise, insert and split the leaf:
     * - create a new leaf node and move half of the keys to the new node
     * - insert the new leaf's smallest key to the parent node
     * - if parent is full, split it too. Keep repeating the process until a parent doesn't need to split
     * - if the root splits, create a new root with one key and two children
     */
    pub fn insert_helper(
        &self,
        parent_node: Option<&InternalNode<K>>,
        node: LatchNode<K>,
        range: &Range<K>,
    ) -> LatchKeyGuard {
        let key_to_add = &range.start_key;
        let write_guard = node.write().unwrap();
        match &*write_guard {
            Node::Internal(internal_node) => {
                let mut next = None;
                for (idx, k) in internal_node.keys.read().unwrap().iter().enumerate() {
                    if key_to_add < k {
                        let next_lock = &internal_node.edges.write().unwrap()[idx];
                        next = next_lock.write().unwrap().clone();
                        break;
                    }

                    if idx == internal_node.keys.read().unwrap().len() - 1 {
                        let next_lock = &internal_node.edges.write().unwrap()[idx + 1];
                        next = next_lock.write().unwrap().clone();
                    }
                }
                let next_node = next.unwrap();

                let child_node_size = next_node.read().unwrap().size();

                // The capacity is self.order - 1 since order represents how many edges a node has
                let is_safe = child_node_size < usize::from(self.order - 1);
                // A thread can release latch on a parent node if its child node is considered safe.
                // In the case of insert, it's safe when the child is not full
                if is_safe {
                    drop(internal_node);
                    drop(write_guard);
                    let res = self.insert_helper(None, next_node.clone(), range);
                    res
                } else {
                    let res = self.insert_helper(Some(internal_node), next_node.clone(), range);
                    if let LatchKeyGuard::Acquired = res {
                        if !internal_node.has_capacity() {
                            let (split_node, median) = internal_node.split();
                            match parent_node {
                                Some(parent_node) => {
                                    parent_node.insert_node(split_node.clone(), median.clone());
                                }
                                None => {
                                    // TODO: This means the current node is the root node. In this case, we create a new root with one key and 2 children
                                    self.root.write().unwrap().replace(Arc::new(RwLock::new(
                                        Node::Internal(InternalNode {
                                            keys: RwLock::new(Vec::from([median.clone()])),
                                            edges: RwLock::new(Vec::from([
                                                RwLock::new(Some(node.clone())),
                                                RwLock::new(Some(split_node.clone())),
                                            ])),
                                            order: self.order,
                                        }),
                                    )));
                                }
                            }
                        }
                    }
                    res
                }
            }
            Node::Leaf(leaf_node) => {
                let res = leaf_node.insert_range(Range {
                    start_key: range.start_key.clone(),
                    end_key: range.end_key.clone(),
                });
                if let LatchKeyGuard::Acquired = res {
                    if !leaf_node.has_capacity() {
                        let (split_node, median) = leaf_node.split(&node);

                        match parent_node {
                            Some(parent_node) => {
                                parent_node.insert_node(split_node.clone(), median.clone());
                            }
                            None => {
                                // This means the current node is the root node. In this case, we create a new root with one key and 2 children
                                self.root.write().unwrap().replace(Arc::new(RwLock::new(
                                    Node::Internal(InternalNode {
                                        keys: RwLock::new(Vec::from([median.clone()])),
                                        edges: RwLock::new(Vec::from([
                                            RwLock::new(Some(node.clone())),
                                            RwLock::new(Some(split_node.clone())),
                                        ])),
                                        order: self.order,
                                    }),
                                )));
                            }
                        };
                    }
                }
                res
            }
        }
    }

    pub fn insert(&self, range: Range<K>) -> LatchKeyGuard {
        let node = self.root.write().unwrap().clone().unwrap();
        self.insert_helper(None, node, &range)
    }

    /**
     * - Find the leaf where the key exists
     * - Remove the key
     * - If the node didn't underflow, stop
     * - if the node underflows.
     *      - if either the left or right sibling has a node to spare, steal the node.
     *        update the keys in the parent since the split point has changed (this involves simply
     *        changing a key above, no deletion or insertion)
     *      - if neither siblings have node to spare, the merge the node with its sibling. If the node
     *        is internal, we will need to incorporate the split key from the parent into the merging.
     *        In either case, we will need to repeat the removal algorithm on the parent node to remove the split
     *        key that previously separated these merged nodes unless the parent is the root and we are removing
     *        the finaly key from the root. In which case the merged node becomes the new root.
     *
     * TODO: if the ancestor's node matches key_to_delete, it also needs to stay locked
     */
    pub fn delete_helper(
        &self,
        key_to_delete: &K,
        parent_info: Option<(&InternalNode<K>, usize, Direction)>,
        current_node: LatchNode<K>,
        is_ancestor_safe: bool,
    ) -> Option<K> {
        let write_guard = current_node.write().unwrap();
        match &*write_guard {
            Node::Internal(internal_node) => {
                let mut next_node_tuple = None;
                let mut edge_idx_option = None;
                for (idx, k) in internal_node.keys.read().unwrap().iter().enumerate() {
                    if key_to_delete < k {
                        edge_idx_option = Some(idx);
                        next_node_tuple = Some((
                            internal_node.edges.write().unwrap()[idx]
                                .read()
                                .unwrap()
                                .clone()
                                .unwrap(),
                            idx,
                            Direction::Left,
                        ));
                        break;
                    }

                    if idx == internal_node.keys.read().unwrap().len() - 1 {
                        edge_idx_option = Some(idx + 1);
                        next_node_tuple = Some((
                            internal_node.edges.read().unwrap()[idx + 1]
                                .read()
                                .unwrap()
                                .clone()
                                .unwrap(),
                            idx + 1,
                            Direction::Right,
                        ));
                        break;
                    }
                }
                let (next_node, _, dir) = next_node_tuple.unwrap();

                let edge_idx = edge_idx_option.unwrap();
                let key_idx = match dir {
                    Direction::Left => edge_idx,
                    Direction::Right => edge_idx - 1,
                };
                let child_has_spare_key = next_node.read().unwrap().has_spare_key();
                let is_edge_key_the_delete_key =
                    &internal_node.keys.read().unwrap()[key_idx] == key_to_delete;

                // A thread can release latch on a parent node if its child node is considered safe.
                // In the case of delete, it's safe when the child is half-full
                let is_safe =
                    child_has_spare_key && !is_edge_key_the_delete_key && is_ancestor_safe;

                // A thread can release latch on a parent node if its child node is considered safe.
                // In the case of delete, it's safe when the child is half-full
                if is_safe {
                    drop(internal_node);
                    drop(write_guard);
                    let new_split_key_option =
                        self.delete_helper(key_to_delete, None, next_node, true);
                    new_split_key_option
                } else {
                    let new_split_key_option = self.delete_helper(
                        key_to_delete,
                        Some((internal_node, edge_idx, dir.clone())),
                        next_node,
                        !is_edge_key_the_delete_key && is_ancestor_safe,
                    );

                    let parent_option = parent_info.and_then(|(parent, _, _)| Some(parent));
                    // Do we need to account for child's deal_with_underflow messing with the indices?
                    if let Some(ref new_split_key) = new_split_key_option {
                        if let Some(parent_node) = parent_option {
                            let key_idx = match dir {
                                Direction::Left => edge_idx,
                                Direction::Right => edge_idx - 1,
                            };
                            parent_node.update_key_at_index(key_idx, new_split_key.clone())
                        }
                    }
                    internal_node.deal_with_underflow(parent_option, edge_idx);
                    new_split_key_option
                }
            }
            Node::Leaf(leaf_node) => leaf_node.delete_key(
                &key_to_delete,
                parent_info.and_then(|(a, b, c)| Some((a, b, c))),
            ),
        }
    }

    /**
     * - Find the leaf where the key exists
     * - Remove the key
     * - If the node didn't underflow, stop
     * - if the node underflows.
     *      - if either the left or right sibling has a node to spare, steal the node.
     *        update the keys in the parent since the split point has changed (this involves simply
     *        changing a key above, no deletion or insertion)
     *      - if neither siblings have node to spare, the merge the node with its sibling. If the node
     *        is internal, we will need to incorporate the split key from the parent into the merging.
     *        In either case, we will need to repeat the removal algorithm on the parent node to remove the split
     *        key that previously separated these merged nodes unless the parent is the root and we are removing
     *        the finaly key from the root. In which case the merged node becomes the new root.
     *
     * TODO: if the ancestor's node matches key_to_delete, it also needs to stay locked
     */
    pub fn delete(&self, key_to_delete: K) -> () {
        let root_node = self.root.read().unwrap().clone().unwrap();
        self.delete_helper(&key_to_delete, None, root_node.clone(), true);
        let root_guard = root_node.write().unwrap();
        match &*root_guard {
            Node::Internal(ref internal_node) => {
                if internal_node.keys.read().unwrap().len() == 0 {
                    let new_root = internal_node.edges.read().unwrap()[0]
                        .borrow()
                        .read()
                        .unwrap()
                        .clone()
                        .unwrap();
                    self.root.write().unwrap().replace(new_root);
                }
            }
            Node::Leaf(_) => {}
        }
    }
}
