use std::{
    borrow::{Borrow, BorrowMut},
    cell::RefCell,
    mem,
    rc::{Rc, Weak},
    sync::{Arc, PoisonError, RwLock, RwLockReadGuard, RwLockWriteGuard},
};

use self::Test::{print_node, print_tree};

struct Foo {}

pub trait NodeKey: std::fmt::Debug + Clone + Eq + PartialOrd + Ord {}

impl NodeKey for i32 {}

type LatchNode<K: NodeKey> = Arc<RwLock<Node<K>>>;
type NodeLink<K: NodeKey> = RefCell<Option<LatchNode<K>>>;
// RefCell<Option<Rc<RBTNode<T>>>>
type WeakNodeLink<K: NodeKey> = RefCell<Option<Weak<RwLock<Node<K>>>>>;
// RefCell<Option<Weak<RBTNode<T>>>>,

#[derive(Debug, Clone)]
pub enum Node<K: NodeKey> {
    Internal(InternalNode<K>),
    Leaf(LeafNode<K>),
}

#[derive(Debug, Clone)]
pub enum Direction {
    Left,
    Right,
}

#[derive(Debug, Clone)]
pub enum LatchIntent {
    DELETE,
    INSERT,
    SEARCH,
}

impl<K: NodeKey> Node<K> {
    pub fn acquire_read_lock(&self) -> RwLockReadGuard<()> {
        match self {
            Node::Internal(internal_node) => internal_node.acquire_read_lock(),
            Node::Leaf(leaf_node) => leaf_node.acquire_read_lock(),
        }
    }

    pub fn acquire_write_lock(&self) -> RwLockWriteGuard<()> {
        match self {
            Node::Internal(internal_node) => internal_node.acquire_write_lock(),
            Node::Leaf(leaf_node) => leaf_node.acquire_write_lock(),
        }
    }

    /**
     * A thread can release latch on a parent node if its child node
     * considered safe. It is safe when:
     * - the node won't split or merge when updated
     * - not full (on insertion)
     * - more than half full (on deletion)
     */
    pub fn is_safe_to_release_parent_latch(&self, intent: LatchIntent) -> bool {
        match self {
            Node::Internal(internal_node) => internal_node.is_safe_to_release_parent_latch(intent),
            Node::Leaf(leaf_node) => leaf_node.is_safe_to_release_parent_latch(intent),
        }
    }

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

    pub fn get_upper(&self) -> Option<K> {
        match self {
            Node::Internal(internal) => {
                let keys = internal.keys.borrow_mut();
                if keys.len() == 0 {
                    None
                } else {
                    Some(keys[keys.len() - 1].clone())
                }
            }
            Node::Leaf(leaf) => {
                let keys = leaf.start_keys.borrow_mut();
                if keys.len() == 0 {
                    None
                } else {
                    Some(keys[keys.len() - 1].clone())
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
                let keys = internal.keys.borrow_mut();
                if keys.len() == 0 {
                    None
                } else {
                    Some(keys[0].clone())
                }
            }
            Node::Leaf(leaf) => {
                let keys = leaf.start_keys.borrow_mut();
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
                let keys = internal.keys.borrow_mut();
                keys.clone()
            }
            Node::Leaf(leaf) => {
                let keys = leaf.start_keys.borrow_mut();
                keys.clone()
            }
        }
    }

    pub fn update_key_at_index(&self, idx: usize, new_key: K) {
        match self {
            Node::Internal(internal) => {
                internal.keys.borrow_mut()[idx] = new_key;
            }
            Node::Leaf(leaf) => {
                panic!("Currently don't support updating key for leaf node")
            }
        }
    }
}

// There's always one more edges than keys
// Order of 3 means each node can only store 2 keys.
#[derive(Debug, Clone)]
pub struct InternalNode<K: NodeKey> {
    keys: RefCell<Vec<K>>,
    // a key's corresponding left edge will contain nodes with keys stricly less
    // than the key
    edges: RefCell<Vec<NodeLink<K>>>,
    order: u16,
    rw_lock: Rc<RwLock<()>>,
}

#[derive(Debug, Clone)]
pub struct LeafNode<K: NodeKey> {
    start_keys: RefCell<Vec<K>>,
    end_keys: RefCell<Vec<K>>,
    left_ptr: WeakNodeLink<K>,
    right_ptr: WeakNodeLink<K>,
    order: u16,
    rw_lock: Rc<RwLock<()>>,
}

// impl internal
impl<K: NodeKey> InternalNode<K> {
    pub fn new(capacity: u16) -> Self {
        InternalNode {
            keys: RefCell::new(Vec::new()),
            edges: RefCell::new(Vec::new()),
            order: capacity,
            rw_lock: Rc::new(RwLock::new(())),
        }
    }

    pub fn acquire_read_lock(&self) -> RwLockReadGuard<()> {
        let lock = self.rw_lock.as_ref();
        lock.read().unwrap()
    }

    pub fn acquire_write_lock(&self) -> RwLockWriteGuard<()> {
        let lock = self.rw_lock.as_ref();
        lock.write().unwrap()
    }

    /**
     * A thread can release latch on a parent node if its child node
     * considered safe. It is safe when:
     * - the node won't split or merge when updated
     * - not full (on insertion)
     * - more than half full (on deletion)
     */
    pub fn is_safe_to_release_parent_latch(&self, intent: LatchIntent) -> bool {
        match intent {
            LatchIntent::DELETE => self.has_spare_key(),
            LatchIntent::INSERT => self.keys.borrow().len() + 1 < usize::from(self.order),
            LatchIntent::SEARCH => true,
        }
    }

    pub fn has_capacity(&self) -> bool {
        self.keys.borrow().len() < usize::from(self.order)
    }

    // key is the first key of the node
    // All values in the node will be >= key. Which means it represents
    // the right edge of the key.
    // If the insert index of key K is n, then the corresponding
    // position for the node is n - 1. Note that n will never be 0
    // because insert_node gets called after a split
    pub fn insert_node(&self, node: LatchNode<K>, insert_key: K) -> () {
        // if key is greater than all elements, then the index is length of the keys (push)
        let mut insert_idx = self.keys.borrow().len();
        for (pos, k) in self.keys.borrow().iter().enumerate() {
            if &insert_key < k {
                insert_idx = pos;
                break;
            }
        }
        self.keys.borrow_mut().insert(insert_idx, insert_key);
        self.edges
            .borrow_mut()
            .insert(insert_idx + 1, RefCell::new(Some(node)));
    }

    pub fn is_underflow(&self) -> bool {
        let min_nodes = self.order / 2;
        self.keys.borrow().len() < min_nodes.try_into().unwrap()
    }

    // Returns whether a sibling can steal a node from the current node
    pub fn has_spare_key(&self) -> bool {
        let min_nodes = self.order / 2;
        self.keys.borrow().len() > min_nodes.into()
    }

    pub fn remove_largest_key(&self) {}

    pub fn remove_smallest_key(&self) {}

    // Tries to steal nodes from siblings if they have spares.
    // Returns whether or not it successfully stole from sibling
    pub fn steal_from_sibling(&self, parent_node: &InternalNode<K>, edge_idx: usize) -> bool {
        let left_sibling = parent_node.find_child_left_sibling(edge_idx);
        let mut is_stolen = false;
        match left_sibling {
            Some(left_latch_node) => {
                let left_guard = left_latch_node.as_ref().write().unwrap();
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
            Some(right_latch_node) => {
                let right_guard = right_latch_node.as_ref().write().unwrap();
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
        let parent_split_key = parent_node.keys.borrow()[edge_idx - 1].clone();
        let left_size = left_sibling.edges.borrow().len();
        let stolen_edge = left_sibling.edges.borrow_mut().remove(left_size - 1);
        let left_keys_len = left_sibling.keys.borrow().len();
        let stolen_split_key = left_sibling.keys.borrow_mut().remove(left_keys_len - 1);
        self.keys.borrow_mut().insert(0, parent_split_key);
        self.edges.borrow_mut().insert(0, stolen_edge);
        parent_node.keys.borrow_mut()[edge_idx - 1] = stolen_split_key;
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
        let parent_split_key = parent_node.keys.borrow()[edge_idx].clone();
        let stolen_edge = right_sibling.edges.borrow_mut().remove(0);
        // This will become parent's new split key
        let stolen_key = right_sibling.keys.borrow_mut().remove(0);
        self.keys.borrow_mut().push(parent_split_key);
        self.edges.borrow_mut().push(stolen_edge);
        parent_node.keys.borrow_mut()[edge_idx] = stolen_key;
        true
    }

    /**
     * Find the left sibling provided the index of the corresponding edge in the parent's node
     */
    pub fn find_child_left_sibling(&self, edge_idx: usize) -> Option<LatchNode<K>> {
        if edge_idx == 0 {
            return None;
        }
        self.edges.borrow()[edge_idx - 1].borrow().clone()
    }

    /**
     * Find the right sibling provided the index of the corresponding edge in the parent's node
     */
    pub fn find_child_right_sibling(&self, edge_idx: usize) -> Option<LatchNode<K>> {
        if edge_idx == self.edges.borrow().len() - 1 {
            return None;
        }
        self.edges.borrow()[edge_idx + 1].borrow().clone()
    }

    pub fn update_key_at_index(&self, idx: usize, new_key: K) {
        self.keys.borrow_mut()[idx] = new_key;
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
        if let Some(ref left_latch_node) = left_sibling {
            let left_guard = left_latch_node.as_ref().write().unwrap();
            let left_node = left_guard.as_internal_node();
            let parent_split_key = parent_node.keys.borrow_mut().remove(edge_idx - 1);
            let mut left_keys = left_node.keys.borrow_mut();
            left_keys.push(parent_split_key);
            left_keys.append(&mut self.keys.borrow_mut());
            left_node
                .edges
                .borrow_mut()
                .append(&mut self.edges.borrow_mut());

            // removing the edge corresponding to the internal node since it's merged into the left node
            parent_node.edges.borrow_mut().remove(edge_idx);
        } else {
            let right_sibling = parent_node.find_child_right_sibling(edge_idx);
            if let Some(right_latch_node) = right_sibling {
                let right_guard = right_latch_node.as_ref().write().unwrap();
                let right_node = right_guard.as_internal_node();
                // we merge right node into the current node
                let parent_split_key = parent_node.keys.borrow_mut().remove(edge_idx);
                let mut current_keys = self.keys.borrow_mut();
                current_keys.push(parent_split_key);
                current_keys.append(&mut right_node.keys.borrow_mut());
                self.edges
                    .borrow_mut()
                    .append(&mut right_node.edges.borrow_mut());

                // removing the edge corresponding to the right node since we are merging into the current node
                parent_node.edges.borrow_mut().remove(edge_idx + 1);
            }
        }
    }

    pub fn contains_key(&self, key: &K) -> bool {
        self.keys.borrow().contains(key)
    }

    pub fn deal_with_underflow(&self, parent_node: &InternalNode<K>, edge_idx: usize) {
        if self.is_underflow() {
            let left_sibling = parent_node.find_child_left_sibling(edge_idx);
            let mut is_stolen = false;
            if let Some(ref left_latch_node) = left_sibling {
                let write_guard = left_latch_node.as_ref().write().unwrap();
                let left_node = write_guard.as_internal_node();
                is_stolen = self.steal_from_left_sibling(left_node, parent_node, edge_idx);
            }
            if !is_stolen {
                let right_sibling = parent_node.find_child_right_sibling(edge_idx);
                if let Some(ref right_latch_node) = right_sibling {
                    let right_guard = right_latch_node.as_ref().write().unwrap();
                    let right_node = right_guard.as_internal_node();
                    is_stolen = self.steal_from_right_sibling(right_node, parent_node, edge_idx);
                }
            }
            if !is_stolen {
                self.merge_with_sibling(parent_node, edge_idx);
            }
        }
    }
}

// impl leaf
impl<K: NodeKey> LeafNode<K> {
    pub fn new(capacity: u16) -> Self {
        LeafNode {
            start_keys: RefCell::new(Vec::new()),
            end_keys: RefCell::new(Vec::new()),
            left_ptr: RefCell::new(None),
            right_ptr: RefCell::new(None),
            order: capacity,
            rw_lock: Rc::new(RwLock::new(())),
        }
    }

    pub fn acquire_read_lock(&self) -> RwLockReadGuard<()> {
        let lock = self.rw_lock.as_ref();
        lock.read().unwrap()
    }

    pub fn acquire_write_lock(&self) -> RwLockWriteGuard<()> {
        let lock = self.rw_lock.as_ref();
        lock.write().unwrap()
    }

    /**
     * A thread can release latch on a parent node if its child node
     * considered safe. It is safe when:
     * - the node won't split or merge when updated
     * - not full (on insertion)
     * - more than half full (on deletion)
     */
    pub fn is_safe_to_release_parent_latch(&self, intent: LatchIntent) -> bool {
        match intent {
            LatchIntent::DELETE => self.has_spare_key(),
            LatchIntent::INSERT => self.start_keys.borrow().len() + 1 < usize::from(self.order),
            LatchIntent::SEARCH => true,
        }
    }

    // order 4 means at most 3 keys per node
    pub fn has_capacity(&self) -> bool {
        self.start_keys.borrow().len() < usize::from(self.order)
    }

    /**
     * Just inserts, doesn't check for overflow and not responsible for splitting.
     */
    pub fn insert_range(&self, range: Range<K>) {
        let mut insert_idx = self.start_keys.borrow().len();
        for (pos, k) in self.start_keys.borrow().iter().enumerate() {
            if &range.start_key < k {
                insert_idx = pos;
                break;
            }
        }
        self.start_keys
            .borrow_mut()
            .insert(insert_idx, range.start_key);
        self.end_keys.borrow_mut().insert(insert_idx, range.end_key);
    }

    pub fn find_key_idx(&self, key: &K) -> Option<usize> {
        for (idx, k) in self.start_keys.borrow().iter().enumerate() {
            if k == key {
                return Some(idx);
            }
        }
        None
    }

    pub fn find_next_larger_key(&self, key: &K) -> Option<usize> {
        for (idx, k) in self.start_keys.borrow().iter().enumerate() {
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
                self.start_keys.borrow_mut().remove(idx);
                self.end_keys.borrow_mut().remove(idx);
                true
            }
            None => false,
        }
    }

    pub fn is_underflow(&self) -> bool {
        let min_nodes = self.order / 2;
        self.start_keys.borrow().len() < min_nodes.try_into().unwrap()
    }

    // Returns whether a sibling can steal a node from the current node
    pub fn has_spare_key(&self) -> bool {
        let min_nodes = self.order / 2;
        self.start_keys.borrow().len() > min_nodes.into()
    }

    pub fn get_smallest_key(&self) -> K {
        self.start_keys.borrow().first().unwrap().clone()
    }

    // Returns the stolen key
    pub fn steal_smallest_key(&self) -> Range<K> {
        if !self.has_spare_key() {
            panic!("Cannot steal key from leaf, will underflow")
        }
        let start_key = self.start_keys.borrow_mut().remove(0);
        let end_key = self.end_keys.borrow_mut().remove(0);
        Range { start_key, end_key }
    }

    pub fn get_largest(&self) -> K {
        self.start_keys.borrow().last().unwrap().clone()
    }

    // Returns the stolen key
    pub fn steal_biggest_key(&self) -> Range<K> {
        if !self.has_spare_key() {
            panic!("Cannot steal key from leaf, will underflow")
        }
        let idx = self.start_keys.borrow().len() - 1;
        let start_key = self.start_keys.borrow_mut().remove(idx);
        let end_key = self.end_keys.borrow_mut().remove(idx);
        Range { start_key, end_key }
    }

    // Returns whether it was stolen from right sibling
    pub fn steal_from_right_leaf_sibling(
        &self,
        key_to_delete: &K,
        right_sibling: LatchNode<K>,
        stack: &Vec<(usize, Direction, &InternalNode<K>)>,
    ) -> bool {
        let right_write_guard = right_sibling.as_ref().write().unwrap();
        let right_sibling = right_write_guard.as_leaf_node();
        if right_sibling.has_spare_key() {
            let stolen_range = right_sibling.steal_smallest_key();
            let stolen_key = stolen_range.start_key.clone();
            self.insert_range(stolen_range);

            // Update any parent's split key. Since we are stealing from right sibling,
            // if the split key is the key to delete, it is now the stolen key from right sibling
            for (iter_idx, (idx, direction, internal_node)) in stack.iter().enumerate() {
                let key_idx = match direction {
                    Direction::Left => *idx,
                    Direction::Right => *idx - 1,
                };

                if iter_idx == stack.len() - 1 {
                    // Update parent's split key. Since we are stealing from right sibling,
                    // the new split_key will be the right sibling's new smallest key
                    internal_node
                        .update_key_at_index(key_idx, right_write_guard.get_lower().unwrap());
                } else {
                    let mut keys = internal_node.keys.borrow_mut();
                    let key = &keys[key_idx];
                    if key == key_to_delete {
                        keys[key_idx] = stolen_key.clone();
                    }
                }
            }
            return true;
        }
        false
    }

    pub fn steal_from_left_leaf_sibling(
        &self,
        key_to_delete: &K,
        left_sibling: LatchNode<K>,
        stack: &Vec<(usize, Direction, &InternalNode<K>)>,
    ) -> bool {
        let left_write_guard = left_sibling.as_ref().write().unwrap();
        let left_sibling = left_write_guard.as_leaf_node();
        if left_sibling.has_spare_key() {
            let stolen_range = left_sibling.steal_biggest_key();
            let stolen_key = stolen_range.start_key.clone();
            self.insert_range(stolen_range);

            let (idx, direction, parent_node) = stack[stack.len() - 1].clone();
            // Update parent's split key. Since we are stealing from left sibling,
            // the new split_key will be the stolen key
            parent_node.update_key_at_index(idx - 1, stolen_key);
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
            Some(left_latch_node) => {
                let left_guard = left_latch_node.as_ref().write().unwrap();
                // merge current node into left node
                let left_node = left_guard.as_leaf_node();
                left_node
                    .start_keys
                    .borrow_mut()
                    .append(&mut self.start_keys.borrow_mut());
                left_node
                    .end_keys
                    .borrow_mut()
                    .append(&mut self.end_keys.borrow_mut());
                // edge_idx - 1 | split_key | edge_idx
                // We want to remove edge_idx and split_key (will be edge_idx - 1 in coresponding keys vec)
                parent_node.edges.borrow_mut().remove(edge_idx);
                parent_node.keys.borrow_mut().remove(edge_idx - 1);
                *left_node.right_ptr.borrow_mut() = self.right_ptr.take();
            }
            None => {
                let right_sibling = parent_node.find_child_right_sibling(edge_idx);
                match right_sibling {
                    Some(right_latch_node) => {
                        let right_guard = right_latch_node.as_ref().write().unwrap();
                        // merge right node into current node
                        let right_node = right_guard.as_leaf_node();
                        self.start_keys
                            .borrow_mut()
                            .append(&mut right_node.start_keys.borrow_mut());
                        self.end_keys
                            .borrow_mut()
                            .append(&mut right_node.end_keys.borrow_mut());

                        // edge_idx | split_key | edge_idx + 1
                        // We want to remove edge_idx + 1 and split_key (will be edge_idx in coresponding keys vec)
                        parent_node.edges.borrow_mut().remove(edge_idx + 1);
                        parent_node.keys.borrow_mut().remove(edge_idx);
                        *self.right_ptr.borrow_mut() = right_node.right_ptr.take();
                    }
                    None => {
                        todo!()
                    }
                };
            }
        }
    }

    pub fn update_ancestors_after_delete(
        &self,
        key_to_delete: &K,
        stack: &Vec<(usize, Direction, &InternalNode<K>)>,
        right_sibling_option: &Option<LatchNode<K>>,
    ) -> () {
        let right_sibling = self.right_ptr.borrow();
        let next_largest_key = self.find_next_largest_key(key_to_delete, right_sibling_option);
        // if the leaf to delete is in the right subtree and the
        // current node is equal to the key to delete, then we update to the next biggest node
        for (iter_idx, (idx, direction, internal_node)) in stack.iter().enumerate() {
            match direction {
                Direction::Left => {}
                Direction::Right => {
                    let key_idx = *idx - 1;
                    let mut keys = internal_node.keys.borrow_mut();
                    if &keys[key_idx] == key_to_delete {
                        keys[key_idx] = next_largest_key.clone();
                    }
                }
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
                return self.start_keys.borrow()[idx].clone();
            }
            None => {
                // This means that the next biggest key is not in the same leaf node
                let right_leaf_option = right_sibling_option.clone();
                let right_latch_node = right_leaf_option.unwrap();
                let right_read_guard = right_latch_node.as_ref().read().unwrap();
                return right_read_guard.as_leaf_node().start_keys.borrow()[0].clone();
            }
        }
    }

    // Returns whether it merged with a sibling
    /**
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
        stack: &Vec<(usize, Direction, &InternalNode<K>)>,
    ) -> bool {
        let is_deleted = self.remove_key(key_to_delete.clone());

        if !is_deleted {
            return false;
        }

        // if there are no parents, then the leaf is the only element. We will allow root to underflow
        if stack.len() == 0 {
            return false;
        }
        let (edge_idx, _, parent_node) = stack[stack.len() - 1].clone();
        let right_sibling_option = parent_node.find_child_right_sibling(edge_idx);
        let left_sibling_option = parent_node.find_child_left_sibling(edge_idx);
        if !self.is_underflow() {
            self.update_ancestors_after_delete(&key_to_delete, &stack, &right_sibling_option);
            return false;
        }
        let mut is_stolen = false;
        // try to borrow left sibling for a key
        if let Some(left_sibling) = left_sibling_option {
            is_stolen = self.steal_from_left_leaf_sibling(&key_to_delete, left_sibling, &stack);
        }
        // try to borrow right sibling for a key
        if !is_stolen {
            if let Some(right_sibling) = right_sibling_option {
                is_stolen =
                    self.steal_from_right_leaf_sibling(&key_to_delete, right_sibling, stack);
            }
        }

        // Can't borrow from either siblings. In this case we merge
        if !is_stolen {
            self.merge_node(parent_node, edge_idx);
            return true;
        }
        return false;
    }
}

// Order of 3 means each node can only store 2 keys.
pub struct BTree<K: NodeKey> {
    root: NodeLink<K>,
    order: u16,
}

pub struct Range<K: NodeKey> {
    start_key: K,
    end_key: K,
}

impl<K: NodeKey> BTree<K> {
    pub fn new(capacity: u16) -> Self {
        BTree {
            root: RefCell::new(Some(Arc::new(RwLock::new(Node::Leaf(LeafNode::new(
                capacity,
            )))))),
            order: capacity,
        }
    }

    /**
     * Returns the node to delete. In addition, it returns a stack of (index, parent_node). The index
     * corresponds to the index of the parent_node. This is useful when we need to find the siblings
     * of the nodes when borrowing / merging.
     */
    pub fn find_leaf_to_delete(
        &self,
        key_to_delete: &K,
    ) -> (
        Option<LatchNode<K>>,
        Vec<(usize, Direction, &InternalNode<K>)>,
    ) {
        let mut temp_node = self.root.borrow().clone();

        let mut next = None;
        let mut stack = Vec::new();
        loop {
            if let Some(node) = temp_node {
                let lock = node.as_ref();
                let guard = lock.write().unwrap();

                match &*guard {
                    Node::Internal(internal_node) => {
                        for (idx, k) in internal_node.keys.borrow().iter().enumerate() {
                            if key_to_delete < k {
                                stack.push((idx, Direction::Left, internal_node));
                                next = internal_node.edges.borrow()[idx].borrow().clone();
                                break;
                            }

                            if idx == internal_node.keys.borrow().len() - 1 {
                                stack.push((idx + 1, Direction::Right, internal_node));
                                next = internal_node.edges.borrow()[idx + 1].borrow().clone();
                                break;
                            }
                        }
                    }
                    Node::Leaf(_) => break,
                };
                match next {
                    Some(ref v) => temp_node = next.clone(),
                    None => panic!("next is not provided"),
                }
            }
        }

        (temp_node, stack)
    }

    pub fn find_internal_node(&self, search_key: &K) -> (Option<Rc<Node<K>>>, Vec<Rc<Node<K>>>) {
        let mut temp_node = self.root.borrow().clone();

        let mut next = None;
        let mut stack = Vec::new();
        loop {
            match temp_node {
                Some(ref node) => match node.as_ref() {
                    Node::Internal(ref internal_node) => {
                        if internal_node.contains_key(search_key) {
                            return (temp_node.clone(), stack);
                        }
                        stack.push(node.clone());
                        for (idx, k) in internal_node.keys.borrow().iter().enumerate() {
                            if search_key < k {
                                next = internal_node.edges.borrow()[idx].borrow().clone();
                                break;
                            }

                            if idx == internal_node.keys.borrow().len() - 1 {
                                next = internal_node.edges.borrow()[idx + 1].borrow().clone();
                            }
                        }
                    }

                    Node::Leaf(_) => break,
                },
                None => panic!("should not be undefined"),
            }

            match next {
                Some(_) => temp_node = next.clone(),
                None => panic!("next is not provided"),
            }
        }
        (None, stack)
    }

    // Determines which leaf node a new key should go into we assume there will at least always be one root.
    // As it traverses down the nodes, acquire write locks. But if it's safe to release parent lock, it will.
    // Returns the leaf node to add and the stack of parent nodes
    pub fn find_leaf_to_add<'a>(
        &self,
        key_to_add: &K,
    ) -> (
        Option<Rc<Node<K>>>,
        Vec<(Rc<Node<K>>, Option<RwLockWriteGuard<()>>)>,
    ) {
        let mut temp_node = self.root.borrow().clone();

        let mut next = None;
        let mut stack: Vec<(Rc<Node<K>>, Option<RwLockWriteGuard<()>>)> = Vec::new();
        loop {
            match &temp_node {
                Some(ref node_rc) => match node_rc.as_ref() {
                    Node::Internal(internal_node) => {
                        let acquired_lock = internal_node.acquire_write_lock();
                        if internal_node.is_safe_to_release_parent_latch(LatchIntent::INSERT) {
                            let len = stack.len();
                            if len > 0 {
                                let idx = len - 1;
                                let parent_lock = mem::replace(&mut stack[idx].1, None);
                                if let Some(write_lock) = parent_lock {
                                    drop(write_lock)
                                }
                            }
                        }
                        stack.push((node_rc.clone(), Some(acquired_lock)));
                        // stack.push((node_rc.clone(), None));

                        for (idx, k) in internal_node.keys.borrow().iter().enumerate() {
                            if key_to_add < k {
                                next = internal_node.edges.borrow()[idx].borrow().clone();
                                break;
                            }

                            if idx == internal_node.keys.borrow().len() - 1 {
                                next = internal_node.edges.borrow()[idx + 1].borrow().clone();
                            }
                        }
                    }
                    Node::Leaf(_) => break,
                },
                None => panic!("should not be undefined"),
            };

            match next {
                Some(_) => temp_node = next.clone(),
                None => panic!("next is not provided"),
            };
        }

        (temp_node, stack)
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
    pub fn insert(&self, range: Range<K>) -> () {
        // TODO: We need the parent node
        let (leaf, parent_stack) = self.find_leaf_to_add(&range.start_key);
        let leaf = leaf.unwrap();
        match leaf.as_ref() {
            Node::Internal(_) => panic!("There must be at least one leaf node in the btree"),
            Node::Leaf(leaf_node) => {
                leaf_node.insert_range(range);
                if !leaf_node.has_capacity() {
                    let (mut split_node, mut median) = BTree::split_node(leaf.clone());

                    let mut offset = 0;
                    let mut current_node = leaf.clone();
                    loop {
                        if parent_stack.len() - offset > 0 {
                            let idx = parent_stack.len() - 1 - offset;
                            current_node = parent_stack[idx].0.clone();
                            // this is the node we want to insert the
                            let curr_parent = current_node.as_ref().as_internal_node();
                            curr_parent.insert_node(split_node.clone(), median.clone());
                            if curr_parent.has_capacity() {
                                break;
                            }
                            (split_node, median) = BTree::split_node(current_node.clone());
                            offset = offset + 1;
                        } else {
                            // root needs to split. Create a new root with one key and 2 children
                            self.root
                                .borrow_mut()
                                .replace(Rc::new(Node::Internal(InternalNode {
                                    keys: RefCell::new(Vec::from([median.clone()])),
                                    edges: RefCell::new(Vec::from([
                                        RefCell::new(Some(current_node.clone())),
                                        RefCell::new(Some(split_node.clone())),
                                    ])),
                                    order: self.order,
                                    rw_lock: Rc::new(RwLock::new(())),
                                })));
                            break;
                        }
                    }
                }
            }
        }
    }

    /**
     * Allocate a new leaf node and move half keys to the new node.
     * Returns the new node and the smallest key in the new node.
     */
    pub fn split_node(node: Rc<Node<K>>) -> (Rc<Node<K>>, K) {
        match node.as_ref() {
            Node::Internal(internal_node) => {
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
                let mid_idx = internal_node.keys.borrow().len() / 2;
                let mut right_keys = internal_node.keys.borrow_mut().split_off(mid_idx);
                let mut right_edges = internal_node.edges.borrow_mut().split_off(mid_idx + 1);
                right_edges.insert(0, RefCell::new(None));
                let right_start = right_keys.remove(0);
                right_edges.remove(0);
                let new_right_node = InternalNode {
                    keys: RefCell::new(right_keys),
                    edges: RefCell::new(right_edges),
                    order: internal_node.order,
                    rw_lock: Rc::new(RwLock::new(())),
                };
                (Rc::new(Node::Internal(new_right_node)), right_start)
            }
            Node::Leaf(leaf_node) => {
                let mid = leaf_node.start_keys.borrow().len() / 2;
                let right_start_keys = leaf_node.start_keys.borrow_mut().split_off(mid);

                let right_end_keys = leaf_node.end_keys.borrow_mut().split_off(mid);
                let right_sibling = leaf_node.right_ptr.borrow_mut().take();
                let right_start = right_start_keys[0].clone();

                let new_right_node = LeafNode {
                    start_keys: RefCell::new(right_start_keys),
                    end_keys: RefCell::new(right_end_keys),
                    left_ptr: RefCell::new(Some(Rc::downgrade(&node))), // TODO: set the left_sibling to the current leaf node later
                    right_ptr: RefCell::new(right_sibling),
                    order: leaf_node.order,
                    rw_lock: Rc::new(RwLock::new(())),
                };
                let right_rc = Rc::new(Node::Leaf(new_right_node));
                leaf_node
                    .right_ptr
                    .borrow_mut()
                    .replace(Rc::downgrade(&right_rc));
                (right_rc, right_start)
            }
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
     *      - if neigher siblings have node to spare, the merge the node with its sibling. If the node
     *        is internal, we will need to incorporate the split key from the parent into the merging.
     *        In either case, we will need to repeat the removal algorithm on the parent node to remove the split
     *        key that previously separated these merged nodes unless the parent is the root and we are removing
     *        the finaly key from the root. In which case the merged node becomes the new root.
     *
     * TODO: if the ancestor's node matches key_to_delete, it also needs to stay locked
     */
    pub fn delete(&self, key_to_delete: K) -> () {
        let (node_to_delete, stack) = self.find_leaf_to_delete(&key_to_delete);
        if let Some(ref node_ref) = node_to_delete {
            let leaf_guard = node_ref.as_ref().write().unwrap();

            let leaf_node = leaf_guard.as_leaf_node();
            let did_leaf_merge = leaf_node.delete_key(&key_to_delete, &stack);

            if did_leaf_merge {
                let mut i = stack.len() - 1;
                loop {
                    if i < 1 {
                        break;
                    }
                    let (_, _, internal_node) = &stack[i];
                    let (edge_idx, _, parent_node) = &stack[i - 1];
                    internal_node.deal_with_underflow(parent_node.clone(), *edge_idx);
                    println!("dealing with underflow!!");

                    i = i - 1;
                }
            }
            if stack.len() > 1 {
                let root = self.root.borrow().clone().unwrap();
                let internal_root = root.as_internal_node();
                if internal_root.keys.borrow().len() == 0 {
                    let new_root = internal_root.edges.borrow()[0].borrow().clone().unwrap();
                    self.root.borrow_mut().replace(new_root);
                }
            }
        }
    }
}

mod Test {
    use std::{borrow::Borrow, cell::RefCell, process::Child, rc::Rc, sync::RwLock};

    use super::{BTree, InternalNode, LeafNode, Node, NodeKey, NodeLink, WeakNodeLink};

    pub fn find_node_and_parent_with_indices<K: NodeKey>(
        tree: &BTree<K>,
        indices: Vec<usize>,
    ) -> (Rc<Node<K>>, Rc<Node<K>>, usize) {
        let last_index = indices.last().unwrap().clone();
        let (node, stack) = find_node_with_indices(tree, indices);
        let last = stack.last().unwrap();
        (node.unwrap(), last.clone(), last_index)
    }

    pub fn find_node_with_indices<K: NodeKey>(
        tree: &BTree<K>,
        indices: Vec<usize>,
    ) -> (Option<Rc<Node<K>>>, Vec<Rc<Node<K>>>) {
        let mut temp_node = tree.root.borrow().clone();
        let mut stack = Vec::new();
        let mut next = None;
        for idx in indices.iter() {
            match temp_node {
                Some(ref node) => match node.as_ref() {
                    Node::Internal(internal_node) => {
                        stack.push(node.clone());
                        next = internal_node.edges.borrow()[*idx].borrow().clone();
                    }

                    Node::Leaf(_) => break,
                },
                None => panic!("should not be undefined"),
            }
            match next {
                Some(_) => temp_node = next.clone(),
                None => panic!("next is not provided"),
            }
        }
        (temp_node, stack)
    }

    #[derive(Debug, Clone)]
    pub enum TestNode<K: NodeKey> {
        Internal(TestInternalNode<K>),
        Leaf(TestLeafNode<K>),
    }

    #[derive(Debug, Clone)]
    pub struct TestInternalNode<K: NodeKey> {
        keys: Vec<K>,
        edges: Vec<Option<TestNode<K>>>,
    }

    #[derive(Debug, Clone)]
    pub struct TestLeafNode<K: NodeKey> {
        keys: Vec<K>,
    }

    pub fn create_test_tree<K: NodeKey>(node: &TestNode<K>, order: u16) -> BTree<K> {
        let node = create_test_node(node, order);
        BTree {
            root: RefCell::new(Some(node)),
            order,
        }
    }

    pub fn create_test_node<K: NodeKey>(node: &TestNode<K>, order: u16) -> Rc<Node<K>> {
        let (node, mut leaves) = create_tree_from_test_node_internal(node, order);

        for (idx, child) in leaves.iter().enumerate() {
            match child.as_ref() {
                Node::Internal(_) => panic!("Node must be a leaf"),
                Node::Leaf(leaf_node) => {
                    if idx > 0 {
                        leaf_node
                            .left_ptr
                            .borrow_mut()
                            .replace(Rc::downgrade(&leaves[idx - 1].clone()));
                    }

                    if idx < leaves.len() - 1 {
                        leaf_node
                            .right_ptr
                            .borrow_mut()
                            .replace(Rc::downgrade(&leaves[idx + 1].clone()));
                    }
                }
            }
        }
        node
    }

    // Returns the created node and any leaves it has
    pub fn create_tree_from_test_node_internal<K: NodeKey>(
        node: &TestNode<K>,
        order: u16,
    ) -> (Rc<Node<K>>, Vec<Rc<Node<K>>>) {
        match node {
            TestNode::Internal(internal_node) => {
                let mut leaves = Vec::new();
                let edges = internal_node
                    .edges
                    .iter()
                    .map(|e| match e {
                        Some(child) => {
                            let (child_node, mut child_leaves) =
                                create_tree_from_test_node_internal(child, order);
                            leaves.append(&mut child_leaves);
                            RefCell::new(Some(child_node))
                            // todo!()
                        }
                        None => RefCell::new(None),
                    })
                    .collect::<Vec<NodeLink<K>>>();

                let ret_node = InternalNode {
                    keys: RefCell::new(internal_node.keys.clone()),
                    edges: RefCell::new(edges),
                    order,
                    rw_lock: Rc::new(RwLock::new(())),
                };
                (Rc::new(Node::Internal(ret_node)), leaves)
            }
            TestNode::Leaf(leaf_node) => {
                let leaf = Node::Leaf(LeafNode {
                    start_keys: RefCell::new(leaf_node.keys.clone()),
                    end_keys: RefCell::new(leaf_node.keys.clone()),
                    left_ptr: RefCell::new(None),
                    right_ptr: RefCell::new(None),
                    order: order,
                    rw_lock: Rc::new(RwLock::new(())),
                });
                let leaf_rc = Rc::new(leaf);
                (leaf_rc.clone(), Vec::from([leaf_rc.clone()]))
            }
        }
    }

    pub fn get_indent(depth: usize) -> String {
        " ".repeat(depth * 2)
    }

    pub fn print_tree<K: NodeKey>(tree: &BTree<K>) {
        print_tree_internal(&tree.root, 0);
    }

    pub fn print_node_recursive<K: NodeKey>(node: Rc<Node<K>>) {
        let tree = BTree {
            root: RefCell::new(Some(node.clone())),
            order: 4,
        };
        print_tree(&tree);
    }

    // Doesn't print recursively. Just prints that single node's attributes
    pub fn print_node<K: NodeKey>(node: Rc<Node<K>>) {
        match node.as_ref() {
            Node::Internal(node) => {
                println!("Internal. Keys: {:?}", node.keys);
            }
            Node::Leaf(ref node) => {
                println!(
                    "Leaf. Keys: {:?}. Left start: {:?} Right start: {:?}",
                    node.start_keys,
                    get_first_key_from_weak_link(&node.left_ptr),
                    get_first_key_from_weak_link(&node.right_ptr)
                );
            }
        }
    }

    pub fn get_start_keys_from_weak_link<K: NodeKey>(link: &WeakNodeLink<K>) -> Option<Vec<K>> {
        let edge = &*link.borrow();
        if let Some(ref rc) = edge {
            let upgraded_ref = rc.upgrade();
            let unwrapped = upgraded_ref.unwrap();
            match unwrapped.as_ref() {
                Node::Internal(_) => {
                    panic!("Cannot get sibling from internal node");
                }
                Node::Leaf(ref node) => {
                    let keys = node.start_keys.borrow();
                    Some(keys.clone())
                }
            }
        } else {
            None
        }
    }

    fn get_first_key_from_weak_link<K: NodeKey>(link: &WeakNodeLink<K>) -> Option<K> {
        let edge = &*link.borrow();
        if let Some(ref rc) = edge {
            let upgraded_ref = rc.upgrade()?;

            let unwrapped = upgraded_ref;
            match unwrapped.as_ref() {
                Node::Internal(_) => {
                    panic!("Cannot get sibling from internal node");
                }
                Node::Leaf(ref node) => {
                    let keys = node.start_keys.borrow();
                    let first = keys.get(0);
                    match first {
                        Some(k) => Some(k.clone()),
                        None => None,
                    }
                }
            }
        } else {
            None
        }
    }

    fn print_tree_internal<K: NodeKey>(link: &NodeLink<K>, depth: usize) {
        let edge = link.borrow().clone();
        if let Some(ref rc) = edge {
            let node = rc.as_ref();
            match node {
                Node::Internal(ref node) => {
                    println!(
                        "{}Internal. Keys: {:?}",
                        get_indent(depth),
                        node.keys.borrow()
                    );

                    for edge in &*node.edges.borrow() {
                        print_tree_internal(edge, depth + 1);
                    }
                }
                Node::Leaf(ref node) => {
                    println!(
                        "{}Leaf. Keys: {:?}. Left start: {:?} Right start: {:?}",
                        get_indent(depth),
                        node.start_keys.borrow(),
                        get_first_key_from_weak_link(&node.left_ptr),
                        get_first_key_from_weak_link(&node.right_ptr)
                    );
                }
            }
        }
    }

    fn assert_node_and_leaves_siblings<K: NodeKey>(node: Rc<Node<K>>, test_node: &TestNode<K>) {
        assert_node(node.clone(), test_node);
        let test_leaves = get_all_test_leaves(test_node);
        let leaves = get_all_leaf_nodes(node.clone());
        assert_eq!(test_leaves.len(), leaves.len());
        for (idx, current_test_node) in test_leaves.iter().enumerate() {
            let curr_node = leaves[idx].clone();
            let left_sibling = &*curr_node.as_leaf_node().left_ptr.borrow();
            let right_sibling = &*curr_node.as_leaf_node().right_ptr.borrow();
            if idx == 0 {
                assert!(left_sibling.is_none());
            } else {
                let test_left_sibling = test_leaves[idx - 1];
                let left_node = right_sibling.as_ref().unwrap().upgrade().unwrap().clone();
                assert_leaf(left_node, &test_left_sibling.keys);
            }

            if idx == test_leaves.len() - 1 {
                assert!(right_sibling.is_none());
            } else {
                let test_right_sibling = test_leaves[idx + 1];
                let right_node = right_sibling.as_ref().unwrap().upgrade().unwrap().clone();
                assert_leaf(right_node, &test_right_sibling.keys);
            }
        }
    }
    /**
     * Given a node link and a test node structure, verify if if the node link
     * has the expected shape and properties
     */
    fn assert_node<K: NodeKey>(node: Rc<Node<K>>, test_node: &TestNode<K>) {
        match test_node {
            TestNode::Internal(test_internal_node) => {
                let node_rc = node.clone();
                let node_ref = node_rc.as_ref();
                let internal_node = node_ref.as_internal_node();
                assert_eq!(&*internal_node.keys.borrow(), &test_internal_node.keys);
                for (idx, child) in internal_node.edges.borrow().iter().enumerate() {
                    let node = child.borrow();
                    match &*node {
                        Some(child_node) => {
                            let test_child = test_internal_node.edges[idx].clone();
                            let unwrapped = test_child.unwrap();
                            assert_node(child_node.clone(), &unwrapped);
                        }
                        None => {
                            if test_internal_node.edges[idx].is_some() {
                                let foo = "";
                            }
                            assert_eq!(test_internal_node.edges[idx].is_none(), true);
                        }
                    };
                }
            }
            TestNode::Leaf(test_leaf) => {
                assert_leaf(node.clone(), &test_leaf.keys);
            }
        };
    }

    fn assert_tree<K: NodeKey>(tree: &BTree<K>, test_node: &TestNode<K>) {
        let root = tree.root.borrow().clone().unwrap();
        assert_node(root, test_node);
    }

    fn get_all_leaves<K: NodeKey>(node: Rc<Node<K>>) -> Vec<Option<Rc<Node<K>>>> {
        let mut leaves = Vec::new();
        match node.as_ref() {
            Node::Internal(internal_node) => {
                for edge in internal_node.edges.borrow().iter() {
                    match &*edge.borrow() {
                        Some(child) => {
                            let mut child_leaves = get_all_leaves(child.clone());
                            leaves.append(&mut child_leaves);
                        }
                        None => leaves.push(None),
                    };
                }
            }
            Node::Leaf(_) => {
                leaves.push(Some(node.clone()));
            }
        };
        leaves
    }

    fn assert_leaf_with_siblings<K: NodeKey>(
        node: Rc<Node<K>>,
        test_leaf: &TestLeafNode<K>,
        test_left_sibling: &Option<TestLeafNode<K>>,
        test_right_sibling: &Option<TestLeafNode<K>>,
    ) {
        assert_leaf(node.clone(), &test_leaf.keys);
        let leaf_node = node.as_ref().as_leaf_node();
        let left_sibling = &*leaf_node.left_ptr.borrow();
        match left_sibling {
            Some(left_node) => {
                assert_leaf(
                    left_node.upgrade().unwrap().clone(),
                    &test_left_sibling.as_ref().unwrap().keys,
                );
            }
            None => {
                assert!(test_left_sibling.is_none());
            }
        };

        let right_sibling = &*leaf_node.right_ptr.borrow();
        match right_sibling {
            Some(right_node) => {
                assert_leaf(
                    right_node.upgrade().unwrap().clone(),
                    &test_right_sibling.as_ref().unwrap().keys,
                );
            }
            None => {
                assert!(test_left_sibling.is_none());
            }
        };
    }

    fn get_all_leaf_nodes<K: NodeKey>(node: Rc<Node<K>>) -> Vec<Rc<Node<K>>> {
        let mut leaves = Vec::new();
        match node.as_ref() {
            Node::Internal(internal_node) => {
                for edge in internal_node.edges.borrow().iter() {
                    if let Some(child) = &*edge.borrow() {
                        let mut child_leaves = get_all_leaf_nodes(child.clone());
                        leaves.append(&mut child_leaves);
                    }
                }
            }
            Node::Leaf(_) => {
                leaves.push(node.clone());
            }
        };
        leaves
    }

    fn get_all_test_leaves<K: NodeKey>(test_node: &TestNode<K>) -> Vec<&TestLeafNode<K>> {
        let mut leaves = Vec::new();
        match test_node {
            TestNode::Internal(internal_node) => {
                for edge in internal_node.edges.iter() {
                    if let Some(child) = edge {
                        let mut child_leaves = get_all_test_leaves(child);
                        leaves.append(&mut child_leaves);
                    }
                }
            }
            TestNode::Leaf(test_leaf) => {
                leaves.push(test_leaf);
            }
        };
        leaves
    }

    fn assert_leaf<K: NodeKey>(node: Rc<Node<K>>, start_keys: &Vec<K>) {
        match &node.as_ref() {
            Node::Internal(_) => panic!("not a leaf node"),
            Node::Leaf(leaf) => {
                assert_eq!(&*leaf.start_keys.borrow(), start_keys)
            }
        }
    }

    fn assert_internal<K: NodeKey>(node: Rc<Node<K>>, start_keys: Vec<K>) {
        match &node.as_ref() {
            Node::Internal(internal_node) => {
                assert_eq!(&*internal_node.keys.borrow(), &start_keys)
            }
            Node::Leaf(_) => panic!("not an internal node"),
        }
    }

    mod search {
        use std::{cell::RefCell, rc::Rc};

        use crate::latch_manager::latch_interval_btree::{
            BTree, InternalNode, LeafNode, Node,
            Test::{
                assert_internal, assert_leaf, create_test_node, create_test_tree, print_tree,
                TestInternalNode, TestLeafNode, TestNode,
            },
        };

        #[test]
        fn one_level_deep() {
            let test_node = TestNode::Internal(TestInternalNode {
                keys: Vec::from([12, 15, 19]),
                edges: Vec::from([
                    Some(TestNode::Leaf(TestLeafNode {
                        keys: Vec::from([11]),
                    })),
                    Some(TestNode::Leaf(TestLeafNode {
                        keys: Vec::from([14]),
                    })),
                    Some(TestNode::Leaf(TestLeafNode {
                        keys: Vec::from([18]),
                    })),
                    Some(TestNode::Leaf(TestLeafNode {
                        keys: Vec::from([25]),
                    })),
                ]),
            });
            let tree = create_test_tree(&test_node, 4);

            let (leaf1, stack) = tree.find_leaf_to_add(&0);
            assert_eq!(stack.len(), 1);
            assert_internal(stack[0].0.clone(), Vec::from([12, 15, 19]));

            assert_leaf(leaf1.unwrap(), &Vec::from([11]));

            let leaf2 = tree.find_leaf_to_add(&15).0.unwrap();
            assert_leaf(leaf2, &Vec::from([18]));

            let leaf4 = tree.find_leaf_to_add(&100).0.unwrap();
            assert_leaf(leaf4, &Vec::from([25]));

            print_tree(&tree);
        }
    }

    mod split {
        use std::{borrow::Borrow, cell::RefCell, rc::Rc, sync::RwLock};

        use crate::latch_manager::latch_interval_btree::{
            BTree, LeafNode, Node,
            Test::{
                assert_leaf_with_siblings, assert_node, assert_tree, get_all_leaf_nodes,
                get_all_leaves, get_start_keys_from_weak_link, print_node,
            },
        };

        use super::{
            create_test_node, create_test_tree, print_node_recursive, print_tree, TestInternalNode,
            TestLeafNode, TestNode,
        };

        #[test]
        fn split_internal() {
            let test_node = TestNode::Internal(TestInternalNode {
                keys: Vec::from([5, 20, 30]),
                edges: Vec::from([
                    None,
                    Some(TestNode::Leaf(TestLeafNode {
                        keys: Vec::from([6, 8, 10]),
                    })),
                    Some(TestNode::Leaf(TestLeafNode {
                        keys: Vec::from([21, 25]),
                    })),
                    Some(TestNode::Leaf(TestLeafNode {
                        keys: Vec::from([35]),
                    })),
                ]),
            });
            let node = create_test_node(&test_node, 4);
            let (split_node, median) = BTree::split_node(node.clone());
            assert_eq!(median, 20);

            let split_test_node = TestNode::Internal(TestInternalNode {
                keys: Vec::from([30]),
                edges: Vec::from([
                    Some(TestNode::Leaf(TestLeafNode {
                        keys: Vec::from([21, 25]),
                    })),
                    Some(TestNode::Leaf(TestLeafNode {
                        keys: Vec::from([35]),
                    })),
                ]),
            });
            assert_node(split_node.clone(), &split_test_node);
            let leaves = get_all_leaves(split_node.clone());
            assert_eq!(leaves.len(), 2);
            assert_leaf_with_siblings(
                leaves[0].as_ref().unwrap().clone(),
                &TestLeafNode {
                    keys: Vec::from([21, 25]),
                },
                &Some(TestLeafNode {
                    keys: Vec::from([6, 8, 10]),
                }),
                &Some(TestLeafNode {
                    keys: Vec::from([35]),
                }),
            );
            // print_node_recursive(split_node.clone());
        }

        #[test]
        fn split_leaf() {
            let leaf = LeafNode {
                start_keys: RefCell::new(Vec::from([0, 1, 2])),
                end_keys: RefCell::new(Vec::from([0, 1, 2])),
                left_ptr: RefCell::new(None),
                right_ptr: RefCell::new(None),
                order: 4,
                rw_lock: Rc::new(RwLock::new(())),
            };

            let leaf_rc = Rc::new(Node::Leaf(leaf));
            let right_sibling = LeafNode {
                start_keys: RefCell::new(Vec::from([4, 5, 6])),
                end_keys: RefCell::new(Vec::from([0, 1, 2])),
                left_ptr: RefCell::new(Some(Rc::downgrade(&leaf_rc))),
                right_ptr: RefCell::new(None),
                order: 4,
                rw_lock: Rc::new(RwLock::new(())),
            };
            let right_sibling_rc = Rc::new(Node::Leaf(right_sibling));
            match leaf_rc.as_ref() {
                Node::Internal(_) => panic!("Leaf is somehow internal"),
                Node::Leaf(leaf) => leaf
                    .right_ptr
                    .borrow_mut()
                    .replace(Rc::downgrade(&right_sibling_rc)),
            };

            let (split_node, right_start_key) = BTree::split_node(leaf_rc.clone());
            assert_eq!(right_start_key, 1);

            match split_node.as_ref() {
                Node::Internal(_) => panic!("Split node cannot be internal"),
                Node::Leaf(leaf) => {
                    assert_eq!(&*leaf.start_keys.borrow(), &Vec::from([1, 2]));
                    assert_eq!(&*leaf.end_keys.borrow(), &Vec::from([1, 2]));
                    let left_start_keys = get_start_keys_from_weak_link(&leaf.left_ptr);
                    match left_start_keys.clone() {
                        Some(left_start_keys) => {
                            assert_eq!(left_start_keys, Vec::from([0]));
                        }
                        None => panic!("Left key has start keys"),
                    }
                    let right_start_keys = get_start_keys_from_weak_link(&leaf.right_ptr);
                    match right_start_keys.clone() {
                        Some(left_start_keys) => {
                            assert_eq!(left_start_keys, Vec::from([4, 5, 6]));
                        }
                        None => panic!("Right key has start keys"),
                    }
                }
            }

            print_node(split_node.clone());
        }
    }

    mod insert {
        use crate::latch_manager::latch_interval_btree::{BTree, Range};

        use super::{
            assert_node, assert_tree, print_tree, TestInternalNode, TestLeafNode, TestNode,
        };

        #[test]
        fn insert_and_split() {
            let tree = BTree::<i32>::new(3);
            tree.insert(Range {
                start_key: 5,
                end_key: 5,
            });
            tree.insert(Range {
                start_key: 10,
                end_key: 10,
            });
            tree.insert(Range {
                start_key: 20,
                end_key: 20,
            });
            print_tree(&tree);

            let test_node = TestNode::Internal(TestInternalNode {
                keys: Vec::from([10]),
                edges: Vec::from([
                    Some(TestNode::Leaf(TestLeafNode {
                        keys: Vec::from([5]),
                    })),
                    Some(TestNode::Leaf(TestLeafNode {
                        keys: Vec::from([10, 20]),
                    })),
                ]),
            });

            assert_tree(&tree, &test_node);
        }

        #[test]
        fn insert_and_split_internal() {
            let tree = BTree::<i32>::new(3);
            tree.insert(Range {
                start_key: 5,
                end_key: 5,
            });
            tree.insert(Range {
                start_key: 10,
                end_key: 10,
            });
            tree.insert(Range {
                start_key: 20,
                end_key: 20,
            });

            let test_node = TestNode::Internal(TestInternalNode {
                keys: Vec::from([10]),
                edges: Vec::from([
                    Some(TestNode::Leaf(TestLeafNode {
                        keys: Vec::from([5]),
                    })),
                    Some(TestNode::Leaf(TestLeafNode {
                        keys: Vec::from([10, 20]),
                    })),
                ]),
            });

            print_tree(&tree);

            assert_tree(&tree, &test_node);

            // here
            tree.insert(Range {
                start_key: 15,
                end_key: 15,
            });
            print_tree(&tree);
            let test_node = TestNode::Internal(TestInternalNode {
                keys: Vec::from([10, 15]),
                edges: Vec::from([
                    Some(TestNode::Leaf(TestLeafNode {
                        keys: Vec::from([5]),
                    })),
                    Some(TestNode::Leaf(TestLeafNode {
                        keys: Vec::from([10]),
                    })),
                    Some(TestNode::Leaf(TestLeafNode {
                        keys: Vec::from([15, 20]),
                    })),
                ]),
            });
            assert_tree(&tree, &test_node);

            tree.insert(Range {
                start_key: 25,
                end_key: 25,
            });
            print_tree(&tree);

            let test_node = TestNode::Internal(TestInternalNode {
                keys: Vec::from([15]),
                edges: Vec::from([
                    Some(TestNode::Internal(TestInternalNode {
                        keys: Vec::from([10]),
                        edges: Vec::from([
                            Some(TestNode::Leaf(TestLeafNode {
                                keys: Vec::from([5]),
                            })),
                            Some(TestNode::Leaf(TestLeafNode {
                                keys: Vec::from([10]),
                            })),
                        ]),
                    })),
                    Some(TestNode::Internal(TestInternalNode {
                        keys: Vec::from([20]),
                        edges: Vec::from([
                            Some(TestNode::Leaf(TestLeafNode {
                                keys: Vec::from([15]),
                            })),
                            Some(TestNode::Leaf(TestLeafNode {
                                keys: Vec::from([20, 25]),
                            })),
                        ]),
                    })),
                ]),
            });

            assert_tree(&tree, &test_node);
        }
    }

    mod leaf_underflow {
        use std::{cell::RefCell, rc::Rc, sync::RwLock};

        use crate::latch_manager::latch_interval_btree::LeafNode;

        #[test]
        fn underflows() {
            let leaf = LeafNode {
                start_keys: RefCell::new(Vec::from([0])),
                end_keys: RefCell::new(Vec::from([0])),
                left_ptr: RefCell::new(None),
                right_ptr: RefCell::new(None),
                order: 4,
                rw_lock: Rc::new(RwLock::new(())),
            };
            assert!(leaf.is_underflow());
        }
    }

    mod delete {
        mod core_delete {
            use crate::latch_manager::latch_interval_btree::Test::{
                assert_tree, create_test_tree, print_tree, TestInternalNode, TestLeafNode, TestNode,
            };

            #[test]
            fn internal_node_stealing_from_left_sibling_3_layers() {
                let test_node = TestNode::Internal(TestInternalNode {
                    keys: Vec::from([20]),
                    edges: Vec::from([
                        Some(TestNode::Internal(TestInternalNode {
                            keys: Vec::from([10, 15]),
                            edges: Vec::from([
                                Some(TestNode::Leaf(TestLeafNode {
                                    keys: Vec::from([5]),
                                })),
                                Some(TestNode::Leaf(TestLeafNode {
                                    keys: Vec::from([10]),
                                })),
                                Some(TestNode::Leaf(TestLeafNode {
                                    keys: Vec::from([15, 18]),
                                })),
                            ]),
                        })),
                        Some(TestNode::Internal(TestInternalNode {
                            keys: Vec::from([30]),
                            edges: Vec::from([
                                Some(TestNode::Leaf(TestLeafNode {
                                    keys: Vec::from([20]),
                                })),
                                Some(TestNode::Leaf(TestLeafNode {
                                    keys: Vec::from([30]),
                                })),
                            ]),
                        })),
                    ]),
                });
                let tree = create_test_tree(&test_node, 3);
                tree.delete(30);

                let expected_tree = TestNode::Internal(TestInternalNode {
                    keys: Vec::from([15]),
                    edges: Vec::from([
                        Some(TestNode::Internal(TestInternalNode {
                            keys: Vec::from([10]),
                            edges: Vec::from([
                                Some(TestNode::Leaf(TestLeafNode {
                                    keys: Vec::from([5]),
                                })),
                                Some(TestNode::Leaf(TestLeafNode {
                                    keys: Vec::from([10]),
                                })),
                            ]),
                        })),
                        Some(TestNode::Internal(TestInternalNode {
                            keys: Vec::from([20]),
                            edges: Vec::from([
                                Some(TestNode::Leaf(TestLeafNode {
                                    keys: Vec::from([15, 18]),
                                })),
                                Some(TestNode::Leaf(TestLeafNode {
                                    keys: Vec::from([20]),
                                })),
                            ]),
                        })),
                    ]),
                });
                assert_tree(&tree, &expected_tree);
            }

            #[test]
            fn internal_node_stealing_from_right_sibling_4_layers() {
                let test_node = TestNode::Internal(TestInternalNode {
                    keys: Vec::from([40]),
                    edges: Vec::from([
                        Some(TestNode::Internal(TestInternalNode {
                            keys: Vec::from([15]),
                            edges: Vec::from([
                                Some(TestNode::Internal(TestInternalNode {
                                    keys: Vec::from([10]),
                                    edges: Vec::from([
                                        Some(TestNode::Leaf(TestLeafNode {
                                            keys: Vec::from([5]),
                                        })),
                                        Some(TestNode::Leaf(TestLeafNode {
                                            keys: Vec::from([10]),
                                        })),
                                    ]),
                                })),
                                Some(TestNode::Internal(TestInternalNode {
                                    keys: Vec::from([18]),
                                    edges: Vec::from([
                                        Some(TestNode::Leaf(TestLeafNode {
                                            keys: Vec::from([15]),
                                        })),
                                        Some(TestNode::Leaf(TestLeafNode {
                                            keys: Vec::from([18, 20]),
                                        })),
                                    ]),
                                })),
                            ]),
                        })),
                        Some(TestNode::Internal(TestInternalNode {
                            keys: Vec::from([60]),
                            edges: Vec::from([
                                Some(TestNode::Internal(TestInternalNode {
                                    keys: Vec::from([50]),
                                    edges: Vec::from([
                                        Some(TestNode::Leaf(TestLeafNode {
                                            keys: Vec::from([40, 45]),
                                        })),
                                        Some(TestNode::Leaf(TestLeafNode {
                                            keys: Vec::from([50]),
                                        })),
                                    ]),
                                })),
                                Some(TestNode::Internal(TestInternalNode {
                                    keys: Vec::from([70]),
                                    edges: Vec::from([
                                        Some(TestNode::Leaf(TestLeafNode {
                                            keys: Vec::from([60]),
                                        })),
                                        Some(TestNode::Leaf(TestLeafNode {
                                            keys: Vec::from([70]),
                                        })),
                                    ]),
                                })),
                            ]),
                        })),
                    ]),
                });
                let tree = create_test_tree(&test_node, 3);
                tree.delete(70);
                print_tree(&tree);

                let expected_tree = TestNode::Internal(TestInternalNode {
                    keys: Vec::from([15, 40]),
                    edges: Vec::from([
                        Some(TestNode::Internal(TestInternalNode {
                            keys: Vec::from([10]),
                            edges: Vec::from([
                                Some(TestNode::Leaf(TestLeafNode {
                                    keys: Vec::from([5]),
                                })),
                                Some(TestNode::Leaf(TestLeafNode {
                                    keys: Vec::from([10]),
                                })),
                            ]),
                        })),
                        Some(TestNode::Internal(TestInternalNode {
                            keys: Vec::from([18]),
                            edges: Vec::from([
                                Some(TestNode::Leaf(TestLeafNode {
                                    keys: Vec::from([15]),
                                })),
                                Some(TestNode::Leaf(TestLeafNode {
                                    keys: Vec::from([18, 20]),
                                })),
                            ]),
                        })),
                        Some(TestNode::Internal(TestInternalNode {
                            keys: Vec::from([50, 60]),
                            edges: Vec::from([
                                Some(TestNode::Leaf(TestLeafNode {
                                    keys: Vec::from([40, 45]),
                                })),
                                Some(TestNode::Leaf(TestLeafNode {
                                    keys: Vec::from([50]),
                                })),
                                Some(TestNode::Leaf(TestLeafNode {
                                    keys: Vec::from([60]),
                                })),
                            ]),
                        })),
                    ]),
                });
                assert_tree(&tree, &expected_tree);
            }

            // hello
            #[test]
            fn internal_node_stealing_from_right_sibling_3_layers() {
                let test_node = TestNode::Internal(TestInternalNode {
                    keys: Vec::from([20, 40]),
                    edges: Vec::from([
                        Some(TestNode::Internal(TestInternalNode {
                            keys: Vec::from([10]),
                            edges: Vec::from([
                                Some(TestNode::Leaf(TestLeafNode {
                                    keys: Vec::from([5]),
                                })),
                                Some(TestNode::Leaf(TestLeafNode {
                                    keys: Vec::from([10]),
                                })),
                            ]),
                        })),
                        Some(TestNode::Internal(TestInternalNode {
                            keys: Vec::from([30]),
                            edges: Vec::from([
                                Some(TestNode::Leaf(TestLeafNode {
                                    keys: Vec::from([20]),
                                })),
                                Some(TestNode::Leaf(TestLeafNode {
                                    keys: Vec::from([30]),
                                })),
                            ]),
                        })),
                        Some(TestNode::Internal(TestInternalNode {
                            keys: Vec::from([45, 50]),
                            edges: Vec::from([
                                Some(TestNode::Leaf(TestLeafNode {
                                    keys: Vec::from([40]),
                                })),
                                Some(TestNode::Leaf(TestLeafNode {
                                    keys: Vec::from([45, 49]),
                                })),
                                Some(TestNode::Leaf(TestLeafNode {
                                    keys: Vec::from([50, 60]),
                                })),
                            ]),
                        })),
                    ]),
                });
                let tree = create_test_tree(&test_node, 3);
                tree.delete(30);

                let expected_tree = TestNode::Internal(TestInternalNode {
                    keys: Vec::from([20, 45]),
                    edges: Vec::from([
                        Some(TestNode::Internal(TestInternalNode {
                            keys: Vec::from([10]),
                            edges: Vec::from([
                                Some(TestNode::Leaf(TestLeafNode {
                                    keys: Vec::from([5]),
                                })),
                                Some(TestNode::Leaf(TestLeafNode {
                                    keys: Vec::from([10]),
                                })),
                            ]),
                        })),
                        Some(TestNode::Internal(TestInternalNode {
                            keys: Vec::from([40]),
                            edges: Vec::from([
                                Some(TestNode::Leaf(TestLeafNode {
                                    keys: Vec::from([20]),
                                })),
                                Some(TestNode::Leaf(TestLeafNode {
                                    keys: Vec::from([40]),
                                })),
                            ]),
                        })),
                        Some(TestNode::Internal(TestInternalNode {
                            keys: Vec::from([50]),
                            edges: Vec::from([
                                Some(TestNode::Leaf(TestLeafNode {
                                    keys: Vec::from([45, 49]),
                                })),
                                Some(TestNode::Leaf(TestLeafNode {
                                    keys: Vec::from([50, 60]),
                                })),
                            ]),
                        })),
                    ]),
                });
                assert_tree(&tree, &expected_tree);
            }
        }

        mod find_leaf_to_delete {
            use crate::latch_manager::latch_interval_btree::Test::{
                create_test_tree, TestInternalNode, TestLeafNode, TestNode,
            };

            #[test]
            fn test_leaf() {
                let test_node = TestNode::Internal(TestInternalNode {
                    keys: Vec::from([15]),
                    edges: Vec::from([
                        Some(TestNode::Internal(TestInternalNode {
                            keys: Vec::from([10]),
                            edges: Vec::from([
                                Some(TestNode::Leaf(TestLeafNode {
                                    keys: Vec::from([5]),
                                })),
                                Some(TestNode::Leaf(TestLeafNode {
                                    keys: Vec::from([10]),
                                })),
                            ]),
                        })),
                        Some(TestNode::Internal(TestInternalNode {
                            keys: Vec::from([20]),
                            edges: Vec::from([
                                Some(TestNode::Leaf(TestLeafNode {
                                    keys: Vec::from([15]),
                                })),
                                Some(TestNode::Leaf(TestLeafNode {
                                    keys: Vec::from([20, 25]),
                                })),
                            ]),
                        })),
                    ]),
                });
                let tree = create_test_tree(&test_node, 3);
                let (node, path) = tree.find_leaf_to_delete(&20);
                let indices = path
                    .iter()
                    .map(|(idx, _, _)| idx.clone())
                    .collect::<Vec<usize>>();
                assert_eq!(indices, Vec::from([1, 1]));
            }
        }

        mod leaf_stealing {
            use crate::latch_manager::latch_interval_btree::{
                Node,
                Test::{create_test_tree, print_tree, TestInternalNode, TestLeafNode, TestNode},
            };

            mod has_spare_keys {
                use std::{cell::RefCell, rc::Rc, sync::RwLock};

                use crate::latch_manager::latch_interval_btree::{
                    LeafNode,
                    Test::{
                        assert_tree, create_test_tree, TestInternalNode, TestLeafNode, TestNode,
                    },
                };

                #[test]
                fn internal_node() {}

                #[test]
                fn leaf_node_has_spare_key() {
                    let leaf_node = LeafNode {
                        start_keys: RefCell::new(Vec::from([0, 1])),
                        end_keys: RefCell::new(Vec::from([0, 1])),
                        left_ptr: RefCell::new(None),
                        right_ptr: RefCell::new(None),
                        order: 3,
                        rw_lock: Rc::new(RwLock::new(())),
                    };
                    assert_eq!(leaf_node.has_spare_key(), true);
                }

                #[test]
                fn leaf_node_has_no_spare_key() {
                    let leaf_node = LeafNode {
                        start_keys: RefCell::new(Vec::from([0])),
                        end_keys: RefCell::new(Vec::from([0])),
                        left_ptr: RefCell::new(None),
                        right_ptr: RefCell::new(None),
                        order: 3,
                        rw_lock: Rc::new(RwLock::new(())),
                    };
                    assert_eq!(leaf_node.has_spare_key(), false);
                }

                #[test]
                fn requires_updating_ancestor() {
                    let test_node = TestNode::Internal(TestInternalNode {
                        keys: Vec::from([4]),
                        edges: Vec::from([
                            Some(TestNode::Internal(TestInternalNode {
                                keys: Vec::from([2]),
                                edges: Vec::from([
                                    Some(TestNode::Leaf(TestLeafNode {
                                        keys: Vec::from([1]),
                                    })),
                                    Some(TestNode::Leaf(TestLeafNode {
                                        keys: Vec::from([2, 3]),
                                    })),
                                ]),
                            })),
                            Some(TestNode::Internal(TestInternalNode {
                                keys: Vec::from([10]),
                                edges: Vec::from([
                                    Some(TestNode::Leaf(TestLeafNode {
                                        keys: Vec::from([4, 5]),
                                    })),
                                    Some(TestNode::Leaf(TestLeafNode {
                                        keys: Vec::from([10, 13]),
                                    })),
                                ]),
                            })),
                        ]),
                    });
                    let tree = create_test_tree(&test_node, 3);
                    tree.delete(4);

                    let expected_node = TestNode::Internal(TestInternalNode {
                        keys: Vec::from([5]),
                        edges: Vec::from([
                            Some(TestNode::Internal(TestInternalNode {
                                keys: Vec::from([2]),
                                edges: Vec::from([
                                    Some(TestNode::Leaf(TestLeafNode {
                                        keys: Vec::from([1]),
                                    })),
                                    Some(TestNode::Leaf(TestLeafNode {
                                        keys: Vec::from([2, 3]),
                                    })),
                                ]),
                            })),
                            Some(TestNode::Internal(TestInternalNode {
                                keys: Vec::from([10]),
                                edges: Vec::from([
                                    Some(TestNode::Leaf(TestLeafNode {
                                        keys: Vec::from([5]),
                                    })),
                                    Some(TestNode::Leaf(TestLeafNode {
                                        keys: Vec::from([10, 13]),
                                    })),
                                ]),
                            })),
                        ]),
                    });
                    assert_tree(&tree, &expected_node);
                }
            }

            mod stealing_core {
                use crate::latch_manager::latch_interval_btree::Test::{
                    assert_tree, create_test_tree, print_tree, TestInternalNode, TestLeafNode,
                    TestNode,
                };

                #[test]
                fn leaf_steals_left_sibling() {
                    let test_node = TestNode::Internal(TestInternalNode {
                        keys: Vec::from([8]),
                        edges: Vec::from([
                            Some(TestNode::Internal(TestInternalNode {
                                keys: Vec::from([5]),
                                edges: Vec::from([
                                    Some(TestNode::Leaf(TestLeafNode {
                                        keys: Vec::from([1, 3]),
                                    })),
                                    Some(TestNode::Leaf(TestLeafNode {
                                        keys: Vec::from([5]),
                                    })),
                                ]),
                            })),
                            Some(TestNode::Internal(TestInternalNode {
                                keys: Vec::from([10]),
                                edges: Vec::from([
                                    Some(TestNode::Leaf(TestLeafNode {
                                        keys: Vec::from([8, 9]),
                                    })),
                                    Some(TestNode::Leaf(TestLeafNode {
                                        keys: Vec::from([10, 15]),
                                    })),
                                ]),
                            })),
                        ]),
                    });
                    let tree = create_test_tree(&test_node, 3);
                    tree.delete(5);
                    let expected_tree_after_delete = TestNode::Internal(TestInternalNode {
                        keys: Vec::from([8]),
                        edges: Vec::from([
                            Some(TestNode::Internal(TestInternalNode {
                                keys: Vec::from([3]),
                                edges: Vec::from([
                                    Some(TestNode::Leaf(TestLeafNode {
                                        keys: Vec::from([1]),
                                    })),
                                    Some(TestNode::Leaf(TestLeafNode {
                                        keys: Vec::from([3]),
                                    })),
                                ]),
                            })),
                            Some(TestNode::Internal(TestInternalNode {
                                keys: Vec::from([10]),
                                edges: Vec::from([
                                    Some(TestNode::Leaf(TestLeafNode {
                                        keys: Vec::from([8, 9]),
                                    })),
                                    Some(TestNode::Leaf(TestLeafNode {
                                        keys: Vec::from([10, 15]),
                                    })),
                                ]),
                            })),
                        ]),
                    });
                    assert_tree(&tree, &expected_tree_after_delete);
                }

                #[test]
                fn leaf_steals_right_sibling() {
                    let test_node = TestNode::Internal(TestInternalNode {
                        keys: Vec::from([10]),
                        edges: Vec::from([
                            Some(TestNode::Internal(TestInternalNode {
                                keys: Vec::from([5]),
                                edges: Vec::from([
                                    Some(TestNode::Leaf(TestLeafNode {
                                        keys: Vec::from([2]),
                                    })),
                                    Some(TestNode::Leaf(TestLeafNode {
                                        keys: Vec::from([5, 6]),
                                    })),
                                ]),
                            })),
                            Some(TestNode::Internal(TestInternalNode {
                                keys: Vec::from([12]),
                                edges: Vec::from([
                                    Some(TestNode::Leaf(TestLeafNode {
                                        keys: Vec::from([10]),
                                    })),
                                    Some(TestNode::Leaf(TestLeafNode {
                                        keys: Vec::from([12, 20]),
                                    })),
                                ]),
                            })),
                        ]),
                    });
                    let tree = create_test_tree(&test_node, 3);
                    tree.delete(10);
                    print_tree(&tree);
                    let expected_tree_after_delete = TestNode::Internal(TestInternalNode {
                        keys: Vec::from([12]),
                        edges: Vec::from([
                            Some(TestNode::Internal(TestInternalNode {
                                keys: Vec::from([5]),
                                edges: Vec::from([
                                    Some(TestNode::Leaf(TestLeafNode {
                                        keys: Vec::from([2]),
                                    })),
                                    Some(TestNode::Leaf(TestLeafNode {
                                        keys: Vec::from([5, 6]),
                                    })),
                                ]),
                            })),
                            Some(TestNode::Internal(TestInternalNode {
                                keys: Vec::from([20]),
                                edges: Vec::from([
                                    Some(TestNode::Leaf(TestLeafNode {
                                        keys: Vec::from([12]),
                                    })),
                                    Some(TestNode::Leaf(TestLeafNode {
                                        keys: Vec::from([20]),
                                    })),
                                ]),
                            })),
                        ]),
                    });
                    assert_tree(&tree, &expected_tree_after_delete);
                }
            }
        }

        mod internal_node_stealing {
            use crate::latch_manager::latch_interval_btree::Test::{
                assert_tree, create_test_tree, find_node_and_parent_with_indices, print_tree,
                TestInternalNode, TestLeafNode, TestNode,
            };

            #[test]
            fn simple_steal_from_left_sibling() {
                let test_node = TestNode::Internal(TestInternalNode {
                    keys: Vec::from([20]),
                    edges: Vec::from([
                        Some(TestNode::Internal(TestInternalNode {
                            keys: Vec::from([10, 15]),
                            edges: Vec::from([
                                Some(TestNode::Leaf(TestLeafNode {
                                    keys: Vec::from([5]),
                                })),
                                Some(TestNode::Leaf(TestLeafNode {
                                    keys: Vec::from([10]),
                                })),
                                Some(TestNode::Leaf(TestLeafNode {
                                    keys: Vec::from([15, 18]),
                                })),
                            ]),
                        })),
                        Some(TestNode::Internal(TestInternalNode {
                            keys: Vec::from([]),
                            edges: Vec::from([Some(TestNode::Leaf(TestLeafNode {
                                keys: Vec::from([20]),
                            }))]),
                        })),
                    ]),
                });
                let tree = create_test_tree(&test_node, 3);
                let (node_rc, parent, edge_idx) =
                    find_node_and_parent_with_indices(&tree, Vec::from([1]));
                let temp = node_rc.as_internal_node();
                let did_steal = temp.steal_from_sibling(parent, edge_idx);
                println!("Did steal {}", did_steal);
                let expected_node = TestNode::Internal(TestInternalNode {
                    keys: Vec::from([15]),
                    edges: Vec::from([
                        Some(TestNode::Internal(TestInternalNode {
                            keys: Vec::from([10]),
                            edges: Vec::from([
                                Some(TestNode::Leaf(TestLeafNode {
                                    keys: Vec::from([5]),
                                })),
                                Some(TestNode::Leaf(TestLeafNode {
                                    keys: Vec::from([10]),
                                })),
                            ]),
                        })),
                        Some(TestNode::Internal(TestInternalNode {
                            keys: Vec::from([20]),
                            edges: Vec::from([
                                Some(TestNode::Leaf(TestLeafNode {
                                    keys: Vec::from([15, 18]),
                                })),
                                Some(TestNode::Leaf(TestLeafNode {
                                    keys: Vec::from([20]),
                                })),
                            ]),
                        })),
                    ]),
                });
                assert_tree(&tree, &expected_node);
            }

            #[test]
            fn simple_steal_from_right_sibling() {
                let test_node = TestNode::Internal(TestInternalNode {
                    keys: Vec::from([20, 40]),
                    edges: Vec::from([
                        Some(TestNode::Internal(TestInternalNode {
                            keys: Vec::from([10]),
                            edges: Vec::from([
                                Some(TestNode::Leaf(TestLeafNode {
                                    keys: Vec::from([5]),
                                })),
                                Some(TestNode::Leaf(TestLeafNode {
                                    keys: Vec::from([10]),
                                })),
                            ]),
                        })),
                        Some(TestNode::Internal(TestInternalNode {
                            keys: Vec::from([]),
                            edges: Vec::from([Some(TestNode::Leaf(TestLeafNode {
                                keys: Vec::from([20]),
                            }))]),
                        })),
                        Some(TestNode::Internal(TestInternalNode {
                            keys: Vec::from([45, 50]),
                            edges: Vec::from([
                                Some(TestNode::Leaf(TestLeafNode {
                                    keys: Vec::from([40]),
                                })),
                                Some(TestNode::Leaf(TestLeafNode {
                                    keys: Vec::from([45, 49]),
                                })),
                                Some(TestNode::Leaf(TestLeafNode {
                                    keys: Vec::from([50, 60]),
                                })),
                            ]),
                        })),
                    ]),
                });
                let tree = create_test_tree(&test_node, 3);
                let (node_rc, parent, edge_idx) =
                    find_node_and_parent_with_indices(&tree, Vec::from([1]));
                let temp = node_rc.as_internal_node();
                let did_steal = temp.steal_from_sibling(parent, edge_idx);
                println!("Did steal {}", did_steal);

                let expected_node = TestNode::Internal(TestInternalNode {
                    keys: Vec::from([20, 45]),
                    edges: Vec::from([
                        Some(TestNode::Internal(TestInternalNode {
                            keys: Vec::from([10]),
                            edges: Vec::from([
                                Some(TestNode::Leaf(TestLeafNode {
                                    keys: Vec::from([5]),
                                })),
                                Some(TestNode::Leaf(TestLeafNode {
                                    keys: Vec::from([10]),
                                })),
                            ]),
                        })),
                        Some(TestNode::Internal(TestInternalNode {
                            keys: Vec::from([40]),
                            edges: Vec::from([
                                Some(TestNode::Leaf(TestLeafNode {
                                    keys: Vec::from([20]),
                                })),
                                Some(TestNode::Leaf(TestLeafNode {
                                    keys: Vec::from([40]),
                                })),
                            ]),
                        })),
                        Some(TestNode::Internal(TestInternalNode {
                            keys: Vec::from([50]),
                            edges: Vec::from([
                                Some(TestNode::Leaf(TestLeafNode {
                                    keys: Vec::from([45, 49]),
                                })),
                                Some(TestNode::Leaf(TestLeafNode {
                                    keys: Vec::from([50, 60]),
                                })),
                            ]),
                        })),
                    ]),
                });
                assert_tree(&tree, &expected_node);
            }
        }
    }

    mod merge {
        mod internal_node {
            use crate::latch_manager::latch_interval_btree::Test::{
                assert_tree, create_test_tree, find_node_and_parent_with_indices,
                find_node_with_indices, print_tree, TestInternalNode, TestLeafNode, TestNode,
            };

            #[test]
            fn merge_with_left() {
                let test_node = TestNode::Internal(TestInternalNode {
                    keys: Vec::from([60]),
                    edges: Vec::from([
                        Some(TestNode::Internal(TestInternalNode {
                            keys: Vec::from([50]),
                            edges: Vec::from([
                                Some(TestNode::Leaf(TestLeafNode {
                                    keys: Vec::from([40, 45]),
                                })),
                                Some(TestNode::Leaf(TestLeafNode {
                                    keys: Vec::from([50]),
                                })),
                            ]),
                        })),
                        Some(TestNode::Internal(TestInternalNode {
                            keys: Vec::from([]),
                            edges: Vec::from([Some(TestNode::Leaf(TestLeafNode {
                                keys: Vec::from([60]),
                            }))]),
                        })),
                    ]),
                });
                let tree = create_test_tree(&test_node, 3);
                let (node, parent, edge_idx) =
                    find_node_and_parent_with_indices(&tree, Vec::from([1]));
                let internal_node = node.as_internal_node();
                internal_node.merge_with_sibling(parent.clone(), edge_idx);
                let expected_tree = TestNode::Internal(TestInternalNode {
                    keys: Vec::from([]),
                    edges: Vec::from([
                        Some(TestNode::Internal(TestInternalNode {
                            keys: Vec::from([50, 60]),
                            edges: Vec::from([
                                Some(TestNode::Leaf(TestLeafNode {
                                    keys: Vec::from([40, 45]),
                                })),
                                Some(TestNode::Leaf(TestLeafNode {
                                    keys: Vec::from([50]),
                                })),
                                Some(TestNode::Leaf(TestLeafNode {
                                    keys: Vec::from([60]),
                                })),
                            ]),
                        })),
                        Some(TestNode::Internal(TestInternalNode {
                            keys: Vec::from([]),
                            edges: Vec::from([Some(TestNode::Leaf(TestLeafNode {
                                keys: Vec::from([60]),
                            }))]),
                        })),
                    ]),
                });
                assert_tree(&tree, &expected_tree);
            }

            #[test]
            fn merge_with_right() {
                let test_node = TestNode::Internal(TestInternalNode {
                    keys: Vec::from([60]),
                    edges: Vec::from([
                        Some(TestNode::Internal(TestInternalNode {
                            keys: Vec::from([50]),
                            edges: Vec::from([
                                Some(TestNode::Leaf(TestLeafNode {
                                    keys: Vec::from([40, 45]),
                                })),
                                Some(TestNode::Leaf(TestLeafNode {
                                    keys: Vec::from([50]),
                                })),
                            ]),
                        })),
                        Some(TestNode::Internal(TestInternalNode {
                            keys: Vec::from([]),
                            edges: Vec::from([Some(TestNode::Leaf(TestLeafNode {
                                keys: Vec::from([60]),
                            }))]),
                        })),
                    ]),
                });
                let tree = create_test_tree(&test_node, 3);
                let (node, parent, edge_idx) =
                    find_node_and_parent_with_indices(&tree, Vec::from([0]));
                let internal_node = node.as_internal_node();
                internal_node.merge_with_sibling(parent.clone(), edge_idx);
                let expected_tree = TestNode::Internal(TestInternalNode {
                    keys: Vec::from([]),
                    edges: Vec::from([
                        Some(TestNode::Internal(TestInternalNode {
                            keys: Vec::from([50, 60]),
                            edges: Vec::from([
                                Some(TestNode::Leaf(TestLeafNode {
                                    keys: Vec::from([40, 45]),
                                })),
                                Some(TestNode::Leaf(TestLeafNode {
                                    keys: Vec::from([50]),
                                })),
                                Some(TestNode::Leaf(TestLeafNode {
                                    keys: Vec::from([60]),
                                })),
                            ]),
                        })),
                        Some(TestNode::Internal(TestInternalNode {
                            keys: Vec::from([]),
                            edges: Vec::from([Some(TestNode::Leaf(TestLeafNode {
                                keys: Vec::from([60]),
                            }))]),
                        })),
                    ]),
                });
                assert_tree(&tree, &expected_tree);
            }
        }

        mod leaf {
            use crate::latch_manager::latch_interval_btree::Test::{
                assert_tree, create_test_tree, print_tree, TestInternalNode, TestLeafNode, TestNode,
            };

            #[test]
            fn merge_with_left_leaf() {
                let test_node = TestNode::Internal(TestInternalNode {
                    keys: Vec::from([16, 20]),
                    edges: Vec::from([
                        Some(TestNode::Leaf(TestLeafNode {
                            keys: Vec::from([5, 10]),
                        })),
                        Some(TestNode::Leaf(TestLeafNode {
                            keys: Vec::from([16]),
                        })),
                        Some(TestNode::Leaf(TestLeafNode {
                            keys: Vec::from([20, 30, 40]),
                        })),
                    ]),
                });
                let tree = create_test_tree(&test_node, 3);
                let (node, stack) = tree.find_leaf_to_delete(&16);
                let unwrapped = node.unwrap();
                let leaf = unwrapped.as_leaf_node();
                let (edge_idx, dir, parent) = stack.last().unwrap();
                leaf.merge_node(parent.clone(), *edge_idx);
                print_tree(&tree);

                let expected_node = TestNode::Internal(TestInternalNode {
                    keys: Vec::from([20]),
                    edges: Vec::from([
                        Some(TestNode::Leaf(TestLeafNode {
                            keys: Vec::from([5, 10, 16]),
                        })),
                        Some(TestNode::Leaf(TestLeafNode {
                            keys: Vec::from([20, 30, 40]),
                        })),
                    ]),
                });
                assert_tree(&tree, &expected_node);
            }

            #[test]
            fn merge_with_right_leaf() {
                let test_node = TestNode::Internal(TestInternalNode {
                    keys: Vec::from([25]),
                    edges: Vec::from([
                        Some(TestNode::Leaf(TestLeafNode {
                            keys: Vec::from([]),
                        })),
                        Some(TestNode::Leaf(TestLeafNode {
                            keys: Vec::from([25]),
                        })),
                    ]),
                });
                let tree = create_test_tree(&test_node, 3);
                let (node, stack) = tree.find_leaf_to_delete(&16);
                let unwrapped = node.unwrap();
                let leaf = unwrapped.as_leaf_node();
                let (edge_idx, dir, parent) = stack.last().unwrap();
                leaf.merge_node(parent.clone(), *edge_idx);
                print_tree(&tree);

                let expected_node = TestNode::Internal(TestInternalNode {
                    keys: Vec::from([]),
                    edges: Vec::from([Some(TestNode::Leaf(TestLeafNode {
                        keys: Vec::from([25]),
                    }))]),
                });
                assert_tree(&tree, &expected_node);
            }
        }
    }

    #[test]
    fn experiment() {
        let my_rwlock = RwLock::new(5);

        let read1 = my_rwlock.read().unwrap(); // one .read() is fine
        let read2 = my_rwlock.read().unwrap(); // two .read()s is also fine

        println!("{:?}, {:?}", read1, read2);
        drop(read1);
        drop(read2);
    }
}
