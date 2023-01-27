use std::{
    cell::RefCell,
    rc::{Rc, Weak},
};

struct Foo {}

pub trait NodeKey: std::fmt::Debug + Clone + Eq + PartialOrd + Ord {}

impl NodeKey for i32 {}

type NodeLink<K: NodeKey> = Option<Rc<RefCell<Node<K>>>>;
type WeakNodeLink<K: NodeKey> = Option<Weak<RefCell<Node<K>>>>;

#[derive(Debug, Clone)]
pub enum Node<K: NodeKey> {
    Internal(InternalNode<K>),
    Leaf(LeafNode<K>),
}

// There's always one more edges than keys

#[derive(Debug, Clone)]
pub struct InternalNode<K: NodeKey> {
    keys: Vec<K>,
    edges: Vec<NodeLink<K>>,
    order: u16,
    upper: Option<K>,
    lower: Option<K>,
}

#[derive(Debug, Clone)]
pub struct LeafNode<K: NodeKey> {
    start_keys: Vec<K>,
    end_keys: Vec<K>,
    left_sibling: WeakNodeLink<K>,
    right_sibling: WeakNodeLink<K>,
    order: u16,
}

impl<K: NodeKey> InternalNode<K> {
    pub fn new(capacity: u16) -> Self {
        InternalNode {
            keys: Vec::new(),
            edges: Vec::new(),
            order: capacity,
            upper: None,
            lower: None,
        }
    }
}

impl<K: NodeKey> LeafNode<K> {
    pub fn new(capacity: u16) -> Self {
        LeafNode {
            start_keys: Vec::new(),
            end_keys: Vec::new(),
            left_sibling: None,
            right_sibling: None,
            order: capacity,
        }
    }

    // returns the newly created leaf
    pub fn split(&mut self) -> LeafNode<K> {
        let mid = self.start_keys.len() / 2;

        todo!()
    }
}

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
            root: Some(Rc::new(RefCell::new(Node::Leaf(LeafNode::new(capacity))))),
            order: capacity,
        }
    }

    // we assume there will at least always be one root
    // determines which leaf node a new key should go into
    pub fn search_leaf(&self, key: &K) -> NodeLink<K> {
        let mut temp_node = self.root.clone();

        loop {
            match temp_node.clone() {
                Some(node) => match &*node.borrow() {
                    Node::Internal(internal_node) => {
                        for (idx, k) in internal_node.keys.iter().enumerate() {
                            if key < k {
                                return internal_node.edges[idx].clone();
                            }

                            if idx == internal_node.keys.len() - 1 {
                                temp_node =
                                    internal_node.edges[internal_node.edges.len() - 1].clone();
                            }
                        }
                    }

                    Node::Leaf(_) => break,
                },
                None => panic!("should not be undefined"),
            }
        }

        temp_node
    }

    /**
     * First search for which leaf node the new key should go into.
     * If the leaf is not at capacity, insert it.
     * Otherwise, split the leaf:
     * - create a new leaf node and move half of the keys to the new node
     * - insert the new leaf's smallest key to the parent node
     * - if parent is full, split it too. Keep repeating the process until a parent doesn't need to split
     * - if the root splits, create a new root with one key and two children
     */
    pub fn insert(&self, range: Range<K>) -> () {}
}

mod Test {
    mod search {
        use std::{cell::RefCell, rc::Rc};

        use crate::latch_manager::latch_interval_btree::{BTree, InternalNode, LeafNode, Node};

        #[test]
        fn hello() {
            let order = 4;
            let first = Some(Rc::new(RefCell::new(Node::Leaf(LeafNode {
                start_keys: Vec::from([11]),
                end_keys: Vec::from([12]),
                left_sibling: None,
                right_sibling: None,
                order: order,
            }))));
            let second = Some(Rc::new(RefCell::new(Node::Leaf(LeafNode {
                start_keys: Vec::from([14]),
                end_keys: Vec::from([14]),
                left_sibling: None,
                right_sibling: None,
                order: order,
            }))));
            let third = Some(Rc::new(RefCell::new(Node::Leaf(LeafNode {
                start_keys: Vec::from([18]),
                end_keys: Vec::from([19]),
                left_sibling: None,
                right_sibling: None,
                order: order,
            }))));
            let fourth = Some(Rc::new(RefCell::new(Node::Leaf(LeafNode {
                start_keys: Vec::from([25]),
                end_keys: Vec::from([30]),
                left_sibling: None,
                right_sibling: None,
                order: order,
            }))));
            let node = InternalNode {
                keys: Vec::from([12, 15, 19]),
                edges: Vec::from([first.clone(), second.clone(), third.clone(), fourth.clone()]), //Vec<Option<Rc<RefCell<Node<K>>>>
                order: order,
                upper: None,
                lower: None,
            };
            let tree = BTree {
                root: Some(Rc::new(RefCell::new(Node::Internal(node)))),
                order: order,
            };
            let leaf1 = tree.search_leaf(&0);
            let unwrapped = leaf1.unwrap();
            let node = &*unwrapped.as_ref().borrow();
            match node {
                Node::Internal(_) => panic!("searched should not be internal node"),
                Node::Leaf(leaf) => {
                    assert_eq!(leaf.start_keys, Vec::from([11]))
                }
            }

            let leaf2 = tree.search_leaf(&15);
            let unwrapped = leaf2.unwrap();
            let node = &*unwrapped.as_ref().borrow();
            match node {
                Node::Internal(_) => panic!("searched should not be internal node"),
                Node::Leaf(leaf) => {
                    assert_eq!(leaf.start_keys, Vec::from([18]))
                }
            }

            let leaf4 = tree.search_leaf(&100);
            let unwrapped = leaf4.unwrap();
            let node = &*unwrapped.as_ref().borrow();
            match node {
                Node::Internal(_) => panic!("searched should not be internal node"),
                Node::Leaf(leaf) => {
                    assert_eq!(leaf.start_keys, Vec::from([25]))
                }
            }
            // match node {}
        }
    }
}
