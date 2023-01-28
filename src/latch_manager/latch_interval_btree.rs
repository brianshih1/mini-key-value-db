use std::{
    cell::RefCell,
    rc::{Rc, Weak},
};

struct Foo {}

pub trait NodeKey: std::fmt::Debug + Clone + Eq + PartialOrd + Ord {}

impl NodeKey for i32 {}

type NodeLink<K: NodeKey> = RefCell<Option<Rc<Node<K>>>>;
// RefCell<Option<Rc<RBTNode<T>>>>
type WeakNodeLink<K: NodeKey> = RefCell<Option<Weak<Node<K>>>>;
// RefCell<Option<Weak<RBTNode<T>>>>,

#[derive(Debug, Clone)]
pub enum Node<K: NodeKey> {
    Internal(InternalNode<K>),
    Leaf(LeafNode<K>),
}

pub fn get_indent(depth: usize) -> String {
    " ".repeat(depth * 2)
}

pub fn print_tree<K: NodeKey>(link: &NodeLink<K>) {
    print_tree_internal(link, 0);
}

fn print_tree_internal<K: NodeKey>(link: &NodeLink<K>, depth: usize) {
    let edge = link.borrow().clone();
    if let Some(ref rc) = edge {
        let node = rc.as_ref();
        match node {
            Node::Internal(node) => {
                println!("{}Internal. Keys: {:?}", get_indent(depth), node.keys);
                for edge in &node.edges {
                    print_tree_internal(edge, depth + 1);
                }
            }
            Node::Leaf(node) => {
                println!("{}Leaf. Keys: {:?}", get_indent(depth), node.start_keys);
            }
        }
    }
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
            left_sibling: RefCell::new(None),
            right_sibling: RefCell::new(None),
            order: capacity,
        }
    }

    // returns the newly created leaf
    // We assume leaf's capacity is at least 2
    pub fn split(&mut self) -> InternalNode<K> {
        let mid = self.start_keys.len() / 2;
        let mut right_start_keys = self.start_keys.split_off(mid);
        let mid_start_key = right_start_keys.remove(0);

        let mut right_end_keys = self.end_keys.split_off(mid);
        let mid_end_key = right_end_keys.remove(0);

        // let new_right_node = LeafNode {
        //     start_keys: right_start_keys,
        //     end_keys: right_end_keys,
        //     left_sibling: self
        // }
        // let new_right_node =
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
            root: RefCell::new(Some(Rc::new(Node::Leaf(LeafNode::new(capacity))))),
            order: capacity,
        }
    }

    // determines which leaf node a new key should go into
    // we assume there will at least always be one root
    pub fn find_leaf_to_add(&self, key: &K) -> Option<Rc<Node<K>>> {
        let mut temp_node = self.root.borrow().clone();

        let mut next = None;
        loop {
            match temp_node {
                Some(ref node) => match node.as_ref() {
                    Node::Internal(internal_node) => {
                        for (idx, k) in internal_node.keys.iter().enumerate() {
                            if key < k {
                                return internal_node.edges[idx].borrow().clone();
                            }

                            if idx == internal_node.keys.len() - 1 {
                                next = internal_node.edges[internal_node.edges.len() - 1]
                                    .borrow()
                                    .clone();
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

    pub fn split_leaf(leaf: Rc<RefCell<Node<K>>>) -> () {}
}

mod Test {
    mod search {
        use std::{cell::RefCell, rc::Rc};

        use crate::latch_manager::latch_interval_btree::{
            print_tree, BTree, InternalNode, LeafNode, Node,
        };

        #[test]
        fn one_level_deep() {
            let order = 4;
            let first = RefCell::new(Some(Rc::new(Node::Leaf(LeafNode {
                start_keys: Vec::from([11]),
                end_keys: Vec::from([12]),
                left_sibling: RefCell::new(None),
                right_sibling: RefCell::new(None),
                order: order,
            }))));

            let second = RefCell::new(Some(Rc::new(Node::Leaf(LeafNode {
                start_keys: Vec::from([14]),
                end_keys: Vec::from([14]),
                left_sibling: RefCell::new(None),
                right_sibling: RefCell::new(None),
                order: order,
            }))));

            let third = RefCell::new(Some(Rc::new(Node::Leaf(LeafNode {
                start_keys: Vec::from([18]),
                end_keys: Vec::from([19]),
                left_sibling: RefCell::new(None),
                right_sibling: RefCell::new(None),
                order: order,
            }))));
            let fourth = RefCell::new(Some(Rc::new(Node::Leaf(LeafNode {
                start_keys: Vec::from([25]),
                end_keys: Vec::from([30]),
                left_sibling: RefCell::new(None),
                right_sibling: RefCell::new(None),
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
                root: RefCell::new(Some(Rc::new(Node::Internal(node)))),
                order: order,
            };
            let leaf1 = tree.find_leaf_to_add(&0).unwrap();

            match &leaf1.as_ref() {
                Node::Internal(_) => panic!("searched should not be internal node"),
                Node::Leaf(leaf) => {
                    assert_eq!(leaf.start_keys, Vec::from([11]))
                }
            }

            let leaf2 = tree.find_leaf_to_add(&15).unwrap();
            match &leaf2.as_ref() {
                Node::Internal(_) => panic!("searched should not be internal node"),
                Node::Leaf(leaf) => {
                    assert_eq!(leaf.start_keys, Vec::from([18]))
                }
            }

            let leaf4 = tree.find_leaf_to_add(&100).unwrap();
            match &leaf4.as_ref() {
                Node::Internal(_) => panic!("searched should not be internal node"),
                Node::Leaf(leaf) => {
                    assert_eq!(leaf.start_keys, Vec::from([25]))
                }
            }

            print_tree(&tree.root);
        }
    }
}
