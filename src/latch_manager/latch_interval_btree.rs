struct Foo {}

pub trait NodeKey: std::fmt::Debug + Clone + Eq + PartialOrd + Ord {}

impl NodeKey for i32 {}

type NodeLink<K: NodeKey> = Option<Box<Node<K>>>;

#[derive(Debug, PartialEq, Clone)]
pub enum Node<K: NodeKey> {
    Internal(InternalNode<K>),
    Leaf(LeafNode<K>),
}

// There's always one more edges than keys

#[derive(Debug, PartialEq, Clone)]
pub struct InternalNode<K: NodeKey> {
    keys: Vec<K>,
    edges: Vec<NodeLink<K>>,
    order: u16,
    upper: Option<K>,
    lower: Option<K>,
}

#[derive(Debug, PartialEq, Clone)]
pub struct LeafNode<K: NodeKey> {
    start_keys: Vec<K>,
    end_keys: Vec<K>,
    left_sibling: NodeLink<K>,
    right_sibling: NodeLink<K>,
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
            root: Some(Box::new(Node::Leaf(LeafNode::new(capacity)))),
            order: capacity,
        }
    }

    // we assume there will at least always be one root
    // determines which leaf node a new key should go into
    pub fn search_leaf(&self, key: &K) -> &NodeLink<K> {
        let mut temp_node = &self.root;

        while let Some(Node::Internal(internal_node)) = temp_node.as_deref() {
            for (idx, k) in internal_node.keys.iter().enumerate() {
                if key < k {
                    temp_node = &internal_node.edges[idx];
                    break;
                }

                if idx == internal_node.keys.len() - 1 {
                    temp_node = &internal_node.edges[internal_node.edges.len() - 1];
                }
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
        use crate::latch_manager::latch_interval_btree::{BTree, InternalNode, LeafNode, Node};

        #[test]
        fn test_search() {
            let order = 4;
            let first = Some(Box::new(Node::Leaf(LeafNode {
                start_keys: Vec::from([11]),
                end_keys: Vec::from([12]),
                left_sibling: None,
                right_sibling: None,
                order: order,
            })));
            let third = Some(Box::new(Node::Leaf(LeafNode {
                start_keys: Vec::from([16]),
                end_keys: Vec::from([17]),
                left_sibling: None,
                right_sibling: None,
                order: order,
            })));
            let internal_node = InternalNode {
                keys: Vec::from([12, 15, 19]),
                edges: Vec::from([
                    first.clone(),
                    Some(Box::new(Node::Leaf(LeafNode::new(order)))),
                    third.clone(),
                    Some(Box::new(Node::Leaf(LeafNode {
                        start_keys: Vec::from([23]),
                        end_keys: Vec::from([28]),
                        left_sibling: None,
                        right_sibling: None,
                        order: order,
                    }))),
                ]),
                order: order,
                upper: None,
                lower: None,
            };
            let tree = BTree {
                root: Some(Box::new(Node::Internal(internal_node))),
                order: order,
            };
            let leaf1 = tree.search_leaf(&0);
            assert_eq!(leaf1, &first);

            let leaf2 = tree.search_leaf(&17);
            assert_eq!(leaf2, &third);
        }
    }
}
