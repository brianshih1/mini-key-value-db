use std::{
    borrow::Borrow,
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
}

// There's always one more edges than keys
#[derive(Debug, Clone)]
pub struct InternalNode<K: NodeKey> {
    keys: RefCell<Vec<K>>,
    edges: RefCell<Vec<NodeLink<K>>>,
    order: u16,
    upper: RefCell<Option<K>>,
    lower: RefCell<Option<K>>,
}

#[derive(Debug, Clone)]
pub struct LeafNode<K: NodeKey> {
    start_keys: RefCell<Vec<K>>,
    end_keys: RefCell<Vec<K>>,
    left_sibling: WeakNodeLink<K>,
    right_sibling: WeakNodeLink<K>,
    order: u16,
}

impl<K: NodeKey> InternalNode<K> {
    pub fn new(capacity: u16) -> Self {
        InternalNode {
            keys: RefCell::new(Vec::new()),
            edges: RefCell::new(Vec::new()),
            order: capacity,
            upper: RefCell::new(None),
            lower: RefCell::new(None),
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
    pub fn insert_node(&self, node: Rc<Node<K>>, key: K) -> () {
        // if key is greater than all elements, then the index is length of the keys (push)
        let mut insert_idx = self.keys.borrow().len();
        for (pos, k) in self.keys.borrow().iter().enumerate() {
            if &key < k {
                insert_idx = pos;
                break;
            }
        }
        self.keys.borrow_mut().insert(insert_idx, key);
        self.edges
            .borrow_mut()
            .insert(insert_idx - 1, RefCell::new(Some(node)));
    }
}

impl<K: NodeKey> LeafNode<K> {
    pub fn new(capacity: u16) -> Self {
        LeafNode {
            start_keys: RefCell::new(Vec::new()),
            end_keys: RefCell::new(Vec::new()),
            left_sibling: RefCell::new(None),
            right_sibling: RefCell::new(None),
            order: capacity,
        }
    }

    // order 4 means at most 3 keys per node
    pub fn has_capacity(&self) -> bool {
        self.start_keys.borrow().len() < usize::from(self.order)
    }

    pub fn insert_range(&self, range: Range<K>) {
        self.start_keys.borrow_mut().push(range.start_key);
        self.end_keys.borrow_mut().push(range.end_key);
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
    // we assume there will at least always be one root.
    // Returns the leaf node to add and the stack of parent nodes
    pub fn find_leaf_to_add(&self, key: &K) -> (Option<Rc<Node<K>>>, Vec<Rc<Node<K>>>) {
        let mut temp_node = self.root.borrow().clone();

        let mut next = None;
        let mut stack = Vec::new();
        loop {
            match temp_node {
                Some(ref node) => {
                    stack.push(node.clone());
                    match node.as_ref() {
                        Node::Internal(internal_node) => {
                            for (idx, k) in internal_node.keys.borrow().iter().enumerate() {
                                if key < k {
                                    return (
                                        internal_node.edges.borrow()[idx].borrow().clone(),
                                        stack,
                                    );
                                }

                                if idx == internal_node.keys.borrow().len() - 1 {
                                    next = internal_node.edges.borrow()
                                        [internal_node.edges.borrow().len() - 1]
                                        .borrow()
                                        .clone();
                                }
                            }
                        }

                        Node::Leaf(_) => break,
                    }
                }
                None => panic!("should not be undefined"),
            }

            match next {
                Some(_) => temp_node = next.clone(),
                None => panic!("next is not provided"),
            }
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
                    let mut idx = parent_stack.len() - 1;

                    loop {
                        let curr = parent_stack[idx].clone();
                        let curr_parent = curr.as_ref().as_internal_node();
                        curr_parent.insert_node(split_node.clone(), median.clone());
                        if curr_parent.has_capacity() {
                            break;
                        }
                        (split_node, median) = BTree::split_node(curr.clone());
                        idx = idx - 1;
                        if idx < 0 {
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
                let right_keys = internal_node.keys.borrow_mut().split_off(mid_idx);
                let mut right_edges = internal_node.edges.borrow_mut().split_off(mid_idx + 1);
                right_edges.insert(0, RefCell::new(None));
                let right_start = right_keys[0].clone();
                let new_right_node = InternalNode {
                    keys: RefCell::new(right_keys),
                    edges: RefCell::new(right_edges),
                    order: internal_node.order,
                    upper: RefCell::new(internal_node.upper.borrow().clone()),
                    lower: RefCell::new(Some(right_start.clone())),
                };
                (Rc::new(Node::Internal(new_right_node)), right_start)
            }
            Node::Leaf(leaf_node) => {
                let mid = leaf_node.start_keys.borrow().len() / 2;
                let right_start_keys = leaf_node.start_keys.borrow_mut().split_off(mid);

                let right_end_keys = leaf_node.end_keys.borrow_mut().split_off(mid);
                let right_sibling = leaf_node.right_sibling.borrow_mut().take();
                let right_start = right_start_keys[0].clone();

                let new_right_node = LeafNode {
                    start_keys: RefCell::new(right_start_keys),
                    end_keys: RefCell::new(right_end_keys),
                    left_sibling: RefCell::new(Some(Rc::downgrade(&node))), // TODO: set the left_sibling to the current leaf node later
                    right_sibling: RefCell::new(right_sibling),
                    order: leaf_node.order,
                };
                let right_rc = Rc::new(Node::Leaf(new_right_node));
                leaf_node
                    .right_sibling
                    .borrow_mut()
                    .replace(Rc::downgrade(&right_rc));
                (right_rc, right_start)
            }
        }
    }
}

mod Test {
    use std::{borrow::Borrow, cell::RefCell, rc::Rc};

    use super::{BTree, InternalNode, LeafNode, Node, NodeKey, NodeLink, WeakNodeLink};

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
                            .left_sibling
                            .borrow_mut()
                            .replace(Rc::downgrade(&leaves[idx - 1].clone()));
                    }

                    if idx < leaves.len() - 1 {
                        leaf_node
                            .right_sibling
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
                    upper: RefCell::new(Some(
                        internal_node.keys[internal_node.keys.len() - 1].clone(),
                    )),
                    lower: RefCell::new(Some(internal_node.keys[0].clone())),
                };
                (Rc::new(Node::Internal(ret_node)), leaves)
            }
            TestNode::Leaf(leaf_node) => {
                let leaf = Node::Leaf(LeafNode {
                    start_keys: RefCell::new(leaf_node.keys.clone()),
                    end_keys: RefCell::new(leaf_node.keys.clone()),
                    left_sibling: RefCell::new(None),
                    right_sibling: RefCell::new(None),
                    order: order,
                });
                let leaf_rc = Rc::new(leaf);
                (leaf_rc.clone(), Vec::from([leaf_rc.clone()]))
            }
        }
    }

    pub fn get_indent(depth: usize) -> String {
        " ".repeat(depth * 2)
    }

    pub fn print_tree<K: NodeKey>(link: &NodeLink<K>) {
        print_tree_internal(link, 0);
    }

    pub fn print_node_recursive<K: NodeKey>(node: Rc<Node<K>>) {
        print_tree(&RefCell::new(Some(node.clone())));
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
                    get_first_key_from_weak_link(&node.left_sibling),
                    get_first_key_from_weak_link(&node.right_sibling)
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
            let upgraded_ref = rc.upgrade();
            let unwrapped = upgraded_ref.unwrap();
            match unwrapped.as_ref() {
                Node::Internal(_) => {
                    panic!("Cannot get sibling from internal node");
                }
                Node::Leaf(ref node) => {
                    let keys = node.start_keys.borrow();
                    let first = keys.get(0);
                    match first {
                        Some(k) => Some(k.clone()),
                        None => todo!(),
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
                Node::Internal(node) => {
                    println!("{}Internal. Keys: {:?}", get_indent(depth), node.keys);

                    for edge in &*node.edges.borrow() {
                        print_tree_internal(edge, depth + 1);
                    }
                }
                Node::Leaf(node) => {
                    println!(
                        "{}Leaf. Keys: {:?}. Left start: {:?} Right start: {:?}",
                        get_indent(depth),
                        node.start_keys,
                        get_first_key_from_weak_link(&node.left_sibling),
                        get_first_key_from_weak_link(&node.right_sibling)
                    );
                }
            }
        }
    }
    /**
     * Given a node link and a test node structure, verify if if the node link
     * has the expected shape and properties
     */
    fn assert_node<K: NodeKey>(node: Rc<Node<K>>, test_node: TestNode<K>) {
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
                            assert_node(child_node.clone(), unwrapped);
                        }
                        None => {
                            assert_eq!(test_internal_node.edges[idx].is_none(), true);
                        }
                    };
                }
            }
            TestNode::Leaf(test_leaf) => {
                assert_leaf(node, test_leaf.keys);
            }
        }
    }

    fn assert_leaf<K: NodeKey>(node: Rc<Node<K>>, start_keys: Vec<K>) {
        match &node.as_ref() {
            Node::Internal(_) => panic!("not a leaf node"),
            Node::Leaf(leaf) => {
                assert_eq!(&*leaf.start_keys.borrow(), &start_keys)
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
            Test::{assert_internal, assert_leaf, print_tree},
        };

        #[test]
        fn one_level_deep() {
            let order = 4;
            let first = RefCell::new(Some(Rc::new(Node::Leaf(LeafNode {
                start_keys: RefCell::new(Vec::from([11])),
                end_keys: RefCell::new(Vec::from([12])),
                left_sibling: RefCell::new(None),
                right_sibling: RefCell::new(None),
                order: order,
            }))));

            let second = RefCell::new(Some(Rc::new(Node::Leaf(LeafNode {
                start_keys: RefCell::new(Vec::from([14])),
                end_keys: RefCell::new(Vec::from([14])),
                left_sibling: RefCell::new(None),
                right_sibling: RefCell::new(None),
                order: order,
            }))));

            let third = RefCell::new(Some(Rc::new(Node::Leaf(LeafNode {
                start_keys: RefCell::new(Vec::from([18])),
                end_keys: RefCell::new(Vec::from([19])),
                left_sibling: RefCell::new(None),
                right_sibling: RefCell::new(None),
                order: order,
            }))));
            let fourth = RefCell::new(Some(Rc::new(Node::Leaf(LeafNode {
                start_keys: RefCell::new(Vec::from([25])),
                end_keys: RefCell::new(Vec::from([30])),
                left_sibling: RefCell::new(None),
                right_sibling: RefCell::new(None),
                order: order,
            }))));

            let node = InternalNode {
                keys: RefCell::new(Vec::from([12, 15, 19])),
                edges: RefCell::new(Vec::from([
                    first.clone(),
                    second.clone(),
                    third.clone(),
                    fourth.clone(),
                ])), //Vec<Option<Rc<RefCell<Node<K>>>>
                order: order,
                upper: RefCell::new(None),
                lower: RefCell::new(None),
            };
            let node_rc = Rc::new(Node::Internal(node));
            let tree = BTree {
                root: RefCell::new(Some(node_rc)),
                order: order,
            };
            let (leaf1, stack) = tree.find_leaf_to_add(&0);
            assert_eq!(stack.len(), 1);
            assert_internal(stack[0].clone(), Vec::from([12, 15, 19]));

            assert_leaf(leaf1.unwrap(), Vec::from([11]));

            let leaf2 = tree.find_leaf_to_add(&15).0.unwrap();
            assert_leaf(leaf2, Vec::from([18]));

            let leaf4 = tree.find_leaf_to_add(&100).0.unwrap();
            assert_leaf(leaf4, Vec::from([25]));

            print_tree(&tree.root);
        }
    }

    mod split {
        use std::{borrow::Borrow, cell::RefCell, rc::Rc};

        use crate::latch_manager::latch_interval_btree::{
            BTree, LeafNode, Node,
            Test::{assert_node, get_start_keys_from_weak_link, print_node},
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
                    None,
                ]),
            });
            let node = create_test_node(&test_node, 4);
            let (split_node, median) = BTree::split_node(node.clone());
            assert_eq!(median, 20);

            let split_test_node = TestNode::Internal(TestInternalNode {
                keys: Vec::from([20, 30]),
                edges: Vec::from([
                    None,
                    Some(TestNode::Leaf(TestLeafNode {
                        keys: Vec::from([21, 25]),
                    })),
                    None,
                ]),
            });
            assert_node(split_node.clone(), split_test_node);
            print_node_recursive(split_node.clone());
        }

        #[test]
        fn split_leaf() {
            let leaf = LeafNode {
                start_keys: RefCell::new(Vec::from([0, 1, 2])),
                end_keys: RefCell::new(Vec::from([0, 1, 2])),
                left_sibling: RefCell::new(None),
                right_sibling: RefCell::new(None),
                order: 4,
            };

            let leaf_rc = Rc::new(Node::Leaf(leaf));
            let right_sibling = LeafNode {
                start_keys: RefCell::new(Vec::from([4, 5, 6])),
                end_keys: RefCell::new(Vec::from([0, 1, 2])),
                left_sibling: RefCell::new(Some(Rc::downgrade(&leaf_rc))),
                right_sibling: RefCell::new(None),
                order: 4,
            };
            let right_sibling_rc = Rc::new(Node::Leaf(right_sibling));
            match leaf_rc.as_ref() {
                Node::Internal(_) => panic!("Leaf is somehow internal"),
                Node::Leaf(leaf) => leaf
                    .right_sibling
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
                    let left_start_keys = get_start_keys_from_weak_link(&leaf.left_sibling);
                    match left_start_keys.clone() {
                        Some(left_start_keys) => {
                            assert_eq!(left_start_keys, Vec::from([0]));
                        }
                        None => panic!("Left key has start keys"),
                    }
                    let right_start_keys = get_start_keys_from_weak_link(&leaf.right_sibling);
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
}
