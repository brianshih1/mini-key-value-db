mod Test {
    use std::{
        borrow::Borrow,
        cell::RefCell,
        process::Child,
        rc::Rc,
        sync::{Arc, RwLock},
    };

    use crate::latch_manager::latch_interval_btree::{
        BTree, InternalNode, LatchNode, LatchWaiters, LeafNode, Node, NodeKey, NodeLink,
        WeakNodeLink,
    };

    pub fn find_node_and_parent_with_indices<K: NodeKey>(
        tree: &BTree<K>,
        indices: Vec<usize>,
    ) -> (LatchNode<K>, LatchNode<K>, usize) {
        let mut temp = tree.root.write().unwrap().clone().unwrap();
        let mut parent = None;

        for (pos, node_idx) in indices.iter().enumerate() {
            let cloned = temp.clone();
            let read_guard = cloned.read().unwrap();
            match &*read_guard {
                Node::Internal(internal_node) => {
                    parent = Some(temp.clone());
                    temp = internal_node.edges.write().unwrap()[*node_idx]
                        .write()
                        .unwrap()
                        .clone()
                        .unwrap();
                }
                Node::Leaf(_) => {}
            }
            if pos + 1 == indices.len() {
                return (temp.clone(), parent.unwrap(), node_idx.clone());
            }
        }
        panic!("Reached end")
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
            root: RwLock::new(Some(node)),
            order,
        }
    }

    pub fn create_test_node<K: NodeKey>(node: &TestNode<K>, order: u16) -> LatchNode<K> {
        let (node, leaves) = create_tree_from_test_node_internal(node, order);

        for (idx, child) in leaves.iter().enumerate() {
            let guard = child.write().unwrap();
            match &*guard {
                Node::Internal(_) => panic!("Node must be a leaf"),
                Node::Leaf(leaf_node) => {
                    if idx > 0 {
                        leaf_node
                            .left_ptr
                            .write()
                            .unwrap()
                            .replace(Arc::downgrade(&leaves[idx - 1].clone()));
                    }

                    if idx < leaves.len() - 1 {
                        leaf_node
                            .right_ptr
                            .write()
                            .unwrap()
                            .replace(Arc::downgrade(&leaves[idx + 1].clone()));
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
    ) -> (LatchNode<K>, Vec<LatchNode<K>>) {
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
                            RwLock::new(Some(child_node))
                            // todo!()
                        }
                        None => RwLock::new(None),
                    })
                    .collect::<Vec<NodeLink<K>>>();

                let ret_node = InternalNode {
                    keys: RwLock::new(internal_node.keys.clone()),
                    edges: RwLock::new(edges),
                    order,
                };
                (Arc::new(RwLock::new(Node::Internal(ret_node))), leaves)
            }
            TestNode::Leaf(leaf_node) => {
                let leaf = Node::Leaf(LeafNode {
                    start_keys: RwLock::new(leaf_node.keys.clone()),
                    end_keys: RwLock::new(leaf_node.keys.clone()),
                    left_ptr: RwLock::new(None),
                    right_ptr: RwLock::new(None),
                    order: order,
                    waiters: RwLock::new(Vec::from([RwLock::new(LatchWaiters {
                        senders: Vec::new(),
                    })])),
                });
                let leaf_latch = Arc::new(RwLock::new(leaf));
                (leaf_latch.clone(), Vec::from([leaf_latch.clone()]))
            }
        }
    }

    pub fn get_indent(depth: usize) -> String {
        " ".repeat(depth * 2)
    }

    pub fn print_tree<K: NodeKey>(tree: &BTree<K>) {
        print_tree_internal(&tree.root, 0);
    }

    pub fn print_node_recursive<K: NodeKey>(node: LatchNode<K>) {
        let tree = BTree {
            root: RwLock::new(Some(node.clone())),
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
        let edge = &*link.read().unwrap();
        if let Some(ref rc) = edge {
            let upgraded_ref = rc.upgrade();
            let unwrapped = upgraded_ref.unwrap();
            let guard = unwrapped.read().unwrap();
            match &*guard {
                Node::Internal(_) => {
                    panic!("Cannot get sibling from internal node");
                }
                Node::Leaf(ref node) => {
                    let keys = node.start_keys.read().unwrap();
                    Some(keys.clone())
                }
            }
        } else {
            None
        }
    }

    fn get_first_key_from_weak_link<K: NodeKey>(link: &WeakNodeLink<K>) -> Option<K> {
        let edge = &*link.read().unwrap();
        if let Some(ref rc) = edge {
            let upgraded_ref = rc.upgrade()?;
            let guard = upgraded_ref.read().unwrap();
            match &*guard {
                Node::Internal(_) => {
                    panic!("Cannot get sibling from internal node");
                }
                Node::Leaf(ref node) => {
                    let keys = node.start_keys.read().unwrap();
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
        let edge = link.read().unwrap().clone();
        if let Some(ref latch) = edge {
            let node = latch.as_ref();
            let guard = latch.read().unwrap();
            match &*guard {
                Node::Internal(ref node) => {
                    println!(
                        "{}Internal. Keys: {:?}",
                        get_indent(depth),
                        node.keys.borrow()
                    );

                    for edge in &*node.edges.read().unwrap() {
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

    fn assert_node_and_leaves_siblings<K: NodeKey>(node: LatchNode<K>, test_node: &TestNode<K>) {
        assert_node(node.clone(), test_node);
        let test_leaves = get_all_test_leaves(test_node);
        let leaves = get_all_leaf_nodes(node.clone());
        assert_eq!(test_leaves.len(), leaves.len());
        for (idx, current_test_node) in test_leaves.iter().enumerate() {
            let curr_node = leaves[idx].clone();
            let guard = curr_node.read().unwrap();
            let leaf_node = guard.as_leaf_node();
            let left_sibling = leaf_node.left_ptr.read().unwrap();
            let right_sibling = leaf_node.right_ptr.read().unwrap();
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
    fn assert_node<K: NodeKey>(node: LatchNode<K>, test_node: &TestNode<K>) {
        match test_node {
            TestNode::Internal(test_internal_node) => {
                let guard = node.write().unwrap();
                let internal_node = guard.as_internal_node();
                assert_eq!(
                    &*internal_node.keys.read().unwrap(),
                    &test_internal_node.keys
                );
                for (idx, child) in internal_node.edges.read().unwrap().iter().enumerate() {
                    let node = child.read().unwrap();
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
        let root = tree.root.borrow().clone().read().unwrap().clone().unwrap();
        assert_node(root, test_node);
    }

    fn get_all_leaves<K: NodeKey>(node: LatchNode<K>) -> Vec<Option<LatchNode<K>>> {
        let mut leaves = Vec::new();
        let guard = node.read().unwrap();
        match &*guard {
            Node::Internal(internal_node) => {
                for edge in internal_node.edges.read().unwrap().iter() {
                    match &*edge.read().unwrap() {
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
        node: LatchNode<K>,
        test_leaf: &TestLeafNode<K>,
        test_left_sibling: &Option<TestLeafNode<K>>,
        test_right_sibling: &Option<TestLeafNode<K>>,
    ) {
        assert_leaf(node.clone(), &test_leaf.keys);
        let guard = node.read().unwrap();
        let leaf_node = guard.as_leaf_node();
        let left_sibling = &*leaf_node.left_ptr.read().unwrap();
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

        let right_sibling = &*leaf_node.right_ptr.read().unwrap();
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

    fn get_all_leaf_nodes<K: NodeKey>(node: LatchNode<K>) -> Vec<LatchNode<K>> {
        let mut leaves = Vec::new();
        let guard = node.read().unwrap();
        match &*guard {
            Node::Internal(internal_node) => {
                for edge in internal_node.edges.read().unwrap().iter() {
                    if let Some(child) = &*edge.read().unwrap() {
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

    fn assert_leaf<K: NodeKey>(node: LatchNode<K>, start_keys: &Vec<K>) {
        let guard = node.read().unwrap();
        match &*guard {
            Node::Internal(_) => panic!("not a leaf node"),
            Node::Leaf(leaf) => {
                assert_eq!(&*leaf.start_keys.read().unwrap(), start_keys)
            }
        }
    }

    fn assert_internal<K: NodeKey>(node: Rc<Node<K>>, start_keys: Vec<K>) {
        match &node.as_ref() {
            Node::Internal(internal_node) => {
                assert_eq!(&*internal_node.keys.read().unwrap(), &start_keys)
            }
            Node::Leaf(_) => panic!("not an internal node"),
        }
    }

    mod split {
        use std::{borrow::Borrow, cell::RefCell, rc::Rc};

        use crate::latch_manager::{
            latch_interval_btree::{BTree, LeafNode, Node},
            latch_interval_btree_test::Test::{
                assert_leaf_with_siblings, assert_node, find_node_and_parent_with_indices,
                get_all_leaves, get_start_keys_from_weak_link,
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
            let guard = node.write().unwrap();
            let (split_node, median) = guard.as_internal_node().split();
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
            let test_node = TestNode::Internal(TestInternalNode {
                keys: Vec::from([4]),
                edges: Vec::from([
                    Some(TestNode::Leaf(TestLeafNode {
                        keys: Vec::from([0, 1, 2]),
                    })),
                    Some(TestNode::Leaf(TestLeafNode {
                        keys: Vec::from([4, 5, 6]),
                    })),
                ]),
            });
            let tree = create_test_tree(&test_node, 4);
            let (node_latch, parent_latch, _) =
                find_node_and_parent_with_indices(&tree, Vec::from([0]));
            let guard = node_latch.write().unwrap();
            let leaf = guard.as_leaf_node();
            let parent_guard = parent_latch.write().unwrap();

            let (split_node, right_start_key) = leaf.split(&node_latch);
            drop(guard);
            drop(parent_guard);
            assert_eq!(right_start_key, 1);

            let split_guard = split_node.write().unwrap();
            let split_leaf = split_guard.as_leaf_node();

            assert_eq!(&*split_leaf.start_keys.read().unwrap(), &Vec::from([1, 2]));
            assert_eq!(&*split_leaf.end_keys.read().unwrap(), &Vec::from([1, 2]));
            let left_start_keys = get_start_keys_from_weak_link(&split_leaf.left_ptr);
            match left_start_keys.clone() {
                Some(left_start_keys) => {
                    assert_eq!(left_start_keys, Vec::from([0]));
                }
                None => panic!("Left key has start keys"),
            }
            let right_start_keys = get_start_keys_from_weak_link(&split_leaf.right_ptr);
            match right_start_keys.clone() {
                Some(left_start_keys) => {
                    assert_eq!(left_start_keys, Vec::from([4, 5, 6]));
                }
                None => panic!("Right key has start keys"),
            }
        }
    }

    mod insert {
        use crate::latch_manager::latch_interval_btree::{BTree, LatchKeyGuard, Range};

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
            let res1 = tree.insert(Range {
                start_key: 5,
                end_key: 5,
            });

            assert!(matches!(res1, LatchKeyGuard::Acquired));
            let res2 = tree.insert(Range {
                start_key: 10,
                end_key: 10,
            });
            assert!(matches!(res2, LatchKeyGuard::Acquired));
            let res3 = tree.insert(Range {
                start_key: 20,
                end_key: 20,
            });
            assert!(matches!(res3, LatchKeyGuard::Acquired));

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
            let res4 = tree.insert(Range {
                start_key: 15,
                end_key: 15,
            });

            assert!(matches!(res4, LatchKeyGuard::Acquired));

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

        #[test]
        fn insert_duplicate() {
            let tree = BTree::<i32>::new(3);
            let res1 = tree.insert(Range {
                start_key: 5,
                end_key: 5,
            });
            assert!(matches!(res1, LatchKeyGuard::Acquired));
            let res2 = tree.insert(Range {
                start_key: 5,
                end_key: 5,
            });
            assert!(!matches!(res2, LatchKeyGuard::Acquired));
        }
    }

    mod leaf_underflow {
        use std::{cell::RefCell, sync::RwLock};

        use crate::latch_manager::latch_interval_btree::{LatchWaiters, LeafNode};

        #[test]
        fn underflows() {
            let leaf = LeafNode {
                start_keys: RwLock::new(Vec::from([0])),
                end_keys: RwLock::new(Vec::from([0])),
                left_ptr: RwLock::new(None),
                right_ptr: RwLock::new(None),
                order: 4,
                waiters: RwLock::new(Vec::from([RwLock::new(LatchWaiters {
                    senders: Vec::new(),
                })])),
            };
            assert!(leaf.is_underflow());
        }
    }

    mod delete {
        mod core_delete {
            use crate::latch_manager::latch_interval_btree_test::Test::{
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

        mod leaf_stealing {
            use crate::latch_manager::latch_interval_btree::Node;

            mod has_spare_keys {
                use std::{cell::RefCell, sync::RwLock};

                use crate::latch_manager::{
                    latch_interval_btree::{LatchWaiters, LeafNode},
                    latch_interval_btree_test::Test::{
                        assert_tree, create_test_tree, TestInternalNode, TestLeafNode, TestNode,
                    },
                };

                #[test]
                fn internal_node() {}

                #[test]
                fn leaf_node_has_spare_key() {
                    let leaf_node = LeafNode {
                        start_keys: RwLock::new(Vec::from([0, 1])),
                        end_keys: RwLock::new(Vec::from([0, 1])),
                        left_ptr: RwLock::new(None),
                        right_ptr: RwLock::new(None),
                        order: 3,
                        waiters: RwLock::new(Vec::from([RwLock::new(LatchWaiters {
                            senders: Vec::new(),
                        })])),
                    };
                    assert_eq!(leaf_node.has_spare_key(), true);
                }

                #[test]
                fn leaf_node_has_no_spare_key() {
                    let leaf_node = LeafNode {
                        start_keys: RwLock::new(Vec::from([0])),
                        end_keys: RwLock::new(Vec::from([0])),
                        left_ptr: RwLock::new(None),
                        right_ptr: RwLock::new(None),
                        order: 3,
                        waiters: RwLock::new(Vec::from([RwLock::new(LatchWaiters {
                            senders: Vec::new(),
                        })])),
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
                use crate::latch_manager::latch_interval_btree_test::Test::{
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
            use crate::latch_manager::latch_interval_btree_test::Test::{
                assert_tree, create_test_tree, find_node_and_parent_with_indices, TestInternalNode,
                TestLeafNode, TestNode,
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
                let (node_latch, parent_latch, edge_idx) =
                    find_node_and_parent_with_indices(&tree, Vec::from([1]));
                let guard = node_latch.write().unwrap();
                let temp = guard.as_internal_node();
                let parent_guard = parent_latch.write().unwrap();
                let did_steal = temp.steal_from_sibling(parent_guard.as_internal_node(), edge_idx);
                println!("Did steal {}", did_steal);
                drop(guard);
                drop(parent_guard);
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
                let (node_latch, parent_latch, edge_idx) =
                    find_node_and_parent_with_indices(&tree, Vec::from([1]));
                let guard = node_latch.write().unwrap();
                let temp = guard.as_internal_node();
                let parent_guard = parent_latch.write().unwrap();
                let did_steal = temp.steal_from_sibling(parent_guard.as_internal_node(), edge_idx);
                println!("Did steal {}", did_steal);
                drop(guard);
                drop(parent_guard);

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
            use crate::latch_manager::latch_interval_btree_test::Test::{
                assert_tree, create_test_tree, find_node_and_parent_with_indices, TestInternalNode,
                TestLeafNode, TestNode,
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
                let (node_latch, parent_latch, edge_idx) =
                    find_node_and_parent_with_indices(&tree, Vec::from([1]));
                let guard = node_latch.write().unwrap();
                let internal_node = guard.as_internal_node();
                let parent_guard = parent_latch.write().unwrap();
                internal_node.merge_with_sibling(parent_guard.as_internal_node(), edge_idx);

                drop(guard);
                drop(parent_guard);

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
                let (node_latch, parent_latch, edge_idx) =
                    find_node_and_parent_with_indices(&tree, Vec::from([0]));
                let node_guard = node_latch.write().unwrap();
                let internal_node = node_guard.as_internal_node();
                let parent_guard = parent_latch.write().unwrap();
                internal_node.merge_with_sibling(parent_guard.as_internal_node(), edge_idx);
                drop(node_guard);
                drop(parent_guard);
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
            use crate::latch_manager::latch_interval_btree_test::Test::{
                assert_tree, create_test_tree, find_node_and_parent_with_indices, print_tree,
                TestInternalNode, TestLeafNode, TestNode,
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
                let (leaf_latch, parent_latch, edge_idx) =
                    find_node_and_parent_with_indices(&tree, Vec::from([1]));
                let leaf_guard = leaf_latch.write().unwrap();
                let leaf = leaf_guard.as_leaf_node();
                let parent_guard = parent_latch.write().unwrap();

                leaf.merge_node(parent_guard.as_internal_node(), edge_idx);
                drop(leaf_guard);
                drop(parent_guard);
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

                let (leaf_latch, parent_latch, edge_idx) =
                    find_node_and_parent_with_indices(&tree, Vec::from([0]));
                let leaf_guard = leaf_latch.write().unwrap();
                let leaf = leaf_guard.as_leaf_node();
                let parent_guard = parent_latch.write().unwrap();

                leaf.merge_node(parent_guard.as_internal_node(), edge_idx);
                drop(leaf_guard);
                drop(parent_guard);
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
        for idx in 0..5 {
            println!("{}", idx);
        }
    }
}
