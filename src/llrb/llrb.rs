use std::{
    borrow::{Borrow, BorrowMut},
    cell::{Ref, RefCell, RefMut},
    cmp::Ordering,
    fmt,
    rc::Rc,
};

pub trait NodeKey: Copy + Eq + PartialOrd + Ord + std::fmt::Display {}

struct Tree<K: NodeKey, V> {
    root: NodeLink<K, V>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Copy)]
pub enum TreeColor {
    RED,
    BLACK,
}

impl fmt::Display for TreeColor {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TreeColor::RED => write!(f, "R"),
            TreeColor::BLACK => write!(f, "B"),
        }
    }
}

type NodeLink<K: NodeKey, V> = Option<Box<Node<K, V>>>;

#[derive(Debug, Clone, PartialEq, Eq)]
struct Node<K: NodeKey, V> {
    start_key: K,
    end_key: K,
    value: V,
    left_node: NodeLink<K, V>,
    right_node: NodeLink<K, V>,
    color: TreeColor,
}

trait LinkHelper<K: NodeKey, V> {
    fn color(&self) -> TreeColor;

    fn left(&self) -> &NodeLink<K, V>;
}

impl<K: NodeKey, V> LinkHelper<K, V> for NodeLink<K, V> {
    fn color(&self) -> TreeColor {
        todo!()
    }

    fn left(&self) -> &NodeLink<K, V> {
        todo!()
    }
}

impl<K: NodeKey, V> fmt::Display for Node<K, V> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "({}, {})", self.start_key, self.end_key)
    }
}

impl<K: NodeKey, V> Node<K, V> {
    fn new_node(color: TreeColor, start_key: K, end_key: K, value: V) -> Self {
        Node {
            start_key: start_key,
            end_key: end_key,
            left_node: None,
            right_node: None,
            value: value,
            color,
        }
    }
}

impl<K: NodeKey, V> Tree<K, V> {
    fn new_with_node(root_node: Node<K, V>) -> Self {
        Tree {
            root: Some(Box::new(root_node)),
        }
    }

    fn new() -> Self {
        Tree { root: None }
    }

    fn insert_node(&mut self, start_key: K, end_key: K, value: V) -> () {
        // let mut cur = Self::get_node_link(&self.root);
        // let mut prev: NodeLink<K, V> = None;

        // while cur.is_some() {
        //     let node_box = cur.unwrap();
        //     prev = Some(Rc::clone(&node_box));
        //     if self.root.is_none() {
        //         self.root = Some(Rc::clone(&node_box));
        //     }
        //     let node = node_box.as_ref().borrow();
        //     let ord = node.start_key.cmp(&start_key);
        //     match ord {
        //         Ordering::Less => {
        //             cur = Self::get_node_link(&node.right_node);
        //         }
        //         Ordering::Equal => {
        //             // TODO: If the interval comes from the same ID, just update end.
        //             // Otherwise, if ID is less, go left, else go right. For now, we will just always
        //             // make it go left
        //             cur = Self::get_node_link(&node.left_node);
        //         }
        //         Ordering::Greater => {
        //             cur = Self::get_node_link(&node.left_node);
        //         }
        //     }
        // }
        // // new nodes are red

        // match prev {
        //     Some(node_rc) => {
        //         let node = Some(Rc::new(RefCell::new(Node::new_node(
        //             TreeColor::RED,
        //             start_key,
        //             end_key,
        //             value,
        //             Some(Rc::clone(&node_rc)),
        //         ))));
        //         let mut node_ref = node_rc.as_ref().borrow_mut();
        //         node_ref.parent_node = Some(Rc::clone(&node_rc));
        //         let ord = node_ref.start_key.cmp(&start_key);

        //         match ord {
        //             Ordering::Less => {
        //                 node_ref.right_node = Self::get_node_link(&node);
        //             }
        //             Ordering::Equal => todo!(),
        //             Ordering::Greater => {
        //                 node_ref.left_node = Self::get_node_link(&node);
        //             }
        //         }
        //         self.fix_insert(Self::get_node_link(&node));
        //     }
        //     None => {
        //         self.root = Some(Rc::new(RefCell::new(Node::new_node(
        //             TreeColor::BLACK,
        //             start_key,
        //             end_key,
        //             value,
        //             Self::get_node_link(&prev),
        //         ))));
        //         return;
        //     }
        // };
    }

    pub fn fix_insert(&mut self, link: NodeLink<K, V>) {
        // let temp = &link;
        // while link.color() == TreeColor::BLACK {
        //     let parent = &temp.clone().unwrap().as_ref().borrow().parent_node;
        // }
        todo!()
    }

    pub fn left_rotate(link: NodeLink<K, V>) {}

    pub fn right_rotate(link: NodeLink<K, V>) {}

    // preorder print of the tree
    pub fn print_tree(&self) {
        self.print_tree_internal(&self.root, 0);
    }

    pub fn print_tree_internal(&self, node: &NodeLink<K, V>, depth: usize) {
        let indent = Self::get_indent(depth * 3);
        match &node {
            Some(node_rc) => {
                let node = node_rc.as_ref().borrow();
                println!(
                    "{}{}({}, {})",
                    &indent, node.color, node.start_key, node.end_key
                );
                self.print_tree_internal(&node.left_node, depth + 1);
                self.print_tree_internal(&node.right_node, depth + 1);
            }
            None => println!("{}None", &indent),
        }
    }

    pub fn get_indent(depth: usize) -> String {
        " ".repeat(depth)
    }

    // turns the tree into a list. This is for testing purposes
    // to assert the sorted list against expected list
    // Note: checking inorder + preorder is enough to verify if two trees are structurally the same
    pub fn to_inorder_list(&self) -> Vec<K> {
        Self::to_inorder_list_internal(&self.root)
    }

    pub fn to_inorder_list_internal(link: &NodeLink<K, V>) -> Vec<K> {
        let mut vec = Vec::new();
        match &link {
            Some(node_rc) => {
                let node = node_rc.as_ref().borrow();
                let key = node.start_key;
                let mut left_tree_list = Self::to_inorder_list_internal(&node.left_node);
                vec.append(&mut left_tree_list);
                vec.push(key);
                let mut right_tree_list = Self::to_inorder_list_internal(&node.right_node);
                vec.append(&mut right_tree_list);
            }
            None => {}
        };
        vec
    }
}

impl NodeKey for i32 {}

mod Test {
    use std::{cell::RefCell, rc::Rc};

    use crate::llrb::llrb::{Node, TreeColor};

    use super::Tree;

    #[test]
    fn test_print_tree() {
        let str = Tree::<i32, i32>::get_indent(3);
        println!("Test: {}foo", str.to_string());
        let mut node = Node::new_node(TreeColor::RED, 3, 3, 12);
        node.left_node = Some(Box::new(Node::new_node(TreeColor::RED, 4, 4, 12)));
        node.right_node = Some(Box::new(Node::new_node(TreeColor::RED, 4, 4, 12)));
        let tree = Tree::new_with_node(node);
        tree.print_tree();
    }

    mod insert_node {
        use crate::llrb::llrb::Tree;

        #[test]
        fn insert_into_empty_tree() {
            let mut tree = Tree::<i32, i32>::new();
            tree.insert_node(2, 3, 1);
            assert_eq!(tree.to_inorder_list(), Vec::from([2]));
        }

        #[test]
        fn insert_a_few_elements() {
            let mut tree = Tree::<i32, i32>::new();
            tree.insert_node(2, 3, 1);
            tree.insert_node(1, 3, 1);
            tree.insert_node(3, 3, 1);
            tree.insert_node(0, 3, 1);
            assert_eq!(tree.to_inorder_list(), Vec::from([0, 1, 2, 3]));
        }
    }

    #[test]
    fn test() {
        let first = Rc::new(12);
        let second = Rc::new(13);
    }
}
