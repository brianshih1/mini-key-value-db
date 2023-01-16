use std::{
    borrow::{Borrow, BorrowMut},
    cell::{Ref, RefCell, RefMut},
    cmp::Ordering,
    fmt,
    rc::Rc,
};

pub trait NodeKey: Copy + Eq + PartialOrd + Ord + std::fmt::Display {}

struct Tree<K: NodeKey, V> {
    nodes: Vec<Node<K, V>>,
    root: usize,
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

const NIL: usize = std::usize::MAX;

#[derive(Debug, Clone, PartialEq, Eq)]
struct Node<K: NodeKey, V> {
    start_key: K,
    end_key: K,
    value: V,
    left_node: usize,
    right_node: usize,
    parent_node: usize,
    color: TreeColor,
}

impl<K: NodeKey, V> Node<K, V> {
    fn left<'a>(&self, tree: &'a Tree<K, V>) -> Option<&'a Node<K, V>> {
        if self.left_node == NIL {
            None
        } else {
            Some(&tree.nodes[self.left_node])
        }
    }

    fn right<'a>(&self, tree: &'a Tree<K, V>) -> Option<&'a Node<K, V>> {
        if self.right_node == NIL {
            None
        } else {
            Some(&tree.nodes[self.right_node])
        }
    }
}

impl<K: NodeKey, V> fmt::Display for Node<K, V> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "({}, {})", self.start_key, self.end_key)
    }
}

impl<K: NodeKey, V> Node<K, V> {
    fn new_node(color: TreeColor, start_key: K, end_key: K, value: V, parent_node: usize) -> Self {
        Node {
            start_key: start_key,
            end_key: end_key,
            left_node: NIL,
            right_node: NIL,
            value: value,
            parent_node,
            color,
        }
    }
}

impl<K: NodeKey, V> Tree<K, V> {
    fn new_with_node(root_node: Node<K, V>) -> Self {
        Tree {
            root: 0,
            nodes: Vec::from([root_node]),
        }
    }

    fn new() -> Self {
        Tree {
            root: NIL,
            nodes: Vec::new(),
        }
    }

    // returns the idx of the added node
    fn add_node(&mut self, node: Node<K, V>) -> usize {
        self.nodes.push(node);
        let idx = self.nodes.len() - 1;
        if self.root == NIL {
            self.root = idx;
        }
        idx
    }

    fn insert_node(&mut self, start_key: K, end_key: K, value: V) -> () {
        let mut cur = self.root;
        let mut prev: usize = NIL;

        while cur != NIL {
            let cur_node = self.get_node(cur);
            let ord = cur_node.start_key.cmp(&start_key);
            prev = cur;
            match ord {
                Ordering::Less => {
                    cur = cur_node.right_node;
                }
                Ordering::Equal => {
                    // TODO: If the interval comes from the same ID, just update end.
                    // Otherwise, if ID is less, go left, else go right. For now, we will just always
                    // make it go left
                    cur = cur_node.left_node;
                }
                Ordering::Greater => {
                    cur = cur_node.left_node;
                }
            }
        }

        if prev == NIL {
            self.add_node(Node::new_node(
                TreeColor::RED,
                start_key,
                end_key,
                value,
                NIL,
            ));
        } else {
            let new_idx = self.add_node(Node::new_node(
                TreeColor::RED,
                start_key,
                end_key,
                value,
                prev,
            ));
            let mut parent_node = self.get_mut_node(prev);
            let ord = parent_node.start_key.cmp(&start_key);
            match ord {
                Ordering::Less => {
                    parent_node.right_node = new_idx;
                }
                Ordering::Equal => {
                    parent_node.left_node = new_idx;
                }
                Ordering::Greater => {
                    parent_node.left_node = new_idx;
                }
            }
            self.fix_insert(new_idx);
        }
    }

    pub fn fix_insert(&mut self, node: usize) {
        // let temp = &link;
        // while link.color() == TreeColor::BLACK {
        //     let parent = temp.unwrap().as_ref().borrow().parent_node;
        // }
    }

    pub fn left_rotate(link: &Node<K, V>) {}

    pub fn right_rotate(link: &Node<K, V>) {}

    pub fn get_node(&self, idx: usize) -> &Node<K, V> {
        &self.nodes[idx]
    }

    pub fn get_mut_node(&mut self, idx: usize) -> &mut Node<K, V> {
        &mut self.nodes[idx]
    }

    pub fn get_root(&self) -> Option<&Node<K, V>> {
        if self.root == NIL {
            None
        } else {
            Some(self.get_node(self.root))
        }
    }

    // preorder print of the tree
    pub fn print_tree(&self) {
        self.print_tree_internal(self.get_root(), 0);
    }

    pub fn print_tree_internal(&self, node: Option<&Node<K, V>>, depth: usize) {
        let indent = Self::get_indent(depth * 3);
        match &node {
            Some(node) => {
                println!(
                    "{}{}({}, {})",
                    &indent, node.color, node.start_key, node.end_key
                );
                self.print_tree_internal(node.left(&self), depth + 1);
                self.print_tree_internal(node.right(&self), depth + 1);
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
        self.to_inorder_list_internal(self.get_root())
    }

    pub fn to_inorder_list_internal(&self, link: Option<&Node<K, V>>) -> Vec<K> {
        let mut vec = Vec::new();
        match &link {
            Some(node) => {
                let key = node.start_key;
                let mut left_tree_list = self.to_inorder_list_internal(node.left(&self));
                vec.append(&mut left_tree_list);
                vec.push(key);
                let mut right_tree_list = self.to_inorder_list_internal(node.right(&self));
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
            tree.print_tree();

            assert_eq!(tree.to_inorder_list(), Vec::from([0, 1, 2, 3]));
        }
    }

    #[test]
    fn test() {
        let first = Rc::new(12);
        let second = Rc::new(13);
    }
}
