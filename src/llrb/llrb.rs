use std::{
    borrow::{Borrow, BorrowMut},
    cell::{Ref, RefCell, RefMut},
    cmp::Ordering,
    fmt,
    rc::Rc,
};

trait NodeKey: Copy + Eq + PartialOrd + Ord + std::fmt::Display {}

struct Tree<K: NodeKey, V> {
    root: NodeLink<K, V>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
enum TreeColor {
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

type NodeRc<K: NodeKey, V> = Rc<RefCell<Node<K, V>>>;
type NodeLink<K: NodeKey, V> = Option<NodeRc<K, V>>;

#[derive(Debug, Clone, PartialEq, Eq)]
struct Node<K: NodeKey, V> {
    start_key: K,
    end_key: K,
    value: V,
    left_node: NodeLink<K, V>,
    right_node: NodeLink<K, V>,
    parent_node: NodeLink<K, V>,
    color: TreeColor,
}

impl<K: NodeKey, V> fmt::Display for Node<K, V> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "({}, {})", self.start_key, self.end_key)
    }
}

impl<K: NodeKey, V> Node<K, V> {
    fn new_node(
        color: TreeColor,
        start_key: K,
        end_key: K,
        value: V,
        parent_node: NodeLink<K, V>,
    ) -> Self {
        Node {
            start_key: start_key,
            end_key: end_key,
            left_node: None,
            right_node: None,
            value: value,
            parent_node: parent_node,
            color,
        }
    }

    pub fn to_list(node: NodeLink<K, V>) -> Vec<NodeRc<K, V>> {
        let mut vec = Vec::new();
        match node {
            Some(node_rc) => {
                vec.push(Rc::clone(&node_rc));
            }
            None => {}
        };
        vec
    }
}

impl<K: NodeKey, V> Tree<K, V> {
    fn new_with_node(root_node: Node<K, V>) -> Self {
        Tree {
            root: Some(Rc::new(RefCell::new(root_node))),
        }
    }

    fn new(root_node: Node<K, V>) -> Self {
        Tree { root: None }
    }

    fn get_node_link(link: &NodeLink<K, V>) -> NodeLink<K, V> {
        match link {
            Some(node_rc) => Some(Rc::clone(&node_rc)),
            None => None,
        }
    }

    fn insert_node(&mut self, start_key: K, end_key: K, value: V) -> () {
        let mut cur = Self::get_node_link(&self.root);
        let mut prev: NodeLink<K, V> = None;

        while cur.is_some() {
            let node_box = cur.unwrap();
            prev = Some(Rc::clone(&node_box));
            if self.root.is_none() {
                self.root = Some(Rc::clone(&node_box));
            }
            let node = node_box.as_ref().borrow();
            let ord = node.start_key.cmp(&start_key);
            match ord {
                Ordering::Less => {
                    cur = Self::get_node_link(&node.left_node);
                }
                Ordering::Equal => {
                    // TODO: If the interval comes from the same ID, just update end.
                    // Otherwise, if ID is less, go left, else go right. For now, we will just always
                    // make it go left
                    cur = Self::get_node_link(&node.left_node);
                }
                Ordering::Greater => {
                    cur = Self::get_node_link(&node.right_node);
                }
            }
        }
        // new nodes are red
        let mut node = Node::new_node(
            TreeColor::RED,
            start_key,
            end_key,
            value,
            Self::get_node_link(&prev),
        );

        match prev {
            Some(node_rc) => {
                let mut node_ref = node_rc.as_ref().borrow_mut();
                node_ref.parent_node = Some(Rc::clone(&node_rc));
                let ord = node_ref.start_key.cmp(&start_key);
                match ord {
                    Ordering::Less => {
                        node_ref.left_node = Some(Rc::new(RefCell::new(node)));
                    }
                    Ordering::Equal => todo!(),
                    Ordering::Greater => {
                        node_ref.right_node = Some(Rc::new(RefCell::new(node)));
                    }
                }
                self.fix_insert(node_ref);
            }
            None => {
                node.color = TreeColor::BLACK;
                self.root = Some(Rc::new(RefCell::new(node)));
                return;
            }
        };
    }

    pub fn fix_insert(&mut self, node: RefMut<Node<K, V>>) {}

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
        let mut node = Node::new_node(TreeColor::RED, 3, 3, 12, None);
        node.left_node = Some(Rc::new(RefCell::new(Node::new_node(
            TreeColor::RED,
            4,
            4,
            12,
            None,
        ))));
        node.right_node = Some(Rc::new(RefCell::new(Node::new_node(
            TreeColor::RED,
            5,
            4,
            12,
            None,
        ))));
        let tree = Tree::new_with_node(node);
        tree.print_tree();

        let boxed = Some(Rc::new(RefCell::new(Node::new_node(
            TreeColor::RED,
            4,
            4,
            12,
            None,
        ))));
        let test = Node::to_list(boxed);
        println!("List {:?}", test);
    }
}
