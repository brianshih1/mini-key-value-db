use std::fmt;

trait NodeKey: Copy + Eq + PartialOrd + Ord + std::fmt::Display {}

/**
 * Use start_key as BSG key.
 * Each node stores the max end_key contained in the subtree.
 */
enum TreeNode<K: NodeKey, V> {
    Node {
        start_key: K,
        end_key: K,
        left_node: Box<TreeNode<K, V>>,
        right_node: Box<TreeNode<K, V>>,
        value: V,
        parent_node: Box<TreeNode<K, V>>,
    },
    Nil,
}

struct Tree<K: NodeKey, V> {
    root: Option<Box<Node<K, V>>>,
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

#[derive(Debug, Clone, PartialEq, Eq)]
struct Node<K: NodeKey, V> {
    start_key: K,
    end_key: K,
    value: V,
    left_node: Option<Box<Node<K, V>>>,
    right_node: Option<Box<Node<K, V>>>,
    parent_node: Option<Box<Node<K, V>>>,
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
        parent_node: Option<Box<Node<K, V>>>,
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

    pub fn to_list<'a>(node: &'a Option<Box<Node<K, V>>>) -> Vec<&'a Node<K, V>> {
        let mut vec = Vec::new();
        match node {
            Some(boxed) => {
                let ele = boxed.as_ref();
                vec.push(ele);
                let mut left_list = Self::to_list(&ele.left_node);
                vec.append(&mut left_list);

                let mut right_list = Self::to_list(&ele.right_node);
                vec.append(&mut right_list);
            }
            None => {}
        };
        vec
    }
}

impl<K: NodeKey, V> Tree<K, V> {
    fn new_with_node(root_node: Node<K, V>) -> Self {
        Tree {
            root: Some(Box::new(root_node)),
        }
    }

    fn new(root_node: Node<K, V>) -> Self {
        Tree { root: None }
    }

    fn insert_node(&mut self, start_key: K, end_key: K, value: V) -> Self {
        let mut cur = self.root;
        let mut prev: &Option<Box<Node<K, V>>> = &None;

        while cur.is_some() {
            prev = &cur;
            if let Some(node_box) = cur {
                let node = node_box.as_ref();
                let ord = node.start_key.cmp(&start_key);
                match ord {
                    std::cmp::Ordering::Less => {
                        cur = node.left_node;
                    }
                    std::cmp::Ordering::Equal => {
                        // TODO: If the interval comes from the same ID, just update end.
                        // Otherwise, if ID is less, go left, else go right. For now, we will just always
                        // make it go left
                        cur = node.left_node;
                    }
                    std::cmp::Ordering::Greater => {
                        cur = node.right_node;
                    }
                }
            }
        }
        // new nodes are red
        let node = Node::new_node(TreeColor::RED, start_key, end_key, value, prev);
        let prev = prev.unwrap();
        prev.left_node = Some(Box::new(node));

        todo!()
    }

    // preorder print of the tree
    pub fn print_tree(&self) {
        self.print_tree_internal(&self.root, 0);
    }

    pub fn print_tree_internal(&self, node: &Option<Box<Node<K, V>>>, depth: usize) {
        let indent = Self::get_indent(depth * 3);
        match &node {
            Some(node) => {
                println!(
                    "{}{}({}, {})",
                    &indent, node.color, node.start_key, node.end_key
                );
                self.print_tree_internal(&node.as_ref().left_node, depth + 1);
                self.print_tree_internal(&node.as_ref().right_node, depth + 1);
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
    use crate::llrb::llrb::{Node, TreeColor};

    use super::Tree;

    #[test]
    fn test_print_tree() {
        let str = Tree::<i32, i32>::get_indent(3);
        println!("Test: {}foo", str.to_string());
        let mut node = Node::new_node(TreeColor::RED, 3, 3, 12, None);
        node.left_node = Some(Box::new(Node::new_node(TreeColor::RED, 4, 4, 12, None)));
        node.right_node = Some(Box::new(Node::new_node(TreeColor::RED, 5, 7, 12, None)));
        let tree = Tree::new_with_node(node.clone());
        tree.print_tree();

        let boxed = Some(Box::new(node));
        let test = Node::to_list(&boxed);
        println!("List {:?}", test);
    }
}
