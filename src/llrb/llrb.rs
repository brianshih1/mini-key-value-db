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
    pub fn new(start_key: K, end_key: K, value: V, color: TreeColor) -> Self {
        Node {
            start_key: start_key,
            end_key: end_key,
            value,
            left_node: NIL,
            right_node: NIL,
            parent_node: NIL,
            color,
        }
    }

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

    fn get_parent<'a>(&self, tree: &'a Tree<K, V>) -> Option<&'a Node<K, V>> {
        if self.parent_node == NIL {
            None
        } else {
            Some(&tree.nodes[self.parent_node])
        }
    }

    fn get_mut_parent<'a>(&self, tree: &'a Tree<K, V>) -> Option<&'a Node<K, V>> {
        if self.parent_node == NIL {
            None
        } else {
            Some(&tree.nodes[self.parent_node])
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

    pub fn set_left(&mut self, parent: usize, new_left: usize) {
        if parent == NIL {
            panic!("cannot be NIL");
        }
        if new_left == NIL {
            self.nodes[parent].left_node = NIL;
        }
        let original_new_node_parent = self.nodes[new_left].parent_node;
        if original_new_node_parent != NIL {
            if self.nodes[original_new_node_parent].left_node == new_left {
                self.nodes[original_new_node_parent].left_node = NIL;
            } else {
                self.nodes[original_new_node_parent].right_node = NIL;
            }
        }
        let original_left = self.nodes[parent].left_node;
        if original_left != NIL {
            self.nodes[original_left].parent_node = NIL;
        }
        self.nodes[parent].left_node = new_left;
        self.nodes[new_left].parent_node = parent;
    }

    pub fn set_right(&mut self, parent: usize, new_right: usize) {
        if parent == NIL {
            panic!("cannot be NIL");
        }
        if new_right == NIL {
            self.nodes[parent].right_node = NIL;
            return;
        }
        let original_new_node_parent = self.nodes[new_right].parent_node;
        if original_new_node_parent != NIL {
            if self.nodes[original_new_node_parent].left_node == new_right {
                self.nodes[original_new_node_parent].left_node = NIL;
            } else {
                self.nodes[original_new_node_parent].right_node = NIL;
            }
        }
        let original_right = self.nodes[parent].right_node;
        if original_right != NIL {
            self.nodes[original_right].parent_node = NIL;
        }
        self.nodes[parent].right_node = new_right;
        self.nodes[new_right].parent_node = parent;
    }

    // returns the idx of the added node
    fn add_node(&mut self, node: Node<K, V>) -> usize {
        self.nodes.push(node);
        let idx = self.nodes.len() - 1;
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
            let added = self.add_node(Node::new_node(
                TreeColor::RED,
                start_key,
                end_key,
                value,
                NIL,
            ));
            self.root = added;
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

    pub fn get_parent_with_idx(&self, k: usize) -> &Node<K, V> {
        let node = self.get_node(k);
        self.get_node(node.parent_node)
    }

    pub fn right_rotate(&mut self, k: usize) {}

    pub fn left_rotate(&mut self, k: usize) {
        let right = self.nodes[k].right_node;
        let right_left = self.nodes[right].left_node;
        let parent = self.nodes[k].parent_node;

        if parent == NIL {
            self.root = right;
            self.nodes[right].parent_node = NIL;
        } else {
            // is k the right of its parent
            let is_k_right = self.nodes[parent].right_node == k;
            if is_k_right {
                self.set_right(parent, right);
            } else {
                self.set_left(parent, right);
            }
        }

        self.set_left(right, k);
        self.set_right(k, right_left);
    }

    pub fn fix_insert(&mut self, k: usize) {
        let mut k = k;
        while self.nodes[k].color == TreeColor::RED {
            let k_parent = self.nodes[k].parent_node;
            // grand parent is guaranteed to not be NIL because of the while check^
            let grand_parent = self.nodes[k_parent].parent_node;
            // if (k.parent == k.parent.parent.right)
            if k_parent == self.nodes[grand_parent].right_node {
                let uncle = self.nodes[grand_parent].left_node;
                match self.nodes[uncle].color {
                    TreeColor::RED => {
                        // case 3.1
                        self.nodes[uncle].color = TreeColor::BLACK;
                        self.nodes[k_parent].color = TreeColor::BLACK;
                        self.nodes[grand_parent].color = TreeColor::RED;
                        k = grand_parent;
                    }
                    TreeColor::BLACK => {
                        if k == self.nodes[k_parent].left_node {
                            // case 3.2.2
                            k = k_parent;
                            self.right_rotate(k);
                        }
                        self.nodes[k_parent].color = TreeColor::BLACK;
                        self.nodes[grand_parent].color = TreeColor::RED;
                        self.left_rotate(k);
                    }
                }
            } else {
                let uncle = self.nodes[k_parent].right_node;
                match self.nodes[uncle].color {
                    TreeColor::RED => {
                        // case 3.1 (mirror)
                        self.nodes[uncle].color = TreeColor::BLACK;
                        self.nodes[k_parent].color = TreeColor::BLACK;
                        self.nodes[grand_parent].color = TreeColor::RED;
                        k = grand_parent;
                    }
                    TreeColor::BLACK => {
                        if k == self.nodes[k_parent].right_node {
                            // 3.2.2 (mirror)
                            k = k_parent;
                            self.left_rotate(k);
                        }
                        // 3.2.1 (mirror)
                        self.nodes[k_parent].color = TreeColor::BLACK;
                        self.nodes[grand_parent].color = TreeColor::RED;
                        self.right_rotate(grand_parent);
                    }
                }
            }
            if self.nodes[k].parent_node == NIL {
                break;
            }
        }
        self.nodes[0].color = TreeColor::BLACK;
    }

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
        self.print_tree_internal(self.root, 0, None);
    }

    pub fn stringify_idx(idx: usize) -> String {
        if idx == NIL {
            "NIL".to_owned()
        } else {
            idx.to_string()
        }
    }

    pub fn print_nodes(&self) {
        for (pos, node) in self.nodes.iter().enumerate() {
            println!(
                "Index {}. Keys: ({}, {}). Parent: {}. Left: {}, Right: {}",
                pos,
                node.start_key,
                node.end_key,
                Self::stringify_idx(node.parent_node),
                Self::stringify_idx(node.left_node),
                Self::stringify_idx(node.right_node)
            )
        }
    }

    pub fn print_tree_internal(&self, node_idx: usize, depth: usize, is_left: Option<bool>) {
        let indent = Self::get_indent(depth * 3);
        if node_idx == NIL {
            println!("{}NIL", &indent)
        } else {
            let node = &self.nodes[node_idx];
            println!(
                "{} - {}{}({}, {}), parent: {}, index: {node_idx}",
                &indent,
                match is_left {
                    Some(is_left) => {
                        if is_left {
                            "Left: ".to_owned()
                        } else {
                            "Right: ".to_owned()
                        }
                    }
                    None => "".to_owned(),
                },
                node.color,
                node.start_key,
                node.end_key,
                if node.parent_node == NIL {
                    "NIL".to_owned()
                } else {
                    node.parent_node.to_string()
                }
            );
            self.print_tree_internal(node.left_node, depth + 1, Some(true));
            self.print_tree_internal(node.right_node, depth + 1, Some(false));
        }
    }

    pub fn get_indent(depth: usize) -> String {
        " ".repeat(depth)
    }

    pub fn to_preorder_keys(&self) -> Vec<K> {
        self.preorder_keys_internal(self.get_root())
    }

    pub fn preorder_keys_internal(&self, link: Option<&Node<K, V>>) -> Vec<K> {
        let mut vec = Vec::new();
        match &link {
            Some(node) => {
                let key = node.start_key;
                vec.push(key);
                let mut left_tree_list = self.preorder_keys_internal(node.left(&self));
                vec.append(&mut left_tree_list);
                let mut right_tree_list = self.preorder_keys_internal(node.right(&self));
                vec.append(&mut right_tree_list);
            }
            None => {}
        };
        vec
    }

    // turns the tree into a list. This is for testing purposes
    // to assert the sorted list against expected list
    // Note: checking inorder + preorder is enough to verify if two trees are structurally the same
    pub fn to_inorder_keys(&self) -> Vec<K> {
        self.inorder_keys_internal(self.get_root())
    }

    pub fn inorder_keys_internal(&self, link: Option<&Node<K, V>>) -> Vec<K> {
        let mut vec = Vec::new();
        match &link {
            Some(node) => {
                let key = node.start_key;
                let mut left_tree_list = self.inorder_keys_internal(node.left(&self));
                vec.append(&mut left_tree_list);
                vec.push(key);
                let mut right_tree_list = self.inorder_keys_internal(node.right(&self));
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
            assert_eq!(tree.to_inorder_keys(), Vec::from([2]));
        }

        #[test]
        fn insert_a_few_elements() {
            let mut tree = Tree::<i32, i32>::new();
            tree.insert_node(2, 3, 1);
            tree.insert_node(1, 3, 1);
            tree.insert_node(3, 3, 1);
            tree.insert_node(0, 3, 1);
            tree.print_tree();

            assert_eq!(tree.to_inorder_keys(), Vec::from([0, 1, 2, 3]));
        }
    }

    mod left_rotate {
        use crate::llrb::llrb::{Node, Tree, TreeColor};

        #[test]
        fn left_rotate_on_root() {
            let mut tree = Tree::<i32, i32>::new();
            let left = tree.add_node(Node::new(0, 0, 3, TreeColor::BLACK));

            let mid = tree.add_node(Node::new(1, 0, 3, TreeColor::BLACK));
            let right = tree.add_node(Node::new(2, 0, 3, TreeColor::BLACK));

            tree.root = mid;
            tree.set_left(mid, left);
            tree.set_right(mid, right);
            tree.left_rotate(mid);
            assert_eq!(tree.to_inorder_keys(), Vec::from([0, 1, 2]));
            assert_eq!(tree.to_preorder_keys(), Vec::from([2, 0, 1]));
        }

        /**
         * Original:
         * 3
         *  - 2
         *  - 6
         *      - 5
         *      - 8
         *          - 7
         *          - 9
         *
         * Perform left rotation on node 6
         *
         * After
         * 3
         *   - 2
         *   - 8
         *      - 6
         *         - 5
         *         - 7
         *      - 9
         */
        #[test]
        fn left_rotate_on_non_root() {
            let mut tree = Tree::<i32, i32>::new();
            let two = tree.add_node(Node::new(2, 0, 3, TreeColor::BLACK));
            let three = tree.add_node(Node::new(3, 0, 3, TreeColor::BLACK));
            let five = tree.add_node(Node::new(5, 0, 3, TreeColor::BLACK));
            let six = tree.add_node(Node::new(6, 0, 3, TreeColor::BLACK));
            let seven = tree.add_node(Node::new(7, 0, 3, TreeColor::BLACK));
            let eight = tree.add_node(Node::new(8, 0, 3, TreeColor::BLACK));
            let nine = tree.add_node(Node::new(9, 0, 3, TreeColor::BLACK));

            tree.root = three;
            tree.set_left(three, two);
            tree.set_right(three, six);
            tree.set_left(six, five);
            tree.set_right(six, eight);
            tree.set_left(eight, seven);
            tree.set_right(eight, nine);
            tree.print_tree();

            tree.left_rotate(six);
            assert_eq!(tree.to_inorder_keys(), Vec::from([2, 3, 5, 6, 7, 8, 9]));
            assert_eq!(tree.to_preorder_keys(), Vec::from([3, 2, 8, 6, 5, 7, 9]));
        }
    }

    #[test]
    fn test() {
        let first = Rc::new(12);
        let second = Rc::new(13);
    }
}
