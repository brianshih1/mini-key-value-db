struct Foo {}

pub trait NodeKey: std::fmt::Debug + Clone + Eq + PartialOrd + Ord {}

type NodeLink<K: NodeKey> = Option<Box<Node<K>>>;
enum Node<K: NodeKey> {
    Internal(InternalNode<K>),
    Leaf(LeafNode<K>),
}

struct InternalNode<K: NodeKey> {
    keys: Vec<K>,
    edges: Vec<NodeLink<K>>,
    order: u16,
    upper: Option<K>,
    lower: Option<K>,
}

struct LeafNode<K: NodeKey> {
    start_keys: Vec<K>,
    end_keys: Vec<K>,
    left_sibling: NodeLink<K>,
    right_sibling: NodeLink<K>,
}

impl<K: NodeKey> InternalNode<K> {}

impl<K: NodeKey> LeafNode<K> {}

struct BTree<K: NodeKey> {
    root: Node<K>,
    order: usize,
}

struct Range<K: NodeKey> {
    start_key: K,
    end_key: K,
}

impl<K: NodeKey> BTree<K> {
    pub fn search() -> () {}

    pub fn new(capacity: usize) -> Self {
        BTree {
            root: (),
            order: (),
        }
    }

    pub fn insert(&self, range: Range<K>) -> () {}
}
