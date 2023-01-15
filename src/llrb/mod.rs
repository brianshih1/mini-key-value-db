mod llrb;

type Link = Option<Box<Node>>;
struct Node {
    left: Link,
}
trait Helper {
    fn left(&self) -> &Link;
}

impl Helper for Link {
    fn left(&self) -> &Link {
        &self.as_ref().unwrap().left
    }
}
