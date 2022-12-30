use super::engine::Writer;

struct MVCCWriter {}

impl Writer for MVCCWriter {
    fn clear_intent(&mut self) -> () {
        todo!()
    }

    fn put_mvcc(&mut self) -> () {
        todo!()
    }

    fn put_intent(&mut self) -> () {
        todo!()
    }

    fn put_engine_key(&mut self) -> () {
        todo!()
    }
}
