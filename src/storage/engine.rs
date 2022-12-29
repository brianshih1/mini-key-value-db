use super::data::MVCCKey;

pub enum MVCCIterKind {
    /**
     *
     */
    Intents,
    NoIntents,
}

pub trait MVCCIterator {
    /**
     * SeekGE advances the iterator to the first key which is >= the provided key
     */
    fn seek_ge(&mut self, key: MVCCKey) -> ();

    /**
     * Valid must be called after Seek(), Next(), Prev(), etc.
     * It returns true if the iterator points to a valid key.
     * It returns false if the iterator has mvoed past the end of the valid
     * range or if there's an error.
     */
    fn valid(&mut self) -> bool;

    /**
     * Next advances the iterator to the next key in the iteration.
     */
    fn next(&mut self) -> ();
}

trait Reader {}

trait Writer {
    fn clear_intent(&mut self) -> ();

    fn put_mvcc(&mut self) -> ();

    fn put_intent(&mut self) -> ();
}

pub struct IterOptions {}
