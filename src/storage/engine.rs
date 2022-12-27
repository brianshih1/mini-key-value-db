pub trait MVCCIterator {
    /**
     * SeekGE advances the iterator to the first key which is >= the provided key
     */
    fn seek_ge() -> ();

    /**
     * Valid must be called after Seek(), Next(), Prev(), etc.
     * It returns true if the iterator points to a valid key.
     * It returns false if the iterator has mvoed past the end of the valid
     * range or if there's an error.
     */
    fn valid() -> bool;

    /**
     * Next advances the iterator to the next key in the iteration.
     */
    fn next() -> ();
}

trait Reader {}

trait Writer {
    fn clear_intent() -> ();

    fn put_mvcc() -> ();

    fn put_intent() -> ();
}
