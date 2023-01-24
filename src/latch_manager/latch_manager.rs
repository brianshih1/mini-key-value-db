pub struct LatchGuard {}

pub trait LatchManager {
    fn acquire(&self) -> LatchGuard;

    fn release(&self, guard: LatchGuard) -> ();
}
