pub mod manual;
mod wallclock_ms;

pub trait ClockSource {
    type Time: Ord;

    fn now(&mut self) -> Result<Self::Time, ()>;
}
