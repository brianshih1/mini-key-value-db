pub mod manual;
mod wallclock_ms;

pub trait WallClock {
    type Time: Ord;

    fn current_time(&mut self) -> Self::Time;
}
