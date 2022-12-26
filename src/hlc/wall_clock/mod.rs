pub mod manual;
mod wallclock_ms;

pub trait WallClock {
    type Time: Ord + Copy;

    fn current_time(&self) -> Self::Time;
}
