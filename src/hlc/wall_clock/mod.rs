pub mod manual;
mod wallclock_ms;

pub trait WallClock {
    fn current_time(&self) -> u64;
}
