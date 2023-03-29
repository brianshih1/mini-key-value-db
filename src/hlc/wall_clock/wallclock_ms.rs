use std::time::SystemTime;

use super::WallClock;

pub struct SystemTimeClock {}

impl WallClock for SystemTimeClock {
    fn current_time(&self) -> u64 {
        let current = SystemTime::now();
        let since_epoch = current.duration_since(SystemTime::UNIX_EPOCH).unwrap();
        since_epoch.as_millis().try_into().unwrap()
    }
}

#[cfg(test)]
mod tests {
    use crate::hlc::wall_clock::WallClock;

    use super::SystemTimeClock;

    #[test]
    fn scratch() {
        let test_clock = SystemTimeClock {};
        println!("Time: {:?}", test_clock.current_time());
    }
}
