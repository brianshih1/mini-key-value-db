use super::WallClock;

pub struct Manual {
    manual_time: u64,
}

impl Manual {
    pub fn new(time: u64) -> Self {
        Manual { manual_time: time }
    }
}

impl WallClock for Manual {
    fn current_time(&self) -> u64 {
        self.manual_time
    }
}
