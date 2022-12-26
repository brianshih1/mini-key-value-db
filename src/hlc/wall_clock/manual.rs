use super::WallClock;

pub struct Manual {
    manual_time: u32,
}

impl Manual {
    pub fn new(time: u32) -> Self {
        Manual { manual_time: time }
    }
}

impl WallClock for Manual {
    type Time = u32;

    fn current_time(&self) -> Self::Time {
        self.manual_time
    }
}
