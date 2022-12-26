use super::WallClock;

pub struct Manual {
    manual_time: u32,
}

impl WallClock for Manual {
    type Time = u32;

    fn current_time(&mut self) -> Self::Time {
        self.manual_time
    }
}
