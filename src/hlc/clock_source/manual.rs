use super::ClockSource;

pub struct Manual {
    manual_time: u32,
}

impl ClockSource for Manual {
    type Time = u32;

    fn now(&mut self) -> Result<Self::Time, ()> {
        Ok(self.manual_time)
    }
}
