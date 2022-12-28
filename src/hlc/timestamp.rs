#[derive(Debug, Eq, PartialOrd, Ord, Clone, Copy)]
pub struct Timestamp {
    pub wall_time: u64,
    pub logical_time: u32,
}

impl<T: Clone> PartialEq for Timestamp {
    fn eq(&self, other: &Self) -> bool {
        self.logical_time == other.logical_time && self.wall_time == other.wall_time
    }
}
