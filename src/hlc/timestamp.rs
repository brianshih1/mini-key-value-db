#[derive(Debug, Eq, PartialOrd, Ord, Clone, Copy)]
pub struct Timestamp<T: Clone> {
    pub wall_time: T,
    pub logical_time: u32,
}

impl<T: Clone> PartialEq for Timestamp<T>
where
    T: PartialEq,
{
    fn eq(&self, other: &Self) -> bool {
        self.logical_time == other.logical_time && self.wall_time == other.wall_time
    }
}
