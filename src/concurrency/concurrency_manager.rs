pub struct ConcurrencyManager {}

pub struct Guard {}

impl ConcurrencyManager {
    pub fn sequence_req(&self) -> Guard {
        todo!()
    }

    pub fn release_guard(&self, guard: Guard) -> () {}
}
