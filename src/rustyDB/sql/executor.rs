use crate::rustyDB::storage::storage::Storage;

use super::plan::{CreateTablePlan, Plan};

pub struct Executor<'a> {
    storage: &'a mut Storage,
    plan: Plan,
}

impl<'a> Executor<'a> {
    pub fn new(storage: &'a mut Storage, plan: Plan) -> Self {
        Self { storage, plan }
    }

    fn execute_plan(&mut self, plan: Plan) {
        match plan {
            Plan::CreateTablePlan(plan) => self.create_table(plan),
        }
    }

    fn create_table(&mut self, plan: CreateTablePlan) {}
}
