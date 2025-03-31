use async_trait::async_trait;

use crate::{
    query::{
        execution::{Output, QueryStateMachineRef},
        logical_planner::Plan,
        Query,
    },
    QueryResult,
};

pub struct QueryHandle {
    query: Query,
    result: Output,
}

impl QueryHandle {
    pub fn new(query: Query, result: Output) -> Self {
        Self { query, result }
    }

    pub fn query(&self) -> &Query {
        &self.query
    }

    pub fn result(self) -> Output {
        self.result
    }
}

#[async_trait]
pub trait DatabaseManagerSystem {
    async fn execute(&self, query: &Query) -> QueryResult<QueryHandle>;
    async fn build_query_state_machine(&self, query: Query) -> QueryResult<QueryStateMachineRef>;
    async fn build_logical_plan(&self, query_state_machine: QueryStateMachineRef) -> QueryResult<Option<Plan>>;
    async fn execute_logical_plan(
        &self,
        logical_plan: Plan,
        query_state_machine: QueryStateMachineRef,
    ) -> QueryResult<QueryHandle>;
}
