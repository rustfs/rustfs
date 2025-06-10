use std::sync::Arc;

use async_trait::async_trait;

use crate::QueryResult;

use super::{
    Query,
    execution::{Output, QueryStateMachine},
    logical_planner::Plan,
};

#[async_trait]
pub trait QueryDispatcher: Send + Sync {
    // fn create_query_id(&self) -> QueryId;

    // fn query_info(&self, id: &QueryId);

    async fn execute_query(&self, query: &Query) -> QueryResult<Output>;

    async fn build_logical_plan(&self, query_state_machine: Arc<QueryStateMachine>) -> QueryResult<Option<Plan>>;

    async fn execute_logical_plan(&self, logical_plan: Plan, query_state_machine: Arc<QueryStateMachine>) -> QueryResult<Output>;

    async fn build_query_state_machine(&self, query: Query) -> QueryResult<Arc<QueryStateMachine>>;

    // fn running_query_infos(&self) -> Vec<QueryInfo>;

    // fn running_query_status(&self) -> Vec<QueryStatus>;

    // fn cancel_query(&self, id: &QueryId);
}
