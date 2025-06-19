use std::sync::Arc;

use api::{
    QueryError,
    query::{
        execution::{QueryExecutionFactory, QueryExecutionRef, QueryStateMachineRef},
        logical_planner::Plan,
        optimizer::Optimizer,
        scheduler::SchedulerRef,
    },
};
use async_trait::async_trait;

use super::query::SqlQueryExecution;

pub type QueryExecutionFactoryRef = Arc<dyn QueryExecutionFactory + Send + Sync>;

pub struct SqlQueryExecutionFactory {
    optimizer: Arc<dyn Optimizer + Send + Sync>,
    scheduler: SchedulerRef,
}

impl SqlQueryExecutionFactory {
    #[inline(always)]
    pub fn new(optimizer: Arc<dyn Optimizer + Send + Sync>, scheduler: SchedulerRef) -> Self {
        Self { optimizer, scheduler }
    }
}

#[async_trait]
impl QueryExecutionFactory for SqlQueryExecutionFactory {
    async fn create_query_execution(
        &self,
        plan: Plan,
        state_machine: QueryStateMachineRef,
    ) -> Result<QueryExecutionRef, QueryError> {
        match plan {
            Plan::Query(query_plan) => Ok(Arc::new(SqlQueryExecution::new(
                state_machine,
                query_plan,
                self.optimizer.clone(),
                self.scheduler.clone(),
            ))),
        }
    }
}
