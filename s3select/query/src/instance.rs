use std::sync::Arc;

use api::{
    query::{dispatcher::QueryDispatcher, execution::QueryStateMachineRef, logical_planner::Plan, Query},
    server::dbms::{DatabaseManagerSystem, QueryHandle},
    QueryResult,
};
use async_trait::async_trait;
use derive_builder::Builder;

#[derive(Builder)]
pub struct RustFSms<D: QueryDispatcher> {
    // query dispatcher & query execution
    query_dispatcher: Arc<D>,
}

#[async_trait]
impl<D> DatabaseManagerSystem for RustFSms<D>
where
    D: QueryDispatcher,
{
    async fn start(&self) -> QueryResult<()> {
        self.query_dispatcher.start().await
    }

    async fn execute(&self, query: &Query) -> QueryResult<QueryHandle> {
        let result = self.query_dispatcher.execute_query(query).await?;

        Ok(QueryHandle::new(query.clone(), result))
    }

    async fn build_query_state_machine(&self, query: Query) -> QueryResult<QueryStateMachineRef> {
        let query_state_machine = self.query_dispatcher.build_query_state_machine(query).await?;

        Ok(query_state_machine)
    }

    async fn build_logical_plan(&self, query_state_machine: QueryStateMachineRef) -> QueryResult<Option<Plan>> {
        let logical_plan = self.query_dispatcher.build_logical_plan(query_state_machine).await?;

        Ok(logical_plan)
    }

    async fn execute_logical_plan(
        &self,
        logical_plan: Plan,
        query_state_machine: QueryStateMachineRef,
    ) -> QueryResult<QueryHandle> {
        let query = query_state_machine.query.clone();
        let result = self
            .query_dispatcher
            .execute_logical_plan(logical_plan, query_state_machine)
            .await?;

        Ok(QueryHandle::new(query.clone(), result))
    }

    fn metrics(&self) -> String {
        let infos = self.query_dispatcher.running_query_infos();
        let status = self.query_dispatcher.running_query_status();

        format!(
            "infos: {}\nstatus: {}\n",
            infos.iter().map(|e| format!("{:?}", e)).collect::<Vec<_>>().join(","),
            status.iter().map(|e| format!("{:?}", e)).collect::<Vec<_>>().join(",")
        )
    }
}
