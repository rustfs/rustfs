use std::sync::Arc;

use api::{
    query::{
        dispatcher::QueryDispatcher, execution::QueryStateMachineRef, logical_planner::Plan, session::SessionCtxFactory, Query,
    },
    server::dbms::{DatabaseManagerSystem, QueryHandle},
    QueryResult,
};
use async_trait::async_trait;
use derive_builder::Builder;
use s3s::dto::SelectObjectContentInput;

use crate::{
    dispatcher::manager::SimpleQueryDispatcherBuilder,
    execution::{factory::SqlQueryExecutionFactory, scheduler::local::LocalScheduler},
    function::simple_func_manager::SimpleFunctionMetadataManager,
    metadata::base_table::BaseTableProvider,
    sql::{optimizer::CascadeOptimizerBuilder, parser::DefaultParser},
};

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
}

pub async fn make_cnosdbms(input: SelectObjectContentInput) -> QueryResult<impl DatabaseManagerSystem> {
    // init Function Manager, we can define some UDF if need
    let func_manager = SimpleFunctionMetadataManager::default();
    // TODO session config need load global system config
    let session_factory = Arc::new(SessionCtxFactory {});
    let parser = Arc::new(DefaultParser::default());
    let optimizer = Arc::new(CascadeOptimizerBuilder::default().build());
    // TODO wrap, and num_threads configurable
    let scheduler = Arc::new(LocalScheduler {});

    let query_execution_factory = Arc::new(SqlQueryExecutionFactory::new(optimizer, scheduler));

    let default_table_provider = Arc::new(BaseTableProvider::default());

    let query_dispatcher = SimpleQueryDispatcherBuilder::default()
        .with_input(input)
        .with_func_manager(Arc::new(func_manager))
        .with_default_table_provider(default_table_provider)
        .with_session_factory(session_factory)
        .with_parser(parser)
        .with_query_execution_factory(query_execution_factory)
        .build()?;

    let mut builder = RustFSmsBuilder::default();

    let db_server = builder.query_dispatcher(query_dispatcher).build().expect("build db server");

    Ok(db_server)
}
