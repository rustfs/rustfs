use std::sync::Arc;

use api::query::execution::{Output, QueryExecution, QueryStateMachineRef};
use api::query::logical_planner::QueryPlan;
use api::query::optimizer::Optimizer;
use api::query::scheduler::SchedulerRef;
use api::{QueryError, QueryResult};
use async_trait::async_trait;
use futures::stream::AbortHandle;
use parking_lot::Mutex;
use tracing::debug;

pub struct SqlQueryExecution {
    query_state_machine: QueryStateMachineRef,
    plan: QueryPlan,
    optimizer: Arc<dyn Optimizer + Send + Sync>,
    scheduler: SchedulerRef,

    abort_handle: Mutex<Option<AbortHandle>>,
}

impl SqlQueryExecution {
    pub fn new(
        query_state_machine: QueryStateMachineRef,
        plan: QueryPlan,
        optimizer: Arc<dyn Optimizer + Send + Sync>,
        scheduler: SchedulerRef,
    ) -> Self {
        Self {
            query_state_machine,
            plan,
            optimizer,
            scheduler,
            abort_handle: Mutex::new(None),
        }
    }

    async fn start(&self) -> QueryResult<Output> {
        // begin optimize
        self.query_state_machine.begin_optimize();
        let physical_plan = self.optimizer.optimize(&self.plan, &self.query_state_machine.session).await?;
        self.query_state_machine.end_optimize();

        // begin schedule
        self.query_state_machine.begin_schedule();
        let stream = self
            .scheduler
            .schedule(physical_plan.clone(), self.query_state_machine.session.inner().task_ctx())
            .await?
            .stream();

        debug!("Success build result stream.");
        self.query_state_machine.end_schedule();

        Ok(Output::StreamData(stream))
    }
}

#[async_trait]
impl QueryExecution for SqlQueryExecution {
    async fn start(&self) -> QueryResult<Output> {
        let (task, abort_handle) = futures::future::abortable(self.start());

        {
            *self.abort_handle.lock() = Some(abort_handle);
        }

        task.await.map_err(|_| QueryError::Cancel)?
    }

    fn cancel(&self) -> QueryResult<()> {
        debug!(
            "cancel sql query execution: sql: {}, state: {:?}",
            self.query_state_machine.query.content(),
            self.query_state_machine.state()
        );

        // change state
        self.query_state_machine.cancel();
        // stop future task
        if let Some(e) = self.abort_handle.lock().as_ref() {
            e.abort()
        };

        debug!(
            "canceled sql query execution: sql: {}, state: {:?}",
            self.query_state_machine.query.content(),
            self.query_state_machine.state()
        );
        Ok(())
    }
}
