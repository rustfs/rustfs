use std::sync::Arc;

use async_trait::async_trait;
use datafusion::physical_plan::ExecutionPlan;

use super::logical_planner::QueryPlan;
use super::session::SessionCtx;
use crate::QueryResult;

pub type OptimizerRef = Arc<dyn Optimizer + Send + Sync>;

#[async_trait]
pub trait Optimizer {
    async fn optimize(&self, plan: &QueryPlan, session: &SessionCtx) -> QueryResult<Arc<dyn ExecutionPlan>>;
}
