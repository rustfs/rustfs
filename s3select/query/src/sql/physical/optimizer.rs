use std::sync::Arc;

use api::query::session::SessionCtx;
use api::QueryResult;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::ExecutionPlan;

pub trait PhysicalOptimizer {
    fn optimize(&self, plan: Arc<dyn ExecutionPlan>, session: &SessionCtx) -> QueryResult<Arc<dyn ExecutionPlan>>;

    fn inject_optimizer_rule(&mut self, optimizer_rule: Arc<dyn PhysicalOptimizerRule + Send + Sync>);
}
