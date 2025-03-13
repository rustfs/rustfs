use std::sync::Arc;

use api::{
    query::{logical_planner::QueryPlan, optimizer::Optimizer, physical_planner::PhysicalPlanner, session::SessionCtx},
    QueryResult,
};
use async_trait::async_trait;
use datafusion::physical_plan::{displayable, ExecutionPlan};
use tracing::debug;

use super::{
    logical::optimizer::{DefaultLogicalOptimizer, LogicalOptimizer},
    physical::{optimizer::PhysicalOptimizer, planner::DefaultPhysicalPlanner},
};

pub struct CascadeOptimizer {
    logical_optimizer: Arc<dyn LogicalOptimizer + Send + Sync>,
    physical_planner: Arc<dyn PhysicalPlanner + Send + Sync>,
    physical_optimizer: Arc<dyn PhysicalOptimizer + Send + Sync>,
}

#[async_trait]
impl Optimizer for CascadeOptimizer {
    async fn optimize(&self, plan: &QueryPlan, session: &SessionCtx) -> QueryResult<Arc<dyn ExecutionPlan>> {
        debug!("Original logical plan:\n{}\n", plan.df_plan.display_indent_schema(),);

        let optimized_logical_plan = self.logical_optimizer.optimize(plan, session)?;

        debug!("Final logical plan:\n{}\n", optimized_logical_plan.display_indent_schema(),);

        let physical_plan = {
            self.physical_planner
                .create_physical_plan(&optimized_logical_plan, session)
                .await
                .map(|p| p)
                .map_err(|err| err)?
        };

        debug!("Original physical plan:\n{}\n", displayable(physical_plan.as_ref()).indent(false));

        let optimized_physical_plan = {
            self.physical_optimizer
                .optimize(physical_plan, session)
                .map(|p| p)
                .map_err(|err| err)?
        };

        Ok(optimized_physical_plan)
    }
}

#[derive(Default)]
pub struct CascadeOptimizerBuilder {
    logical_optimizer: Option<Arc<dyn LogicalOptimizer + Send + Sync>>,
    physical_planner: Option<Arc<dyn PhysicalPlanner + Send + Sync>>,
    physical_optimizer: Option<Arc<dyn PhysicalOptimizer + Send + Sync>>,
}

impl CascadeOptimizerBuilder {
    pub fn with_logical_optimizer(mut self, logical_optimizer: Arc<dyn LogicalOptimizer + Send + Sync>) -> Self {
        self.logical_optimizer = Some(logical_optimizer);
        self
    }

    pub fn with_physical_planner(mut self, physical_planner: Arc<dyn PhysicalPlanner + Send + Sync>) -> Self {
        self.physical_planner = Some(physical_planner);
        self
    }

    pub fn with_physical_optimizer(mut self, physical_optimizer: Arc<dyn PhysicalOptimizer + Send + Sync>) -> Self {
        self.physical_optimizer = Some(physical_optimizer);
        self
    }

    pub fn build(self) -> CascadeOptimizer {
        let default_logical_optimizer = Arc::new(DefaultLogicalOptimizer::default());
        let default_physical_planner = Arc::new(DefaultPhysicalPlanner::default());

        let logical_optimizer = self.logical_optimizer.unwrap_or(default_logical_optimizer);
        let physical_planner = self.physical_planner.unwrap_or_else(|| default_physical_planner.clone());
        let physical_optimizer = self.physical_optimizer.unwrap_or(default_physical_planner);

        CascadeOptimizer {
            logical_optimizer,
            physical_planner,
            physical_optimizer,
        }
    }
}
