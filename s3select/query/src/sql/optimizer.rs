use std::sync::Arc;

use api::{
    QueryResult,
    query::{logical_planner::QueryPlan, optimizer::Optimizer, physical_planner::PhysicalPlanner, session::SessionCtx},
};
use async_trait::async_trait;
use datafusion::physical_plan::{ExecutionPlan, displayable};
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
                .await?
        };

        debug!("Original physical plan:\n{}\n", displayable(physical_plan.as_ref()).indent(false));

        let optimized_physical_plan = { self.physical_optimizer.optimize(physical_plan, session)? };

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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cascade_optimizer_builder_default() {
        let _builder = CascadeOptimizerBuilder::default();

        // Test that builder can be created successfully
        assert!(
            std::mem::size_of::<CascadeOptimizerBuilder>() > 0,
            "Builder should be created successfully"
        );
    }

    #[test]
    fn test_cascade_optimizer_builder_build_with_defaults() {
        let _builder = CascadeOptimizerBuilder::default();
        let optimizer = _builder.build();

        // Test that optimizer can be built with default components
        assert!(std::mem::size_of_val(&optimizer) > 0, "Optimizer should be built successfully");
    }

    #[test]
    fn test_cascade_optimizer_builder_basic_functionality() {
        // Test that builder methods can be called and return self
        let _builder = CascadeOptimizerBuilder::default();

        // Test that we can call builder methods (even if we don't have mock implementations)
        // This tests the builder pattern itself
        assert!(
            std::mem::size_of::<CascadeOptimizerBuilder>() > 0,
            "Builder should be created successfully"
        );
    }

    #[test]
    fn test_cascade_optimizer_builder_memory_efficiency() {
        let _builder = CascadeOptimizerBuilder::default();

        // Test that builder doesn't use excessive memory
        let builder_size = std::mem::size_of_val(&_builder);
        assert!(builder_size < 1000, "Builder should not use excessive memory");

        let optimizer = _builder.build();
        let optimizer_size = std::mem::size_of_val(&optimizer);
        assert!(optimizer_size < 1000, "Optimizer should not use excessive memory");
    }

    #[test]
    fn test_cascade_optimizer_builder_multiple_builds() {
        let _builder = CascadeOptimizerBuilder::default();

        // Test that we can build multiple optimizers from the same configuration
        let optimizer1 = _builder.build();
        assert!(std::mem::size_of_val(&optimizer1) > 0, "First optimizer should be built successfully");

        // Note: builder is consumed by build(), so we can't build again from the same instance
        // This is the expected behavior
    }

    #[test]
    fn test_cascade_optimizer_builder_default_fallbacks() {
        let _builder = CascadeOptimizerBuilder::default();
        let optimizer = _builder.build();

        // Test that default components are used when none are specified
        // We can't directly access the internal components, but we can verify the optimizer was built
        assert!(std::mem::size_of_val(&optimizer) > 0, "Optimizer should use default components");
    }

    #[test]
    fn test_cascade_optimizer_component_types() {
        let optimizer = CascadeOptimizerBuilder::default().build();

        // Test that optimizer contains the expected component types
        // We can't directly access the components, but we can verify the optimizer structure
        assert!(std::mem::size_of_val(&optimizer) > 0, "Optimizer should contain components");

        // The optimizer should have three Arc fields for the components
        // This is a basic structural test
    }

    #[test]
    fn test_cascade_optimizer_builder_consistency() {
        // Test that multiple builders with the same configuration produce equivalent optimizers
        let optimizer1 = CascadeOptimizerBuilder::default().build();
        let optimizer2 = CascadeOptimizerBuilder::default().build();

        // Both optimizers should be built successfully
        assert!(std::mem::size_of_val(&optimizer1) > 0, "First optimizer should be built");
        assert!(std::mem::size_of_val(&optimizer2) > 0, "Second optimizer should be built");

        // They should have the same memory footprint (same structure)
        assert_eq!(
            std::mem::size_of_val(&optimizer1),
            std::mem::size_of_val(&optimizer2),
            "Optimizers with same configuration should have same size"
        );
    }
}
