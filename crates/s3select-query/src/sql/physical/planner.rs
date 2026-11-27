// Copyright 2024 RustFS Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::sync::Arc;

use async_trait::async_trait;
use datafusion::common::config::ConfigOptions;
use datafusion::common::tree_node::{Transformed, TransformedResult as _, TreeNode as _};
use datafusion::error::Result as DFResult;
use datafusion::execution::SessionStateBuilder;
use datafusion::logical_expr::LogicalPlan;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_optimizer::aggregate_statistics::AggregateStatistics;
use datafusion::physical_optimizer::join_selection::JoinSelection;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::filter::FilterExec;
use datafusion::physical_planner::{
    DefaultPhysicalPlanner as DFDefaultPhysicalPlanner, ExtensionPlanner, PhysicalPlanner as DFPhysicalPlanner,
};
use rustfs_s3select_api::QueryResult;
use rustfs_s3select_api::query::physical_planner::PhysicalPlanner;
use rustfs_s3select_api::query::session::SessionCtx;

use super::optimizer::PhysicalOptimizer;

pub struct DefaultPhysicalPlanner {
    ext_physical_transform_rules: Vec<Arc<dyn ExtensionPlanner + Send + Sync>>,
    /// Responsible for optimizing a physical execution plan
    ext_physical_optimizer_rules: Vec<Arc<dyn PhysicalOptimizerRule + Send + Sync>>,
}

impl DefaultPhysicalPlanner {}

impl DefaultPhysicalPlanner {}

impl Default for DefaultPhysicalPlanner {
    fn default() -> Self {
        let ext_physical_transform_rules: Vec<Arc<dyn ExtensionPlanner + Send + Sync>> = vec![
            // can add rules at here
        ];

        // We need to take care of the rule ordering. They may influence each other.
        let ext_physical_optimizer_rules: Vec<Arc<dyn PhysicalOptimizerRule + Sync + Send>> = vec![
            Arc::new(AggregateStatistics::new()),
            // Statistics-based join selection will change the Auto mode to a real join implementation,
            // like collect left, or hash join, or future sort merge join, which will influence the
            // EnforceDistribution and EnforceSorting rules as they decide whether to add additional
            // repartitioning and local sorting steps to meet distribution and ordering requirements.
            // Therefore, it should run before EnforceDistribution and EnforceSorting.
            Arc::new(JoinSelection::new()),
            Arc::new(StreamFilterOutput),
        ];

        Self {
            ext_physical_transform_rules,
            ext_physical_optimizer_rules,
        }
    }
}

#[async_trait]
impl PhysicalPlanner for DefaultPhysicalPlanner {
    async fn create_physical_plan(
        &self,
        logical_plan: &LogicalPlan,
        session: &SessionCtx,
    ) -> QueryResult<Arc<dyn ExecutionPlan>> {
        // Inject extended physical plan optimization rules into df's session state
        let new_state = SessionStateBuilder::new_from_existing(session.inner().clone())
            .with_physical_optimizer_rules(self.ext_physical_optimizer_rules.clone())
            .build();

        // Construct df's Physical Planner with extended physical plan transformation rules
        let planner = DFDefaultPhysicalPlanner::with_extension_planners(self.ext_physical_transform_rules.clone());

        // Execute df's physical plan planning and optimization
        planner
            .create_physical_plan(logical_plan, &new_state)
            .await
            .map_err(|e| e.into())
    }

    fn inject_physical_transform_rule(&mut self, rule: Arc<dyn ExtensionPlanner + Send + Sync>) {
        self.ext_physical_transform_rules.push(rule)
    }
}

/// Prevents `FilterExec` from coalescing filtered rows across input batches.
///
/// DataFusion's `FilterExec` buffers surviving rows until it reaches `batch_size`
/// outside the query memory pool. S3 Select scans emit already byte-bounded batches,
/// so setting `batch_size = 1` streams every non-empty filtered batch immediately.
#[derive(Debug)]
struct StreamFilterOutput;

impl PhysicalOptimizerRule for StreamFilterOutput {
    fn optimize(&self, plan: Arc<dyn ExecutionPlan>, _config: &ConfigOptions) -> DFResult<Arc<dyn ExecutionPlan>> {
        plan.transform_up(|node| {
            let Some(filter) = node.downcast_ref::<FilterExec>() else {
                return Ok(Transformed::no(node));
            };
            let filter: Arc<dyn ExecutionPlan> = Arc::new(filter.with_batch_size(1)?);
            Ok(Transformed::yes(filter))
        })
        .data()
    }

    fn name(&self) -> &str {
        "StreamFilterOutput"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

impl PhysicalOptimizer for DefaultPhysicalPlanner {
    fn optimize(&self, plan: Arc<dyn ExecutionPlan>, _session: &SessionCtx) -> QueryResult<Arc<dyn ExecutionPlan>> {
        Ok(plan)
    }

    fn inject_optimizer_rule(&mut self, optimizer_rule: Arc<dyn PhysicalOptimizerRule + Send + Sync>) {
        self.ext_physical_optimizer_rules.push(optimizer_rule);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{Int64Array, RecordBatch};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::datasource::memory::MemorySourceConfig;
    use datafusion::execution::TaskContext;
    use datafusion::logical_expr::Operator;
    use datafusion::physical_expr::expressions::{binary, col, lit};
    use datafusion::physical_plan::collect;

    #[tokio::test]
    async fn filter_output_is_not_coalesced_across_input_batches() {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let batches = [vec![1, 2, 3], vec![4, 5], vec![6], vec![7, 8, 9]]
            .into_iter()
            .map(|ids| RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(Int64Array::from(ids))]).expect("batch"))
            .collect::<Vec<_>>();
        let input = MemorySourceConfig::try_new_exec(&[batches], Arc::clone(&schema), None).expect("input");
        let predicate = binary(col("id", &schema).expect("column"), Operator::NotEq, lit(6_i64), &schema).expect("predicate");
        let filter: Arc<dyn ExecutionPlan> = Arc::new(FilterExec::try_new(predicate, input).expect("filter"));
        let rows = |batches: Vec<RecordBatch>| batches.iter().map(RecordBatch::num_rows).collect::<Vec<_>>();

        let coalesced = collect(Arc::clone(&filter), Arc::new(TaskContext::default()))
            .await
            .expect("stock filter");
        assert_eq!(rows(coalesced), [8]);

        let filter = StreamFilterOutput
            .optimize(filter, &ConfigOptions::default())
            .expect("optimized filter");
        let streamed = collect(filter, Arc::new(TaskContext::default()))
            .await
            .expect("streaming filter");
        assert_eq!(rows(streamed), [3, 2, 3]);
    }
}
