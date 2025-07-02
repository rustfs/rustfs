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
use datafusion::execution::SessionStateBuilder;
use datafusion::logical_expr::LogicalPlan;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_optimizer::aggregate_statistics::AggregateStatistics;
use datafusion::physical_optimizer::coalesce_batches::CoalesceBatches;
use datafusion::physical_optimizer::join_selection::JoinSelection;
use datafusion::physical_plan::ExecutionPlan;
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

impl DefaultPhysicalPlanner {
    #[allow(dead_code)]
    fn with_physical_transform_rules(mut self, rules: Vec<Arc<dyn ExtensionPlanner + Send + Sync>>) -> Self {
        self.ext_physical_transform_rules = rules;
        self
    }
}

impl DefaultPhysicalPlanner {
    #[allow(dead_code)]
    fn with_optimizer_rules(mut self, rules: Vec<Arc<dyn PhysicalOptimizerRule + Send + Sync>>) -> Self {
        self.ext_physical_optimizer_rules = rules;
        self
    }
}

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
            // The CoalesceBatches rule will not influence the distribution and ordering of the
            // whole plan tree. Therefore, to avoid influencing other rules, it should run last.
            Arc::new(CoalesceBatches::new()),
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
        // 将扩展的物理计划优化规则注入 df 的 session state
        let new_state = SessionStateBuilder::new_from_existing(session.inner().clone())
            .with_physical_optimizer_rules(self.ext_physical_optimizer_rules.clone())
            .build();

        // 通过扩展的物理计划转换规则构造 df 的 Physical Planner
        let planner = DFDefaultPhysicalPlanner::with_extension_planners(self.ext_physical_transform_rules.clone());

        // 执行 df 的物理计划规划及优化
        planner
            .create_physical_plan(logical_plan, &new_state)
            .await
            .map_err(|e| e.into())
    }

    fn inject_physical_transform_rule(&mut self, rule: Arc<dyn ExtensionPlanner + Send + Sync>) {
        self.ext_physical_transform_rules.push(rule)
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
