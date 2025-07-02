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
use datafusion::logical_expr::LogicalPlan;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_planner::ExtensionPlanner;

use super::session::SessionCtx;
use crate::QueryResult;

#[async_trait]
pub trait PhysicalPlanner {
    /// Given a `LogicalPlan`, create an `ExecutionPlan` suitable for execution
    async fn create_physical_plan(
        &self,
        logical_plan: &LogicalPlan,
        session_state: &SessionCtx,
    ) -> QueryResult<Arc<dyn ExecutionPlan>>;

    fn inject_physical_transform_rule(&mut self, rule: Arc<dyn ExtensionPlanner + Send + Sync>);
}
