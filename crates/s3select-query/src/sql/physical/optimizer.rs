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

use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::ExecutionPlan;
use rustfs_s3select_api::QueryResult;
use rustfs_s3select_api::query::session::SessionCtx;

pub trait PhysicalOptimizer {
    fn optimize(&self, plan: Arc<dyn ExecutionPlan>, session: &SessionCtx) -> QueryResult<Arc<dyn ExecutionPlan>>;

    fn inject_optimizer_rule(&mut self, optimizer_rule: Arc<dyn PhysicalOptimizerRule + Send + Sync>);
}
