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
use datafusion::physical_plan::ExecutionPlan;

use super::logical_planner::QueryPlan;
use super::session::SessionCtx;
use crate::QueryResult;

pub type OptimizerRef = Arc<dyn Optimizer + Send + Sync>;

#[async_trait]
pub trait Optimizer {
    async fn optimize(&self, plan: &QueryPlan, session: &SessionCtx) -> QueryResult<Arc<dyn ExecutionPlan>>;
}
