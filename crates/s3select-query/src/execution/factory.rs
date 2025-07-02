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
use rustfs_s3select_api::{
    QueryError,
    query::{
        execution::{QueryExecutionFactory, QueryExecutionRef, QueryStateMachineRef},
        logical_planner::Plan,
        optimizer::Optimizer,
        scheduler::SchedulerRef,
    },
};

use super::query::SqlQueryExecution;

pub type QueryExecutionFactoryRef = Arc<dyn QueryExecutionFactory + Send + Sync>;

pub struct SqlQueryExecutionFactory {
    optimizer: Arc<dyn Optimizer + Send + Sync>,
    scheduler: SchedulerRef,
}

impl SqlQueryExecutionFactory {
    #[inline(always)]
    pub fn new(optimizer: Arc<dyn Optimizer + Send + Sync>, scheduler: SchedulerRef) -> Self {
        Self { optimizer, scheduler }
    }
}

#[async_trait]
impl QueryExecutionFactory for SqlQueryExecutionFactory {
    async fn create_query_execution(
        &self,
        plan: Plan,
        state_machine: QueryStateMachineRef,
    ) -> Result<QueryExecutionRef, QueryError> {
        match plan {
            Plan::Query(query_plan) => Ok(Arc::new(SqlQueryExecution::new(
                state_machine,
                query_plan,
                self.optimizer.clone(),
                self.scheduler.clone(),
            ))),
        }
    }
}
