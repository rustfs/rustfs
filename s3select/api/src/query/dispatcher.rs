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

use crate::QueryResult;

use super::{
    Query,
    execution::{Output, QueryStateMachine},
    logical_planner::Plan,
};

#[async_trait]
pub trait QueryDispatcher: Send + Sync {
    // fn create_query_id(&self) -> QueryId;

    // fn query_info(&self, id: &QueryId);

    async fn execute_query(&self, query: &Query) -> QueryResult<Output>;

    async fn build_logical_plan(&self, query_state_machine: Arc<QueryStateMachine>) -> QueryResult<Option<Plan>>;

    async fn execute_logical_plan(&self, logical_plan: Plan, query_state_machine: Arc<QueryStateMachine>) -> QueryResult<Output>;

    async fn build_query_state_machine(&self, query: Query) -> QueryResult<Arc<QueryStateMachine>>;

    // fn running_query_infos(&self) -> Vec<QueryInfo>;

    // fn running_query_status(&self) -> Vec<QueryStatus>;

    // fn cancel_query(&self, id: &QueryId);
}
