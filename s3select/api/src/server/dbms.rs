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

use async_trait::async_trait;

use crate::{
    QueryResult,
    query::{
        Query,
        execution::{Output, QueryStateMachineRef},
        logical_planner::Plan,
    },
};

pub struct QueryHandle {
    query: Query,
    result: Output,
}

impl QueryHandle {
    pub fn new(query: Query, result: Output) -> Self {
        Self { query, result }
    }

    pub fn query(&self) -> &Query {
        &self.query
    }

    pub fn result(self) -> Output {
        self.result
    }
}

#[async_trait]
pub trait DatabaseManagerSystem {
    async fn execute(&self, query: &Query) -> QueryResult<QueryHandle>;
    async fn build_query_state_machine(&self, query: Query) -> QueryResult<QueryStateMachineRef>;
    async fn build_logical_plan(&self, query_state_machine: QueryStateMachineRef) -> QueryResult<Option<Plan>>;
    async fn execute_logical_plan(
        &self,
        logical_plan: Plan,
        query_state_machine: QueryStateMachineRef,
    ) -> QueryResult<QueryHandle>;
}
