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
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::logical_expr::LogicalPlan as DFPlan;
use std::sync::Arc;

use crate::QueryResult;

use super::ast::ExtStatement;
use super::session::SessionCtx;

#[derive(Clone)]
pub enum Plan {
    // only support query sql
    /// Query plan
    Query(QueryPlan),
}

impl Plan {
    pub fn schema(&self) -> SchemaRef {
        match self {
            Self::Query(p) => Arc::new(p.df_plan.schema().as_arrow().clone()),
        }
    }
}

#[derive(Debug, Clone)]
pub struct QueryPlan {
    pub df_plan: DFPlan,
    pub is_tag_scan: bool,
}

impl QueryPlan {
    pub fn is_explain(&self) -> bool {
        matches!(self.df_plan, DFPlan::Explain(_) | DFPlan::Analyze(_))
    }
}

#[async_trait]
pub trait LogicalPlanner {
    async fn create_logical_plan(&self, statement: ExtStatement, session: &SessionCtx) -> QueryResult<Plan>;
}
