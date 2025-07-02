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

use async_recursion::async_recursion;
use async_trait::async_trait;
use datafusion::sql::{planner::SqlToRel, sqlparser::ast::Statement};
use rustfs_s3select_api::{
    QueryError, QueryResult,
    query::{
        ast::ExtStatement,
        logical_planner::{LogicalPlanner, Plan, QueryPlan},
        session::SessionCtx,
    },
};

use crate::metadata::ContextProviderExtension;

pub struct SqlPlanner<'a, S: ContextProviderExtension> {
    _schema_provider: &'a S,
    df_planner: SqlToRel<'a, S>,
}

#[async_trait]
impl<S: ContextProviderExtension + Send + Sync> LogicalPlanner for SqlPlanner<'_, S> {
    async fn create_logical_plan(&self, statement: ExtStatement, session: &SessionCtx) -> QueryResult<Plan> {
        let plan = { self.statement_to_plan(statement, session).await? };

        Ok(plan)
    }
}

impl<'a, S: ContextProviderExtension + Send + Sync + 'a> SqlPlanner<'a, S> {
    /// Create a new query planner
    pub fn new(schema_provider: &'a S) -> Self {
        SqlPlanner {
            _schema_provider: schema_provider,
            df_planner: SqlToRel::new(schema_provider),
        }
    }

    /// Generate a logical plan from an  Extent SQL statement
    #[async_recursion]
    pub(crate) async fn statement_to_plan(&self, statement: ExtStatement, session: &SessionCtx) -> QueryResult<Plan> {
        match statement {
            ExtStatement::SqlStatement(stmt) => self.df_sql_to_plan(*stmt, session).await,
        }
    }

    async fn df_sql_to_plan(&self, stmt: Statement, _session: &SessionCtx) -> QueryResult<Plan> {
        match stmt {
            Statement::Query(_) => {
                let df_plan = self.df_planner.sql_statement_to_plan(stmt)?;
                let plan = Plan::Query(QueryPlan {
                    df_plan,
                    is_tag_scan: false,
                });

                Ok(plan)
            }
            _ => Err(QueryError::NotImplemented { err: stmt.to_string() }),
        }
    }
}
