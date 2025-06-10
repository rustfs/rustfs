use api::{
    QueryError, QueryResult,
    query::{
        ast::ExtStatement,
        logical_planner::{LogicalPlanner, Plan, QueryPlan},
        session::SessionCtx,
    },
};
use async_recursion::async_recursion;
use async_trait::async_trait;
use datafusion::sql::{planner::SqlToRel, sqlparser::ast::Statement};

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
