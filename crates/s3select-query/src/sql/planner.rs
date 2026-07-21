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

use std::ops::ControlFlow;

use async_recursion::async_recursion;
use async_trait::async_trait;
use datafusion::sql::{
    planner::SqlToRel,
    sqlparser::ast::{
        GroupByExpr, ObjectNamePart, OrderByKind, Query, Select, SelectFlavor, SetExpr, Statement, TableFactor, Visit, Visitor,
    },
};
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
                validate_s3_select_statement(&stmt)?;
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

fn validate_s3_select_statement(statement: &Statement) -> QueryResult<()> {
    let Statement::Query(query) = statement else {
        return Err(unsupported_structure("only SELECT queries are supported"));
    };

    if query.with.is_some()
        || query.order_by.as_ref().is_some_and(|order_by| {
            order_by.interpolate.is_some()
                || match &order_by.kind {
                    OrderByKind::Expressions(expressions) => expressions.iter().any(|expression| expression.with_fill.is_some()),
                    OrderByKind::All(_) => true,
                }
        })
        || query.fetch.is_some()
        || !query.locks.is_empty()
        || query.for_clause.is_some()
        || query.settings.is_some()
        || query.format_clause.is_some()
        || !query.pipe_operators.is_empty()
    {
        return Err(unsupported_structure("the query contains an unsupported clause"));
    }
    if let Some(limit_clause) = query.limit_clause.as_ref()
        && !matches!(
            limit_clause,
            datafusion::sql::sqlparser::ast::LimitClause::LimitOffset {
                limit: Some(_),
                offset: None,
                limit_by,
            } if limit_by.is_empty()
        )
    {
        return Err(unsupported_structure("only LIMIT without OFFSET is supported"));
    }
    if let Some(datafusion::sql::sqlparser::ast::LimitClause::LimitOffset { limit: Some(limit), .. }) =
        query.limit_clause.as_ref()
        && limit.to_string().parse::<u64>().is_err()
    {
        return Err(unsupported_structure("LIMIT must be a non-negative integer"));
    }

    let mut detector = SubqueryDetector { visited_root: false };
    if query.visit(&mut detector).is_break() {
        return Err(unsupported_structure("subqueries are not supported"));
    }

    let SetExpr::Select(select) = query.body.as_ref() else {
        return Err(unsupported_structure("set operations and nested queries are not supported"));
    };
    validate_select(select)
}

fn validate_select(select: &Select) -> QueryResult<()> {
    if !select.optimizer_hints.is_empty()
        || select.distinct.is_some()
        || select.select_modifiers.is_some()
        || select.top.is_some()
        || select.exclude.is_some()
        || select.into.is_some()
        || !select.lateral_views.is_empty()
        || select.prewhere.is_some()
        || !select.connect_by.is_empty()
        || !select.cluster_by.is_empty()
        || !select.distribute_by.is_empty()
        || !select.sort_by.is_empty()
        || select.having.is_some()
        || !select.named_window.is_empty()
        || select.qualify.is_some()
        || select.value_table_mode.is_some()
        || select.flavor != SelectFlavor::Standard
        || !matches!(&select.group_by, GroupByExpr::Expressions(_, modifiers) if modifiers.is_empty())
    {
        return Err(unsupported_structure("the SELECT contains an unsupported clause"));
    }

    let [table] = select.from.as_slice() else {
        return Err(unsupported_structure("exactly one S3Object source is required"));
    };
    if !table.joins.is_empty() {
        return Err(unsupported_structure("JOIN is not supported"));
    }
    let TableFactor::Table {
        name,
        alias,
        args,
        with_hints,
        version,
        with_ordinality,
        partitions,
        sample,
        index_hints,
        ..
    } = &table.relation
    else {
        return Err(unsupported_structure("subqueries and table functions are not supported"));
    };
    if args.is_some()
        || !with_hints.is_empty()
        || version.is_some()
        || *with_ordinality
        || !partitions.is_empty()
        || sample.is_some()
        || !index_hints.is_empty()
        || alias.as_ref().is_some_and(|alias| !alias.columns.is_empty())
    {
        return Err(unsupported_structure("the S3Object source contains unsupported modifiers"));
    }
    let ([ObjectNamePart::Identifier(table_name)] | [ObjectNamePart::Identifier(table_name), ObjectNamePart::Identifier(_)]) =
        name.0.as_slice()
    else {
        return Err(unsupported_structure("the source must be S3Object"));
    };
    let is_s3_object = if table_name.quote_style.is_some() {
        table_name.value == "S3Object"
    } else {
        table_name.value.eq_ignore_ascii_case("S3Object")
    };
    if !is_s3_object {
        return Err(unsupported_structure("the source must be S3Object"));
    }

    Ok(())
}

fn unsupported_structure(message: &str) -> QueryError {
    QueryError::UnsupportedSqlStructure {
        message: message.to_string(),
    }
}

struct SubqueryDetector {
    visited_root: bool,
}

impl Visitor for SubqueryDetector {
    type Break = ();

    fn pre_visit_query(&mut self, _query: &Query) -> ControlFlow<Self::Break> {
        if self.visited_root {
            ControlFlow::Break(())
        } else {
            self.visited_root = true;
            ControlFlow::Continue(())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::validate_s3_select_statement;
    use crate::sql::parser::ExtParser;
    use datafusion::sql::sqlparser::ast::Statement;
    use rustfs_s3select_api::{QueryError, query::ast::ExtStatement};

    fn parse_statement(sql: &str) -> Statement {
        let mut statements = ExtParser::parse_sql(sql).expect("SQL should parse");
        let ExtStatement::SqlStatement(statement) = statements.pop_front().expect("one SQL statement");
        *statement
    }

    #[test]
    fn accepts_s3_select_query_shape() {
        let statement = parse_statement("SELECT s.id FROM S3Object AS s WHERE s.id = '1' LIMIT 10");

        assert!(validate_s3_select_statement(&statement).is_ok());
    }

    #[test]
    fn accepts_json_sub_path_source() {
        let statement = parse_statement("SELECT e.name FROM S3Object.employees AS e");

        assert!(validate_s3_select_statement(&statement).is_ok());
    }

    #[test]
    fn accepts_group_by_and_order_by() {
        let statement = parse_statement("SELECT department, COUNT(*) FROM S3Object GROUP BY department ORDER BY department");

        assert!(validate_s3_select_statement(&statement).is_ok());
    }

    #[test]
    fn rejects_join() {
        let statement = parse_statement("SELECT * FROM S3Object a JOIN S3Object b ON a.id = b.id");

        assert!(matches!(
            validate_s3_select_statement(&statement),
            Err(QueryError::UnsupportedSqlStructure { message }) if message == "JOIN is not supported"
        ));
    }

    #[test]
    fn rejects_subquery() {
        let statement = parse_statement("SELECT * FROM S3Object WHERE id IN (SELECT id FROM S3Object)");

        assert!(matches!(
            validate_s3_select_statement(&statement),
            Err(QueryError::UnsupportedSqlStructure { message }) if message == "subqueries are not supported"
        ));
    }

    #[test]
    fn rejects_subquery_in_order_by() {
        let statement = parse_statement("SELECT id FROM S3Object ORDER BY (SELECT id FROM S3Object)");

        assert!(matches!(
            validate_s3_select_statement(&statement),
            Err(QueryError::UnsupportedSqlStructure { message }) if message == "subqueries are not supported"
        ));
    }

    #[test]
    fn rejects_non_s3_object_source() {
        let statement = parse_statement("SELECT * FROM other_table");

        assert!(matches!(
            validate_s3_select_statement(&statement),
            Err(QueryError::UnsupportedSqlStructure { message }) if message == "the source must be S3Object"
        ));
    }

    #[test]
    fn rejects_unsupported_select_clauses() {
        for sql in [
            "SELECT DISTINCT id FROM S3Object",
            "SELECT * FROM S3Object OFFSET 1",
            "SELECT * FROM S3Object UNION SELECT * FROM S3Object",
        ] {
            let statement = parse_statement(sql);
            assert!(
                matches!(validate_s3_select_statement(&statement), Err(QueryError::UnsupportedSqlStructure { .. })),
                "query should be rejected: {sql}"
            );
        }
    }
}
