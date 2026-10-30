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

use std::{collections::HashMap, convert::Infallible, ops::ControlFlow};

use async_trait::async_trait;
use datafusion::{
    common::{Column, Result as DFResult},
    logical_expr::{Expr as LogicalExpr, LogicalPlan, LogicalPlanBuilder},
    sql::{
        planner::{IdentNormalizer, SqlToRel},
        sqlparser::ast::{
            AccessExpr, Expr, GroupByExpr, Ident, JsonPath, JsonPathElem, ObjectNamePart, OrderBy, OrderByKind, Query, Select,
            SelectFlavor, SelectItem, SetExpr, Statement, Subscript, TableAlias, TableFactor, Value, Visit, VisitMut, Visitor,
            VisitorMut,
        },
    },
};
use rustfs_s3select_api::{
    QueryError, QueryResult, SelectError,
    query::{
        ast::{ExtStatement, JsonPathSegment, JsonSource},
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
    pub(crate) async fn statement_to_plan(&self, statement: ExtStatement, session: &SessionCtx) -> QueryResult<Plan> {
        match statement {
            ExtStatement::SqlStatement(stmt) => self.df_sql_to_plan(*stmt, session).await,
        }
    }

    pub(crate) async fn prepared_statement_to_plan(&self, statement: ExtStatement, session: &SessionCtx) -> QueryResult<Plan> {
        match statement {
            ExtStatement::SqlStatement(stmt) => self.df_prepared_sql_to_plan(*stmt, session).await,
        }
    }

    async fn df_sql_to_plan(&self, mut stmt: Statement, session: &SessionCtx) -> QueryResult<Plan> {
        prepare_s3_select_statement(&mut stmt)?;
        self.df_prepared_sql_to_plan(stmt, session).await
    }

    async fn df_prepared_sql_to_plan(&self, stmt: Statement, _session: &SessionCtx) -> QueryResult<Plan> {
        let keys = path_projection_keys(&stmt);
        let df_plan = self.df_planner.sql_statement_to_plan(stmt).map_err(classify_planner_error)?;
        let df_plan = name_path_projections(df_plan, &keys)?;
        Ok(Plan::Query(QueryPlan {
            df_plan,
            is_tag_scan: false,
        }))
    }
}

fn classify_planner_error(error: datafusion::common::DataFusionError) -> QueryError {
    if matches!(
        &error,
        datafusion::common::DataFusionError::Plan(message)
            if message.starts_with("Failed to coerce arguments to satisfy a call to")
                || (message.starts_with("Internal error: Function '")
                    && message.contains("' failed to match any signature, errors:"))
    ) {
        return SelectError::IncorrectSqlFunctionArgumentType.into();
    }

    error.into()
}

pub(crate) fn prepare_s3_select_statement(statement: &mut Statement) -> QueryResult<JsonSource> {
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
    if Visit::visit(&*query, &mut detector).is_break() {
        return Err(unsupported_structure("subqueries are not supported"));
    }

    let source = {
        let SetExpr::Select(select) = query.body.as_mut() else {
            return Err(unsupported_structure("set operations and nested queries are not supported"));
        };
        prepare_select(select)?
    };
    let mut normalizer = PartiQlSubscriptNormalizer;
    let _ = VisitMut::visit(query, &mut normalizer);
    if let SetExpr::Select(select) = query.body.as_mut() {
        alias_path_projections(select, query.order_by.as_ref());
    }
    Ok(source)
}

/// Names an unaliased path projection after its last key, as AWS does
/// (`s.obj0.id` -> `id`), instead of DataFusion's `s.obj0[id]`. Names that
/// could collide with another output name keep DataFusion's name. Wildcard
/// columns, computed expressions, and retained paths have unknown output names
/// here, so they disable aliasing. Unprojected ORDER BY expressions also disable
/// aliasing because DataFusion adds their input columns to the projection.
/// GROUP BY can refer to these aliases; [`name_path_projections`] names the
/// paths left here once the planned output names are known.
fn alias_path_projections(select: &mut Select, order_by: Option<&OrderBy>) {
    if has_wildcard(select) || sorts_by_unprojected_expr(select, order_by) {
        return;
    }
    let Some(qualifier) = source_qualifier(select) else {
        return;
    };
    let normalizer = IdentNormalizer::default();
    let names: Vec<Option<String>> = select
        .projection
        .iter()
        .map(|item| {
            let ident = match item {
                SelectItem::UnnamedExpr(Expr::Nested(_)) => None,
                SelectItem::UnnamedExpr(expr) => column_name(expr),
                SelectItem::ExprWithAlias { alias, .. } => Some(alias),
                _ => None,
            };
            ident.map(|ident| normalizer.normalize(ident.clone()))
        })
        .collect();
    if names.iter().any(Option::is_none) {
        return;
    }
    let mut counts = HashMap::<String, usize>::new();
    for name in names.iter().flatten() {
        *counts.entry(name.clone()).or_default() += 1;
    }
    // Retained paths have generated names that cannot be inferred from their last key.
    // Keep the whole projection unchanged so a sibling alias cannot collide with them.
    if names.iter().flatten().any(|name| counts.get(name) != Some(&1)) {
        return;
    }
    for item in &mut select.projection {
        let SelectItem::UnnamedExpr(expr) = item else {
            continue;
        };
        let Some(alias) = path_key(expr, &qualifier).cloned() else {
            continue;
        };
        let expr = std::mem::replace(expr, Expr::Identifier(Ident::new("")));
        *item = SelectItem::ExprWithAlias { expr, alias };
    }
}

/// The output name DataFusion gives a column reference: its last part.
fn column_name(expr: &Expr) -> Option<&Ident> {
    match expr {
        Expr::Identifier(ident) => Some(ident),
        Expr::CompoundIdentifier(parts) => parts.last(),
        Expr::Nested(inner) => column_name(inner),
        _ => None,
    }
}

fn has_wildcard(select: &Select) -> bool {
    select
        .projection
        .iter()
        .any(|item| matches!(item, SelectItem::Wildcard(_) | SelectItem::QualifiedWildcard(..)))
}

fn sorts_by_unprojected_expr(select: &Select, order_by: Option<&OrderBy>) -> bool {
    let Some(OrderBy {
        kind: OrderByKind::Expressions(sort_exprs),
        ..
    }) = order_by
    else {
        return false;
    };
    sort_exprs.iter().any(|sort_expr| {
        !select
            .projection
            .iter()
            .any(|item| matches!(item, SelectItem::UnnamedExpr(projected) if *projected == sort_expr.expr))
    })
}

/// The normalized table alias, or table name, that may prefix a path.
fn source_qualifier(select: &Select) -> Option<String> {
    let normalizer = IdentNormalizer::default();
    match &select.from.first()?.relation {
        TableFactor::Table { alias: Some(alias), .. } => Some(normalizer.normalize(alias.name.clone())),
        TableFactor::Table { name, .. } => match name.0.first() {
            Some(ObjectNamePart::Identifier(table_name)) => Some(normalizer.normalize(table_name.clone())),
            _ => None,
        },
        _ => None,
    }
}

/// The last key of a path of at least two keys after the source qualifier
/// (`s.obj0.id` -> `id`), the name AWS gives the projection.
fn path_key<'a>(expr: &'a Expr, qualifier: &str) -> Option<&'a Ident> {
    let parts = match expr {
        Expr::CompoundIdentifier(parts) => parts,
        Expr::Nested(inner) => return path_key(inner, qualifier),
        _ => return None,
    };
    let path = match parts.split_first() {
        Some((first, rest)) if IdentNormalizer::default().normalize(first.clone()) == qualifier => rest,
        _ => parts.as_slice(),
    };
    match path {
        [_, _, ..] => path.last(),
        _ => None,
    }
}

/// The normalized key of each path projection left unaliased by
/// [`alias_path_projections`]. Empty when a wildcard hides the output
/// positions, or when an ORDER BY expression is not projected.
fn path_projection_keys(statement: &Statement) -> Vec<Option<String>> {
    let Statement::Query(query) = statement else {
        return Vec::new();
    };
    let SetExpr::Select(select) = query.body.as_ref() else {
        return Vec::new();
    };
    if has_wildcard(select) || sorts_by_unprojected_expr(select, query.order_by.as_ref()) {
        return Vec::new();
    }
    let Some(qualifier) = source_qualifier(select) else {
        return Vec::new();
    };
    let normalizer = IdentNormalizer::default();
    select
        .projection
        .iter()
        .map(|item| match item {
            SelectItem::UnnamedExpr(expr) => path_key(expr, &qualifier).map(|key| normalizer.normalize(key.clone())),
            _ => None,
        })
        .collect()
}

/// Renames each path projection to its key from [`path_projection_keys`]
/// unless another output column keeps or receives that name. Names come from
/// the planned schema, so computed, aliased and retained columns all count.
fn name_path_projections(plan: LogicalPlan, keys: &[Option<String>]) -> DFResult<LogicalPlan> {
    let schema = plan.schema();
    if keys.len() != schema.fields().len() {
        return Ok(plan);
    }
    let mut renames: Vec<Option<&str>> = keys
        .iter()
        .zip(schema.fields())
        .map(|(key, field)| key.as_deref().filter(|key| key != field.name()))
        .collect();
    // Dropping a rename brings back the generated name, which can collide with another rename.
    loop {
        let mut counts = HashMap::<&str, usize>::new();
        for (rename, field) in renames.iter().zip(schema.fields()) {
            *counts.entry(rename.unwrap_or(field.name())).or_default() += 1;
        }
        let mut dropped = false;
        for rename in &mut renames {
            if rename.is_some_and(|name| counts[name] > 1) {
                *rename = None;
                dropped = true;
            }
        }
        if !dropped {
            break;
        }
    }
    if renames.iter().all(Option::is_none) {
        return Ok(plan);
    }
    let columns: Vec<_> = schema
        .iter()
        .zip(renames)
        .map(|(column, rename)| {
            let column = LogicalExpr::Column(Column::from(column));
            match rename {
                Some(name) => column.alias(name),
                None => column,
            }
        })
        .collect();
    LogicalPlanBuilder::from(plan).project(columns)?.build()
}

struct PartiQlSubscriptNormalizer;

impl VisitorMut for PartiQlSubscriptNormalizer {
    type Break = Infallible;

    fn post_visit_expr(&mut self, expr: &mut Expr) -> ControlFlow<Self::Break> {
        let Expr::JsonAccess { value, path } = expr else {
            return ControlFlow::Continue(());
        };
        if !matches!(path.path.first(), Some(JsonPathElem::Bracket { .. }))
            || path
                .path
                .iter()
                .any(|element| matches!(element, JsonPathElem::ColonBracket { .. }))
        {
            return ControlFlow::Continue(());
        }

        let mut appended_access = Vec::with_capacity(path.path.len());
        for element in std::mem::take(&mut path.path) {
            match element {
                JsonPathElem::Dot { key, quoted } => {
                    let identifier = if quoted {
                        Ident::with_quote('"', key)
                    } else {
                        Ident::new(key)
                    };
                    appended_access.push(AccessExpr::Dot(Expr::Identifier(identifier)));
                }
                JsonPathElem::Bracket { key } => {
                    appended_access.push(AccessExpr::Subscript(Subscript::Index { index: key }));
                }
                JsonPathElem::ColonBracket { key } => {
                    appended_access.push(AccessExpr::Subscript(Subscript::Index { index: key }));
                }
            }
        }

        let value = std::mem::replace(value, Box::new(Expr::Identifier(Ident::new(""))));
        let (root, mut access_chain) = match *value {
            Expr::CompoundFieldAccess { root, access_chain } => (root, access_chain),
            root => (Box::new(root), Vec::new()),
        };
        access_chain.extend(appended_access);
        *expr = Expr::CompoundFieldAccess { root, access_chain };
        ControlFlow::Continue(())
    }
}

fn implicit_source_alias(source_path: &[JsonPathSegment]) -> Ident {
    match source_path.last() {
        Some(JsonPathSegment::Key { name, quoted: true }) => Ident::with_quote('"', name),
        Some(JsonPathSegment::Key { name, quoted: false }) => Ident::new(name),
        Some(JsonPathSegment::Index(_) | JsonPathSegment::ArrayWildcard | JsonPathSegment::ObjectWildcard) | None => {
            Ident::new("_1")
        }
    }
}

fn prepare_select(select: &mut Select) -> QueryResult<JsonSource> {
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

    let [table] = select.from.as_mut_slice() else {
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
        json_path,
    } = &mut table.relation
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
    let Some(ObjectNamePart::Identifier(table_name)) = name.0.first() else {
        return Err(SelectError::DataSourcePathUnsupported.into());
    };
    let is_s3_object = if table_name.quote_style.is_some() {
        table_name.value == "S3Object"
    } else {
        table_name.value.eq_ignore_ascii_case("S3Object")
    };
    if !is_s3_object {
        return Err(SelectError::DataSourcePathUnsupported.into());
    }

    let mut source_path = Vec::new();
    for part in &name.0[1..] {
        let ObjectNamePart::Identifier(identifier) = part else {
            return Err(SelectError::DataSourcePathUnsupported.into());
        };
        if identifier.quote_style.is_none() && identifier.value == "*" {
            source_path.push(JsonPathSegment::ObjectWildcard);
        } else {
            source_path.push(JsonPathSegment::Key {
                name: identifier.value.clone(),
                quoted: identifier.quote_style.is_some(),
            });
        }
    }
    if let Some(json_path) = json_path.as_ref() {
        append_json_path_segments(&mut source_path, json_path)?;
    }
    if alias.is_none() && !source_path.is_empty() {
        *alias = Some(TableAlias {
            explicit: true,
            name: implicit_source_alias(&source_path),
            columns: Vec::new(),
            at: None,
        });
    }
    let scalar_column = alias
        .as_ref()
        .map(|alias| IdentNormalizer::default().normalize(alias.name.clone()))
        .or_else(|| {
            source_path
                .is_empty()
                .then(|| IdentNormalizer::default().normalize(table_name.clone()))
        });
    name.0.truncate(1);
    *json_path = None;
    Ok(JsonSource::new(source_path, scalar_column))
}

fn append_json_path_segments(source_path: &mut Vec<JsonPathSegment>, json_path: &JsonPath) -> QueryResult<()> {
    for element in &json_path.path {
        let segment = match element {
            JsonPathElem::Dot { key, quoted } if key == "*" && !quoted => JsonPathSegment::ObjectWildcard,
            JsonPathElem::Dot { key, quoted } => JsonPathSegment::Key {
                name: key.clone(),
                quoted: *quoted,
            },
            JsonPathElem::Bracket { key: Expr::Wildcard(_) } => JsonPathSegment::ArrayWildcard,
            JsonPathElem::Bracket { key: Expr::Value(value) } => match &value.value {
                Value::Number(number, false) => JsonPathSegment::Index(
                    number
                        .to_string()
                        .parse()
                        .map_err(|_| QueryError::from(SelectError::DataSourcePathUnsupported))?,
                ),
                Value::SingleQuotedString(key) => JsonPathSegment::Key {
                    name: key.clone(),
                    quoted: true,
                },
                _ => return Err(SelectError::DataSourcePathUnsupported.into()),
            },
            JsonPathElem::Bracket { .. } | JsonPathElem::ColonBracket { .. } => {
                return Err(SelectError::DataSourcePathUnsupported.into());
            }
        };
        source_path.push(segment);
    }
    Ok(())
}

fn unsupported_structure(message: &str) -> QueryError {
    SelectError::UnsupportedSqlStructure {
        message: message.to_string(),
    }
    .into()
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
    use super::{path_projection_keys, prepare_s3_select_statement};
    use crate::sql::parser::ExtParser;
    use datafusion::sql::sqlparser::ast::{AccessExpr, Expr, Statement, Visit, Visitor};
    use rustfs_s3select_api::{
        QueryResult, SelectError,
        query::ast::{ExtStatement, JsonPathSegment, JsonSource},
    };
    use std::ops::ControlFlow;

    fn parse_statement(sql: &str) -> Statement {
        let mut statements = ExtParser::parse_sql(sql).expect("SQL should parse");
        let ExtStatement::SqlStatement(statement) = statements.pop_front().expect("one SQL statement");
        *statement
    }

    fn validate_s3_select_statement(statement: &Statement) -> QueryResult<JsonSource> {
        prepare_s3_select_statement(&mut statement.clone())
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
    fn prepares_nested_json_source_path_and_normalizes_table() {
        let mut statement = parse_statement("SELECT e.name FROM S3Object[*].employees[*] AS e");

        let source = prepare_s3_select_statement(&mut statement).expect("JSON source path should be supported");

        assert_eq!(
            source.path(),
            &[
                JsonPathSegment::ArrayWildcard,
                JsonPathSegment::Key {
                    name: "employees".to_string(),
                    quoted: false,
                },
                JsonPathSegment::ArrayWildcard,
            ]
        );
        assert_eq!(statement.to_string(), "SELECT e.name FROM S3Object AS e");
    }

    #[test]
    fn partiql_source_support_preserves_projection_and_filter_subscripts() {
        let mut statement = parse_statement("SELECT s.tags[1] FROM S3Object AS s WHERE s.values[0] = 1");

        prepare_s3_select_statement(&mut statement).expect("array expressions should remain supported");
        let mut counter = FieldAccessCounter::default();
        let _ = Visit::visit(&statement, &mut counter);

        assert_eq!(counter.json_accesses, 0);
        assert_eq!(counter.subscripts, 2);
    }

    #[derive(Default)]
    struct FieldAccessCounter {
        json_accesses: usize,
        subscripts: usize,
    }

    impl Visitor for FieldAccessCounter {
        type Break = ();

        fn pre_visit_expr(&mut self, expr: &Expr) -> ControlFlow<Self::Break> {
            match expr {
                Expr::JsonAccess { .. } => self.json_accesses += 1,
                Expr::CompoundFieldAccess { access_chain, .. } => {
                    self.subscripts += access_chain
                        .iter()
                        .filter(|access| matches!(access, AccessExpr::Subscript(_)))
                        .count();
                }
                _ => {}
            }
            ControlFlow::Continue(())
        }
    }

    #[test]
    fn prepares_array_index_and_object_wildcard_paths() {
        let mut index_statement = parse_statement("SELECT * FROM S3Object[0]");
        let mut wildcard_statement = parse_statement("SELECT * FROM S3Object[*].*");

        assert_eq!(
            prepare_s3_select_statement(&mut index_statement)
                .expect("array index should be supported")
                .path(),
            &[JsonPathSegment::Index(0)]
        );
        assert_eq!(
            prepare_s3_select_statement(&mut wildcard_statement)
                .expect("object wildcard should be supported")
                .path(),
            &[JsonPathSegment::ArrayWildcard, JsonPathSegment::ObjectWildcard]
        );
    }

    #[test]
    fn quoted_star_remains_an_object_key() {
        let mut statement = parse_statement("SELECT * FROM S3Object.\"*\"");

        let source = prepare_s3_select_statement(&mut statement).expect("quoted key should be supported");

        assert_eq!(
            source.path(),
            &[JsonPathSegment::Key {
                name: "*".to_string(),
                quoted: true,
            }]
        );
    }

    #[test]
    fn preserves_quoted_keys_and_adds_implicit_source_aliases() {
        let mut key_statement = parse_statement("SELECT employee.name FROM S3Object[*].department.employee");
        let mut wildcard_statement = parse_statement("SELECT _1.name FROM S3Object[*].employees[*]");

        prepare_s3_select_statement(&mut key_statement).expect("named source path should be supported");
        prepare_s3_select_statement(&mut wildcard_statement).expect("wildcard source path should be supported");

        assert_eq!(key_statement.to_string(), "SELECT employee.name FROM S3Object AS employee");
        assert_eq!(wildcard_statement.to_string(), "SELECT _1.name FROM S3Object AS _1");
    }

    #[test]
    fn root_scalar_aliases_are_preserved_and_unquoted_aliases_are_normalized() {
        let mut implicit = parse_statement("SELECT S3Object FROM S3Object");
        let mut unquoted = parse_statement("SELECT V FROM S3Object AS V");
        let mut quoted = parse_statement("SELECT \"V\" FROM S3Object AS \"V\"");

        let implicit_source = prepare_s3_select_statement(&mut implicit).expect("implicit root alias should be supported");
        let unquoted_source = prepare_s3_select_statement(&mut unquoted).expect("unquoted root alias should be supported");
        let quoted_source = prepare_s3_select_statement(&mut quoted).expect("quoted root alias should be supported");

        assert!(implicit_source.path().is_empty());
        assert_eq!(implicit_source.scalar_column(), Some("s3object"));
        assert!(unquoted_source.path().is_empty());
        assert_eq!(unquoted_source.scalar_column(), Some("v"));
        assert!(quoted_source.path().is_empty());
        assert_eq!(quoted_source.scalar_column(), Some("V"));
    }

    #[test]
    fn unquoted_terminal_scalar_alias_uses_datafusion_identifier_case() {
        let mut statement = parse_statement("SELECT NAME FROM S3Object[*].NAME");

        let source = prepare_s3_select_statement(&mut statement).expect("terminal scalar source should be supported");

        assert_eq!(source.scalar_column(), Some("name"));
        assert_eq!(statement.to_string(), "SELECT NAME FROM S3Object AS NAME");
    }

    #[test]
    fn single_quoted_source_key_adds_a_quoted_implicit_alias() {
        let mut statement = parse_statement("SELECT \"Employee Data\".id FROM S3Object['Employee Data']");

        let source = prepare_s3_select_statement(&mut statement).expect("single-quoted source key should be supported");

        assert_eq!(
            source.path(),
            &[JsonPathSegment::Key {
                name: "Employee Data".to_string(),
                quoted: true,
            }]
        );
        assert_eq!(source.scalar_column(), Some("Employee Data"));
        assert_eq!(statement.to_string(), "SELECT \"Employee Data\".id FROM S3Object AS \"Employee Data\"");
    }

    #[test]
    fn accepts_object_wildcard_continuation() {
        let mut statement = parse_statement("SELECT * FROM S3Object[*].groups.*.id");

        assert_eq!(
            prepare_s3_select_statement(&mut statement)
                .expect("object wildcard continuation should be supported")
                .path(),
            &[
                JsonPathSegment::ArrayWildcard,
                JsonPathSegment::Key {
                    name: "groups".to_string(),
                    quoted: false,
                },
                JsonPathSegment::ObjectWildcard,
                JsonPathSegment::Key {
                    name: "id".to_string(),
                    quoted: false,
                },
            ]
        );
    }

    #[test]
    fn rejects_non_literal_or_out_of_range_array_indexes() {
        for sql in [
            "SELECT * FROM S3Object[-1]",
            "SELECT * FROM S3Object[1 + 1]",
            "SELECT * FROM S3Object[999999999999999999999999999999999999]",
        ] {
            let statement = parse_statement(sql);
            assert!(
                matches!(
                    validate_s3_select_statement(&statement),
                    Err(ref error)
                        if matches!(error.s3_select_policy_error(), Some(SelectError::DataSourcePathUnsupported))
                ),
                "query should reject an unsafe array index: {sql}"
            );
        }
    }

    fn prepared(sql: &str) -> String {
        let mut statement = parse_statement(sql);
        prepare_s3_select_statement(&mut statement).expect("statement should be supported");
        statement.to_string()
    }

    #[test]
    fn unaliased_path_projections_are_named_after_their_last_key() {
        for (sql, expected) in [
            ("SELECT s.obj0.id FROM S3Object AS s", "SELECT s.obj0.id AS id FROM S3Object AS s"),
            (
                "SELECT s.a.b.c, s.x.\"Y\" FROM S3Object AS s",
                "SELECT s.a.b.c AS c, s.x.\"Y\" AS \"Y\" FROM S3Object AS s",
            ),
            ("SELECT obj0.id FROM S3Object", "SELECT obj0.id AS id FROM S3Object"),
            (
                "SELECT s.obj0.id FROM S3Object AS s WHERE s.obj0.id = 'a' ORDER BY s.obj0.id",
                "SELECT s.obj0.id AS id FROM S3Object AS s WHERE s.obj0.id = 'a' ORDER BY s.obj0.id",
            ),
        ] {
            assert_eq!(prepared(sql), expected, "{sql}");
        }
    }

    #[test]
    fn projections_keep_their_names_when_a_key_alias_is_absent_or_ambiguous() {
        for sql in [
            "SELECT s.id FROM S3Object AS s",
            "SELECT S3Object.id FROM S3Object",
            "SELECT s.tags[1] FROM S3Object AS s",
            "SELECT s.obj0.id AS x FROM S3Object AS s",
            "SELECT s.obj0.id, s.obj1.id FROM S3Object AS s",
            "SELECT s.obj0.ID, s.obj1.id FROM S3Object AS s",
            "SELECT s.obj0.id, s.id FROM S3Object AS s",
            "SELECT s.obj0.id, s.name AS id FROM S3Object AS s",
            "SELECT s.obj0.\"Id\", s.obj1.\"Id\" FROM S3Object AS s",
            "SELECT *, s.obj0.id FROM S3Object AS s",
            "SELECT s.*, s.obj0.id FROM S3Object AS s",
            "SELECT s.obj0.id FROM S3Object AS s ORDER BY s.id",
            "SELECT s.obj0.id FROM S3Object AS s ORDER BY upper(id)",
            "SELECT s.obj0.id, (s.id) FROM S3Object AS s",
            "SELECT s.obj.\"lower(s.id)\", LOWER(s.id) FROM S3Object AS s",
            "SELECT s.obj.\"count(*)\", COUNT(*) FROM S3Object AS s GROUP BY s.obj.\"count(*)\"",
            "SELECT s.obj0.id, COUNT(*) FROM S3Object AS s GROUP BY s.obj0.id",
        ] {
            assert_eq!(prepared(sql), sql);
        }
    }

    #[test]
    fn path_projection_keys_cover_paths_left_unaliased() {
        let keys = |sql: &str| {
            let mut statement = parse_statement(sql);
            prepare_s3_select_statement(&mut statement).expect("statement should be supported");
            path_projection_keys(&statement)
        };
        let key = |key: &str| Some(key.to_owned());
        for (sql, expected) in [
            ("SELECT s.obj0.id FROM S3Object AS s", vec![None]),
            ("SELECT s.obj0.id, s.obj1.id FROM S3Object AS s", vec![key("id"), key("id")]),
            (
                "SELECT s.a.b.c, s.x.\"Y\", s.obj0.ID, lower(s.id) FROM S3Object AS s",
                vec![key("c"), key("Y"), key("id"), None],
            ),
            ("SELECT obj0.id, COUNT(*) FROM S3Object GROUP BY obj0.id", vec![key("id"), None]),
            ("SELECT (s.obj0.id), ((s.obj1.id)) FROM S3Object AS s", vec![key("id"), key("id")]),
            (
                "SELECT s.id, s.tags[1], s.obj0.id AS x, (s.id) FROM S3Object AS s",
                vec![None, None, None, None],
            ),
            ("SELECT S3Object.id, lower(id) FROM S3Object", vec![None, None]),
            ("SELECT *, s.obj0.id FROM S3Object AS s", vec![]),
            ("SELECT s.*, s.obj0.id FROM S3Object AS s", vec![]),
            ("SELECT s.obj0.id FROM S3Object AS s ORDER BY s.id", vec![]),
            ("SELECT s.obj0.id FROM S3Object AS s ORDER BY upper(id)", vec![]),
        ] {
            assert_eq!(keys(sql), expected, "{sql}");
        }
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
            Err(ref err) if matches!(
                err.s3_select_policy_error(),
                Some(SelectError::UnsupportedSqlStructure { message }) if message == "JOIN is not supported"
            )
        ));
    }

    #[test]
    fn rejects_subquery() {
        let statement = parse_statement("SELECT * FROM S3Object WHERE id IN (SELECT id FROM S3Object)");

        assert!(matches!(
            validate_s3_select_statement(&statement),
            Err(ref err) if matches!(
                err.s3_select_policy_error(),
                Some(SelectError::UnsupportedSqlStructure { message }) if message == "subqueries are not supported"
            )
        ));
    }

    #[test]
    fn rejects_subquery_in_order_by() {
        let statement = parse_statement("SELECT id FROM S3Object ORDER BY (SELECT id FROM S3Object)");

        assert!(matches!(
            validate_s3_select_statement(&statement),
            Err(ref err) if matches!(
                err.s3_select_policy_error(),
                Some(SelectError::UnsupportedSqlStructure { message }) if message == "subqueries are not supported"
            )
        ));
    }

    #[test]
    fn rejects_non_s3_object_source() {
        let statement = parse_statement("SELECT * FROM other_table");

        assert!(matches!(
            validate_s3_select_statement(&statement),
            Err(ref err) if matches!(
                err.s3_select_policy_error(),
                Some(SelectError::DataSourcePathUnsupported)
            )
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
                matches!(
                    validate_s3_select_statement(&statement),
                    Err(ref err) if matches!(err.s3_select_policy_error(), Some(SelectError::UnsupportedSqlStructure { .. }))
                ),
                "query should be rejected: {sql}"
            );
        }
    }
}
