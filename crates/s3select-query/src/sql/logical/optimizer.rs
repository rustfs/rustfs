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

use datafusion::{
    execution::SessionStateBuilder,
    logical_expr::LogicalPlan,
    optimizer::{
        OptimizerRule, common_subexpr_eliminate::CommonSubexprEliminate,
        decorrelate_predicate_subquery::DecorrelatePredicateSubquery, eliminate_cross_join::EliminateCrossJoin,
        eliminate_duplicated_expr::EliminateDuplicatedExpr, eliminate_filter::EliminateFilter, eliminate_join::EliminateJoin,
        eliminate_limit::EliminateLimit, eliminate_outer_join::EliminateOuterJoin,
        extract_equijoin_predicate::ExtractEquijoinPredicate, filter_null_join_keys::FilterNullJoinKeys,
        propagate_empty_relation::PropagateEmptyRelation, push_down_filter::PushDownFilter, push_down_limit::PushDownLimit,
        replace_distinct_aggregate::ReplaceDistinctWithAggregate, scalar_subquery_to_join::ScalarSubqueryToJoin,
        simplify_expressions::SimplifyExpressions, single_distinct_to_groupby::SingleDistinctToGroupBy,
    },
};
use rustfs_s3select_api::{
    QueryResult,
    query::{analyzer::AnalyzerRef, logical_planner::QueryPlan, session::SessionCtx},
};
use tracing::debug;

use crate::sql::analyzer::DefaultAnalyzer;

pub trait LogicalOptimizer: Send + Sync {
    fn optimize(&self, plan: &QueryPlan, session: &SessionCtx) -> QueryResult<LogicalPlan>;

    fn inject_optimizer_rule(&mut self, optimizer_rule: Arc<dyn OptimizerRule + Send + Sync>);
}

pub struct DefaultLogicalOptimizer {
    // fit datafusion
    // TODO refactor
    analyzer: AnalyzerRef,
    rules: Vec<Arc<dyn OptimizerRule + Send + Sync>>,
}

impl DefaultLogicalOptimizer {
    #[allow(dead_code)]
    fn with_optimizer_rules(mut self, rules: Vec<Arc<dyn OptimizerRule + Send + Sync>>) -> Self {
        self.rules = rules;
        self
    }
}

impl Default for DefaultLogicalOptimizer {
    fn default() -> Self {
        let analyzer = Arc::new(DefaultAnalyzer::default());

        // additional optimizer rule
        let rules: Vec<Arc<dyn OptimizerRule + Send + Sync>> = vec![
            // df default rules start
            Arc::new(SimplifyExpressions::new()),
            Arc::new(ReplaceDistinctWithAggregate::new()),
            Arc::new(EliminateJoin::new()),
            Arc::new(DecorrelatePredicateSubquery::new()),
            Arc::new(ScalarSubqueryToJoin::new()),
            Arc::new(ExtractEquijoinPredicate::new()),
            // simplify expressions does not simplify expressions in subqueries, so we
            // run it again after running the optimizations that potentially converted
            // subqueries to joins
            Arc::new(SimplifyExpressions::new()),
            Arc::new(EliminateDuplicatedExpr::new()),
            Arc::new(EliminateFilter::new()),
            Arc::new(EliminateCrossJoin::new()),
            Arc::new(CommonSubexprEliminate::new()),
            Arc::new(EliminateLimit::new()),
            Arc::new(PropagateEmptyRelation::new()),
            Arc::new(FilterNullJoinKeys::default()),
            Arc::new(EliminateOuterJoin::new()),
            // Filters can't be pushed down past Limits, we should do PushDownFilter after PushDownLimit
            Arc::new(PushDownLimit::new()),
            Arc::new(PushDownFilter::new()),
            Arc::new(SingleDistinctToGroupBy::new()),
            // The previous optimizations added expressions and projections,
            // that might benefit from the following rules
            Arc::new(SimplifyExpressions::new()),
            Arc::new(CommonSubexprEliminate::new()),
            // PushDownProjection can pushdown Projections through Limits, do PushDownLimit again.
            Arc::new(PushDownLimit::new()),
            // df default rules end
            // custom rules can add at here
        ];

        Self { analyzer, rules }
    }
}

impl LogicalOptimizer for DefaultLogicalOptimizer {
    fn optimize(&self, plan: &QueryPlan, session: &SessionCtx) -> QueryResult<LogicalPlan> {
        let analyzed_plan = { self.analyzer.analyze(&plan.df_plan, session)? };

        debug!("Analyzed logical plan:\n{}\n", plan.df_plan.display_indent_schema(),);

        let optimizeed_plan = {
            SessionStateBuilder::new_from_existing(session.inner().clone())
                .with_optimizer_rules(self.rules.clone())
                .build()
                .optimize(&analyzed_plan)?
        };

        Ok(optimizeed_plan)
    }

    fn inject_optimizer_rule(&mut self, optimizer_rule: Arc<dyn OptimizerRule + Send + Sync>) {
        self.rules.push(optimizer_rule);
    }
}
