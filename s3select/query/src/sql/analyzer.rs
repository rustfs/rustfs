use api::query::analyzer::Analyzer;
use api::query::session::SessionCtx;
use api::QueryResult;
use datafusion::logical_expr::LogicalPlan;
use datafusion::optimizer::analyzer::Analyzer as DFAnalyzer;

pub struct DefaultAnalyzer {
    inner: DFAnalyzer,
}

impl DefaultAnalyzer {
    pub fn new() -> Self {
        let analyzer = DFAnalyzer::default();
        // we can add analyzer rule at here

        Self { inner: analyzer }
    }
}

impl Default for DefaultAnalyzer {
    fn default() -> Self {
        Self::new()
    }
}

impl Analyzer for DefaultAnalyzer {
    fn analyze(&self, plan: &LogicalPlan, session: &SessionCtx) -> QueryResult<LogicalPlan> {
        let plan = self
            .inner
            .execute_and_check(plan.to_owned(), session.inner().config_options(), |_, _| {})?;
        Ok(plan)
    }
}
