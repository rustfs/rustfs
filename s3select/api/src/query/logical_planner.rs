use datafusion::arrow::datatypes::SchemaRef;
use datafusion::logical_expr::LogicalPlan as DFPlan;

#[derive(Clone)]
pub enum Plan {
    // only support query sql
    /// Query plan
    Query(QueryPlan),
}

impl Plan {
    pub fn schema(&self) -> SchemaRef {
        match self {
            Self::Query(p) => SchemaRef::from(p.df_plan.schema().as_ref().to_owned()),
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
