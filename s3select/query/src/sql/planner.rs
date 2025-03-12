use datafusion::sql::planner::SqlToRel;

use crate::metadata::ContextProviderExtension;

pub struct SqlPlanner<'a, S: ContextProviderExtension> {
    schema_provider: &'a S,
    df_planner: SqlToRel<'a, S>,
}
