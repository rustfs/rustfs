use std::collections::HashSet;
use std::sync::Arc;

use datafusion::logical_expr::{AggregateUDF, ScalarUDF, WindowUDF};

use crate::QueryResult;

pub type FuncMetaManagerRef = Arc<dyn FunctionMetadataManager + Send + Sync>;
pub trait FunctionMetadataManager {
    fn register_udf(&mut self, udf: ScalarUDF) -> QueryResult<()>;

    fn register_udaf(&mut self, udaf: AggregateUDF) -> QueryResult<()>;

    fn register_udwf(&mut self, udwf: WindowUDF) -> QueryResult<()>;

    fn udf(&self, name: &str) -> QueryResult<Arc<ScalarUDF>>;

    fn udaf(&self, name: &str) -> QueryResult<Arc<AggregateUDF>>;

    fn udwf(&self, name: &str) -> QueryResult<Arc<WindowUDF>>;

    fn udfs(&self) -> HashSet<String>;
}
