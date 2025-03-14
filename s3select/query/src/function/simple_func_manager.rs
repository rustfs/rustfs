use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use api::query::function::FunctionMetadataManager;
use api::{QueryError, QueryResult};
use datafusion::logical_expr::{AggregateUDF, ScalarUDF, WindowUDF};

pub type SimpleFunctionMetadataManagerRef = Arc<SimpleFunctionMetadataManager>;

#[derive(Debug, Default)]
pub struct SimpleFunctionMetadataManager {
    /// Scalar functions that are registered with the context
    pub scalar_functions: HashMap<String, Arc<ScalarUDF>>,
    /// Aggregate functions registered in the context
    pub aggregate_functions: HashMap<String, Arc<AggregateUDF>>,
    /// Window functions registered in the context
    pub window_functions: HashMap<String, Arc<WindowUDF>>,
}

impl FunctionMetadataManager for SimpleFunctionMetadataManager {
    fn register_udf(&mut self, f: ScalarUDF) -> QueryResult<()> {
        self.scalar_functions.insert(f.inner().name().to_uppercase(), Arc::new(f));
        Ok(())
    }

    fn register_udaf(&mut self, f: AggregateUDF) -> QueryResult<()> {
        self.aggregate_functions.insert(f.inner().name().to_uppercase(), Arc::new(f));
        Ok(())
    }

    fn register_udwf(&mut self, f: WindowUDF) -> QueryResult<()> {
        self.window_functions.insert(f.inner().name().to_uppercase(), Arc::new(f));
        Ok(())
    }

    fn udf(&self, name: &str) -> QueryResult<Arc<ScalarUDF>> {
        let result = self.scalar_functions.get(&name.to_uppercase());

        result
            .cloned()
            .ok_or_else(|| QueryError::FunctionExists { name: name.to_string() })
    }

    fn udaf(&self, name: &str) -> QueryResult<Arc<AggregateUDF>> {
        let result = self.aggregate_functions.get(&name.to_uppercase());

        result
            .cloned()
            .ok_or_else(|| QueryError::FunctionNotExists { name: name.to_string() })
    }

    fn udwf(&self, name: &str) -> QueryResult<Arc<WindowUDF>> {
        let result = self.window_functions.get(&name.to_uppercase());

        result
            .cloned()
            .ok_or_else(|| QueryError::FunctionNotExists { name: name.to_string() })
    }

    fn udfs(&self) -> HashSet<String> {
        self.scalar_functions.keys().cloned().collect()
    }
}
