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

use std::collections::HashMap;
use std::sync::Arc;

use datafusion::execution::SessionStateDefaults;
use datafusion::logical_expr::{AggregateUDF, ScalarUDF, WindowUDF};
use rustfs_s3select_api::query::function::FunctionMetadataManager;
use rustfs_s3select_api::{QueryError, QueryResult};
use tracing::debug;

pub type SimpleFunctionMetadataManagerRef = Arc<SimpleFunctionMetadataManager>;

#[derive(Debug)]
pub struct SimpleFunctionMetadataManager {
    /// Scalar functions that are registered with the context
    pub scalar_functions: HashMap<String, Arc<ScalarUDF>>,
    /// Aggregate functions registered in the context
    pub aggregate_functions: HashMap<String, Arc<AggregateUDF>>,
    /// Window functions registered in the context
    pub window_functions: HashMap<String, Arc<WindowUDF>>,
}

impl Default for SimpleFunctionMetadataManager {
    fn default() -> Self {
        let mut func_meta_manager = Self {
            scalar_functions: Default::default(),
            aggregate_functions: Default::default(),
            window_functions: Default::default(),
        };
        SessionStateDefaults::default_scalar_functions().into_iter().for_each(|udf| {
            let existing_udf = func_meta_manager.register_udf(udf.clone());
            if let Ok(()) = existing_udf {
                debug!("Overwrote an existing UDF: {}", udf.name());
            }
        });

        SessionStateDefaults::default_aggregate_functions()
            .into_iter()
            .for_each(|udaf| {
                let existing_udaf = func_meta_manager.register_udaf(udaf.clone());
                if let Ok(()) = existing_udaf {
                    debug!("Overwrote an existing UDAF: {}", udaf.name());
                }
            });

        SessionStateDefaults::default_window_functions().into_iter().for_each(|udwf| {
            let existing_udwf = func_meta_manager.register_udwf(udwf.clone());
            if let Ok(()) = existing_udwf {
                debug!("Overwrote an existing UDWF: {}", udwf.name());
            }
        });

        func_meta_manager
    }
}

impl FunctionMetadataManager for SimpleFunctionMetadataManager {
    fn register_udf(&mut self, f: Arc<ScalarUDF>) -> QueryResult<()> {
        self.scalar_functions.insert(f.inner().name().to_uppercase(), f);
        Ok(())
    }

    fn register_udaf(&mut self, f: Arc<AggregateUDF>) -> QueryResult<()> {
        self.aggregate_functions.insert(f.inner().name().to_uppercase(), f);
        Ok(())
    }

    fn register_udwf(&mut self, f: Arc<WindowUDF>) -> QueryResult<()> {
        self.window_functions.insert(f.inner().name().to_uppercase(), f);
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

    fn udfs(&self) -> Vec<String> {
        self.scalar_functions.keys().cloned().collect()
    }
    fn udafs(&self) -> Vec<String> {
        self.aggregate_functions.keys().cloned().collect()
    }
    fn udwfs(&self) -> Vec<String> {
        self.window_functions.keys().cloned().collect()
    }
}
