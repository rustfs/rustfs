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

use datafusion::logical_expr::{AggregateUDF, ScalarUDF, WindowUDF};

use crate::QueryResult;

pub type FuncMetaManagerRef = Arc<dyn FunctionMetadataManager + Send + Sync>;
pub trait FunctionMetadataManager {
    fn register_udf(&mut self, udf: Arc<ScalarUDF>) -> QueryResult<()>;

    fn register_udaf(&mut self, udaf: Arc<AggregateUDF>) -> QueryResult<()>;

    fn register_udwf(&mut self, udwf: Arc<WindowUDF>) -> QueryResult<()>;

    fn udf(&self, name: &str) -> QueryResult<Arc<ScalarUDF>>;

    fn udaf(&self, name: &str) -> QueryResult<Arc<AggregateUDF>>;

    fn udwf(&self, name: &str) -> QueryResult<Arc<WindowUDF>>;

    fn udfs(&self) -> Vec<String>;
    fn udafs(&self) -> Vec<String>;
    fn udwfs(&self) -> Vec<String>;
}
