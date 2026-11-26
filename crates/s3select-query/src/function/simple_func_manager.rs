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

use crate::function::size_bound::bound_result_size;

pub type SimpleFunctionMetadataManagerRef = Arc<SimpleFunctionMetadataManager>;

/// Explicit allowlist of canonical DataFusion scalar function names exposed to S3 Select SQL.
///
/// S3 Select must not expose the entire DataFusion catalog (such as `arrow_cast`,
/// `repeat`, `lpad`, `rpad`, `replace`, `levenshtein`, `version`, `uuid`, regex,
/// encoding, crypto, or nested-array builders). Aliases declared by an allowlisted
/// UDF (such as `length` / `char_length` for `character_length`, `substring` for
/// `substr`, `trim` for `btrim`, and `pow` for `power`) are registered alongside it.
const ALLOWED_SCALAR_FUNCTIONS: &[&str] = &[
    // Conditional
    "coalesce",
    "greatest",
    "least",
    "nullif",
    "nvl",
    "nvl2",
    // String
    "ascii",
    "bit_length",
    "btrim",
    "character_length",
    "chr",
    "concat",
    "concat_ws",
    "contains",
    "ends_with",
    "initcap",
    "left",
    "lower",
    "ltrim",
    "octet_length",
    "reverse",
    "right",
    "rtrim",
    "split_part",
    "starts_with",
    "strpos",
    "substr",
    "substr_index",
    "upper",
    // Math
    "abs",
    "acos",
    "acosh",
    "asin",
    "asinh",
    "atan",
    "atan2",
    "atanh",
    "cbrt",
    "ceil",
    "cos",
    "cosh",
    "cot",
    "degrees",
    "exp",
    "floor",
    "isnan",
    "iszero",
    "ln",
    "log",
    "log10",
    "log2",
    "nanvl",
    "pi",
    "power",
    "radians",
    "round",
    "signum",
    "sin",
    "sinh",
    "sqrt",
    "tan",
    "tanh",
    "trunc",
    // Date / Time
    "current_date",
    "current_time",
    "date_bin",
    "date_part",
    "date_trunc",
    "from_unixtime",
    "make_date",
    "make_time",
    "now",
    "to_char",
    "to_date",
    "to_local_time",
    "to_time",
    "to_timestamp",
    "to_timestamp_micros",
    "to_timestamp_millis",
    "to_timestamp_nanos",
    "to_timestamp_seconds",
    "to_unixtime",
];

/// Explicit allowlist of canonical DataFusion aggregate function names exposed to S3 Select SQL.
const ALLOWED_AGGREGATE_FUNCTIONS: &[&str] = &["avg", "count", "max", "min", "sum"];

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
        for udf in SessionStateDefaults::default_scalar_functions() {
            if ALLOWED_SCALAR_FUNCTIONS.contains(&udf.name()) {
                let _ = func_meta_manager.register_udf(udf);
            }
        }

        for udaf in SessionStateDefaults::default_aggregate_functions() {
            if ALLOWED_AGGREGATE_FUNCTIONS.contains(&udaf.name()) {
                let _ = func_meta_manager.register_udaf(udaf);
            }
        }

        // S3 Select does not support window functions, so no default window UDFs are registered.

        func_meta_manager
    }
}

impl FunctionMetadataManager for SimpleFunctionMetadataManager {
    fn register_udf(&mut self, f: Arc<ScalarUDF>) -> QueryResult<()> {
        let f = bound_result_size(f);
        self.scalar_functions.insert(f.inner().name().to_uppercase(), Arc::clone(&f));
        for alias in f.aliases() {
            self.scalar_functions.insert(alias.to_uppercase(), Arc::clone(&f));
        }
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn allowlist_entries_all_resolve_in_default_manager() {
        let manager = SimpleFunctionMetadataManager::default();
        for name in ALLOWED_SCALAR_FUNCTIONS {
            assert!(manager.udf(name).is_ok(), "allowlisted scalar function '{name}' must be registered");
        }
        for alias in [
            "length",
            "char_length",
            "substring",
            "trim",
            "pow",
            "ifnull",
            "position",
            "instr",
        ] {
            assert!(manager.udf(alias).is_ok(), "allowlisted alias '{alias}' must be registered");
        }
        for name in ALLOWED_AGGREGATE_FUNCTIONS {
            assert!(manager.udaf(name).is_ok(), "allowlisted aggregate function '{name}' must be registered");
        }
        assert!(manager.udwfs().is_empty(), "S3 Select must not register default window functions");
    }

    #[test]
    fn disallowed_datafusion_functions_are_not_registered() {
        let manager = SimpleFunctionMetadataManager::default();
        for disallowed in [
            "arrow_cast",
            "arrow_try_cast",
            "arrow_typeof",
            "arrow_field",
            "arrow_metadata",
            "cast_to_type",
            "try_cast_to_type",
            "repeat",
            "lpad",
            "rpad",
            "replace",
            "levenshtein",
            "version",
            "uuid",
            "input_file_name",
            "file_row_index",
            "encode",
            "decode",
            "regexp_like",
            "regexp_match",
            "regexp_replace",
            "regexp_count",
            "regexp_instr",
            "random",
            "rand",
            "factorial",
        ] {
            assert!(
                manager.udf(disallowed).is_err(),
                "non-S3-Select scalar function '{disallowed}' must not be registered"
            );
        }
        for disallowed in [
            "array_agg",
            "string_agg",
            "median",
            "percentile_cont",
            "first_value",
            "last_value",
        ] {
            assert!(
                manager.udaf(disallowed).is_err(),
                "non-S3-Select aggregate function '{disallowed}' must not be registered"
            );
        }
        for disallowed in ["row_number", "rank", "dense_rank", "lag", "lead"] {
            assert!(manager.udwf(disallowed).is_err(), "window function '{disallowed}' must not be registered");
        }
    }
}
