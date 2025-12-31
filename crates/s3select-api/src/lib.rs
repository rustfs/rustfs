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

use datafusion::{common::DataFusionError, sql::sqlparser::parser::ParserError};
use snafu::{Backtrace, Location, Snafu};
use std::fmt::Display;

pub mod object_store;
pub mod query;
pub mod server;

#[cfg(test)]
mod test;

pub type QueryResult<T> = Result<T, QueryError>;

#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum QueryError {
    #[snafu(display("DataFusion error: {}", source))]
    Datafusion {
        source: Box<DataFusionError>,
        location: Location,
        backtrace: Backtrace,
    },

    #[snafu(display("This feature is not implemented: {}", err))]
    NotImplemented { err: String },

    #[snafu(display("Multi-statement not allow, found num:{}, sql:{}", num, sql))]
    MultiStatement { num: usize, sql: String },

    #[snafu(display("Failed to build QueryDispatcher. err: {}", err))]
    BuildQueryDispatcher { err: String },

    #[snafu(display("The query has been canceled"))]
    Cancel,

    #[snafu(display("{}", source))]
    Parser { source: ParserError },

    #[snafu(display("Udf not exists, name:{}.", name))]
    FunctionNotExists { name: String },

    #[snafu(display("Udf already exists, name:{}.", name))]
    FunctionExists { name: String },

    #[snafu(display("Store Error, e:{}.", e))]
    StoreError { e: String },
}

impl From<DataFusionError> for QueryError {
    fn from(value: DataFusionError) -> Self {
        match value {
            DataFusionError::External(e) if e.downcast_ref::<QueryError>().is_some() => *e.downcast::<QueryError>().unwrap(),

            v => Self::Datafusion {
                source: Box::new(v),
                location: Default::default(),
                backtrace: Backtrace::capture(),
            },
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedTable {
    // path
    table: String,
}

impl ResolvedTable {
    pub fn table(&self) -> &str {
        &self.table
    }
}

impl Display for ResolvedTable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let Self { table } = self;
        write!(f, "{table}")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::common::DataFusionError;
    use datafusion::sql::sqlparser::parser::ParserError;

    #[test]
    fn test_query_error_display() {
        let err = QueryError::NotImplemented {
            err: "feature X".to_string(),
        };
        assert_eq!(err.to_string(), "This feature is not implemented: feature X");

        let err = QueryError::MultiStatement {
            num: 2,
            sql: "SELECT 1; SELECT 2;".to_string(),
        };
        assert_eq!(err.to_string(), "Multi-statement not allow, found num:2, sql:SELECT 1; SELECT 2;");

        let err = QueryError::Cancel;
        assert_eq!(err.to_string(), "The query has been canceled");

        let err = QueryError::FunctionNotExists {
            name: "my_func".to_string(),
        };
        assert_eq!(err.to_string(), "Udf not exists, name:my_func.");

        let err = QueryError::StoreError {
            e: "connection failed".to_string(),
        };
        assert_eq!(err.to_string(), "Store Error, e:connection failed.");
    }

    #[test]
    fn test_query_error_from_datafusion_error() {
        let df_error = DataFusionError::Plan("invalid plan".to_string());
        let query_error: QueryError = df_error.into();

        match query_error {
            QueryError::Datafusion { source, .. } => {
                assert!(source.to_string().contains("invalid plan"));
            }
            _ => panic!("Expected Datafusion error"),
        }
    }

    #[test]
    fn test_query_error_from_parser_error() {
        let parser_error = ParserError::ParserError("syntax error".to_string());
        let query_error = QueryError::Parser { source: parser_error };

        assert!(query_error.to_string().contains("syntax error"));
    }

    #[test]
    fn test_resolved_table() {
        let table = ResolvedTable {
            table: "my_table".to_string(),
        };

        assert_eq!(table.table(), "my_table");
        assert_eq!(table.to_string(), "my_table");
    }

    #[test]
    fn test_resolved_table_clone_and_eq() {
        let table1 = ResolvedTable {
            table: "table1".to_string(),
        };
        let table2 = table1.clone();
        let table3 = ResolvedTable {
            table: "table2".to_string(),
        };

        assert_eq!(table1, table2);
        assert_ne!(table1, table3);
    }
}
