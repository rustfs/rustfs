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

#![recursion_limit = "256"]

use datafusion::{
    arrow::error::ArrowError,
    common::{DataFusionError, SchemaError},
    parquet::errors::ParquetError,
    sql::sqlparser::parser::ParserError,
};
use std::{error::Error as StdError, fmt::Display};
use thiserror::Error;

pub mod object_store;
pub mod query;
pub mod server;
mod storage_api;
pub use storage_api::SelectObjectSnapshot;

#[cfg(test)]
mod test;

pub type QueryResult<T> = Result<T, QueryError>;
pub(crate) use storage_api::crate_boundary::{
    PrepareSelectObjectSnapshotError, SELECT_DEFAULT_READ_BUFFER_SIZE, SelectGetObjectReader, SelectObjectOptions,
    SelectObjectSnapshotReadError, SelectStorageError, SelectStore, SnapshotConsistencyError, resolve_select_object_store_handle,
    select_is_err_bucket_not_found, select_is_err_object_not_found, select_is_err_version_not_found,
};

#[derive(Debug, Error)]
pub enum QueryError {
    #[error("DataFusion error: {source}")]
    Datafusion { source: Box<DataFusionError> },

    #[error("This feature is not implemented: {err}")]
    NotImplemented { err: String },

    #[error("Multi-statement not allow, found num:{num}, sql:{sql}")]
    MultiStatement { num: usize, sql: String },

    #[error("Failed to build QueryDispatcher. err: {err}")]
    BuildQueryDispatcher { err: String },

    #[error("The query has been canceled")]
    Cancel,

    #[error("{source}")]
    Parser {
        #[from]
        source: ParserError,
    },

    #[error("Udf not exists, name:{name}.")]
    FunctionNotExists { name: String },

    #[error("Udf already exists, name:{name}.")]
    FunctionExists { name: String },

    #[error("Store Error, e:{e}.")]
    StoreError { e: String },
}

#[derive(Clone, Debug, Error, PartialEq, Eq)]
pub enum SelectError {
    #[error("The file is not in a supported compression format. Only GZIP and BZIP2 are supported.")]
    InvalidCompressionFormat,

    #[error("The data source type is not valid. Only CSV, JSON, and Parquet are supported.")]
    InvalidDataSource,

    #[error(
        "Object decompression failed. Check that the object is properly compressed using the format specified in the request."
    )]
    TruncatedInput,

    #[error("An error occurred while parsing the CSV file. Check the file and try again.")]
    CsvParsingError,

    #[error("An error occurred while parsing the JSON file. Check the file and try again.")]
    JsonParsingError,

    #[error("An error occurred while parsing the Parquet file. Check the file and try again.")]
    ParquetParsingError,

    #[error("{message}")]
    ParseSelectFailure { message: String },

    #[error("The SQL expression is invalid.")]
    InvalidQuery,

    #[error("The SQL expression contains a data type that is not valid.")]
    InvalidDataType,

    #[error("An incorrect argument type was specified in a function call in the SQL expression.")]
    IncorrectSqlFunctionArgumentType,

    #[error("The data source path in the SQL expression is not supported.")]
    DataSourcePathUnsupported,

    #[error("Unsupported S3 Select SQL structure: {message}")]
    UnsupportedSqlStructure { message: String },

    #[error("We encountered an unsupported SQL operation.")]
    UnsupportedSqlOperation,

    #[error("A column name or a path provided does not exist in the SQL expression.")]
    EvaluatorBindingDoesNotExist,

    #[error("The field name matches to multiple fields in the file. Check the SQL expression and the file, and try again.")]
    AmbiguousFieldName,

    #[error("The value of a parameter in ScanRange element is invalid. Check the service API documentation and try again.")]
    InvalidScanRange,

    #[error("S3 Select query concurrency limit reached")]
    QueryConcurrencyLimit,

    #[error("S3 Select query exceeded the {seconds}-second execution limit")]
    QueryTimeout { seconds: u64 },

    #[error("S3 Select query resource limit exceeded")]
    ResourceExhausted,

    #[error("The specified bucket does not exist.")]
    BucketNotFound,

    #[error("The specified key does not exist.")]
    ObjectNotFound,

    #[error("The query was canceled")]
    Canceled,

    #[error("An internal error occurred.")]
    InternalError,
}

pub type S3SelectPolicyError = SelectError;

const MAX_ERROR_SOURCE_DEPTH: usize = 16;

impl QueryError {
    fn source_error<T: StdError + 'static>(&self) -> Option<&T> {
        let mut err: &(dyn StdError + 'static) = self;
        for _ in 0..MAX_ERROR_SOURCE_DEPTH {
            if let Some(source) = err.downcast_ref::<T>() {
                return Some(source);
            }
            err = err.source()?;
        }
        None
    }

    pub fn is_snapshot_consistency_error(&self) -> bool {
        self.source_error::<SnapshotConsistencyError>().is_some()
    }

    pub fn s3_select_policy_error(&self) -> Option<&S3SelectPolicyError> {
        self.source_error()
    }

    pub fn select_error(&self) -> SelectError {
        let mut err: &(dyn StdError + 'static) = match self {
            Self::Datafusion { source } => source.as_ref(),
            _ => self,
        };
        for _ in 0..MAX_ERROR_SOURCE_DEPTH {
            if let Some(select_error) = classify_select_error_source(err) {
                return select_error;
            }
            let Some(source) = err.source() else {
                break;
            };
            err = source;
        }

        match self {
            QueryError::NotImplemented { .. } => SelectError::UnsupportedSqlOperation,
            QueryError::MultiStatement { .. } => SelectError::UnsupportedSqlStructure {
                message: "multiple SQL statements are not supported".to_string(),
            },
            QueryError::BuildQueryDispatcher { .. } | QueryError::FunctionExists { .. } | QueryError::StoreError { .. } => {
                SelectError::InternalError
            }
            QueryError::Cancel => SelectError::Canceled,
            QueryError::FunctionNotExists { .. } => SelectError::InvalidQuery,
            QueryError::Datafusion { .. } | QueryError::Parser { .. } => SelectError::InternalError,
        }
    }
}

fn classify_select_error_source(err: &(dyn StdError + 'static)) -> Option<SelectError> {
    if let Some(error) = err.downcast_ref::<SelectError>() {
        return Some(error.clone());
    }
    if let Some(error) = err.downcast_ref::<object_store::SelectObjectStoreError>() {
        return Some(error.select_error());
    }
    if let Some(error) = err.downcast_ref::<datafusion::object_store::Error>() {
        return match error {
            datafusion::object_store::Error::NotFound { source, .. } => Some(
                source
                    .downcast_ref::<object_store::SelectObjectStoreError>()
                    .map_or(SelectError::ObjectNotFound, object_store::SelectObjectStoreError::select_error),
            ),
            _ => None,
        };
    }
    if let Some(error) = err.downcast_ref::<ParserError>() {
        return Some(SelectError::ParseSelectFailure {
            message: error.to_string(),
        });
    }
    if let Some(error) = err.downcast_ref::<ArrowError>() {
        return match error {
            ArrowError::CsvError(_) => Some(SelectError::CsvParsingError),
            ArrowError::JsonError(_) => Some(SelectError::JsonParsingError),
            ArrowError::ParquetError(_) => Some(SelectError::ParquetParsingError),
            ArrowError::CastError(_) | ArrowError::ParseError(_) => Some(SelectError::InvalidDataType),
            ArrowError::MemoryError(_) => Some(SelectError::ResourceExhausted),
            ArrowError::ExternalError(_) | ArrowError::IoError(_, _) => None,
            _ => Some(SelectError::InternalError),
        };
    }
    if let Some(error) = err.downcast_ref::<ParquetError>() {
        return match error {
            ParquetError::External(_) => None,
            _ => Some(SelectError::ParquetParsingError),
        };
    }
    if let Some(error) = err.downcast_ref::<SchemaError>() {
        return Some(match error {
            SchemaError::FieldNotFound { .. } => SelectError::EvaluatorBindingDoesNotExist,
            SchemaError::AmbiguousReference { .. }
            | SchemaError::DuplicateQualifiedField { .. }
            | SchemaError::DuplicateUnqualifiedField { .. } => SelectError::AmbiguousFieldName,
        });
    }
    if let Some(error) = err.downcast_ref::<DataFusionError>() {
        return match error {
            DataFusionError::NotImplemented(_) => Some(SelectError::UnsupportedSqlOperation),
            DataFusionError::Plan(_) => Some(SelectError::InvalidQuery),
            DataFusionError::ResourcesExhausted(_) => Some(SelectError::ResourceExhausted),
            DataFusionError::Internal(_)
            | DataFusionError::Execution(_)
            | DataFusionError::Configuration(_)
            | DataFusionError::Substrait(_)
            | DataFusionError::Ffi(_) => Some(SelectError::InternalError),
            DataFusionError::ArrowError(_, _)
            | DataFusionError::ParquetError(_)
            | DataFusionError::ObjectStore(_)
            | DataFusionError::IoError(_)
            | DataFusionError::SQL(_, _)
            | DataFusionError::SchemaError(_, _)
            | DataFusionError::ExecutionJoin(_)
            | DataFusionError::External(_)
            | DataFusionError::Context(_, _)
            | DataFusionError::Diagnostic(_, _)
            | DataFusionError::Collection(_)
            | DataFusionError::Shared(_) => None,
        };
    }
    None
}

impl From<SelectError> for QueryError {
    fn from(value: SelectError) -> Self {
        Self::Datafusion {
            source: Box::new(DataFusionError::External(Box::new(value))),
        }
    }
}

impl From<DataFusionError> for QueryError {
    fn from(value: DataFusionError) -> Self {
        match value {
            DataFusionError::External(e) => match e.downcast::<QueryError>() {
                Ok(query_error) => *query_error,
                Err(e) => Self::Datafusion {
                    source: Box::new(DataFusionError::External(e)),
                },
            },
            v => Self::Datafusion { source: Box::new(v) },
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

        let err = SelectError::UnsupportedSqlStructure {
            message: "JOIN is not supported".to_string(),
        };
        assert_eq!(err.to_string(), "Unsupported S3 Select SQL structure: JOIN is not supported");

        let err = QueryError::Cancel;
        assert_eq!(err.to_string(), "The query has been canceled");

        assert_eq!(
            SelectError::QueryConcurrencyLimit.to_string(),
            "S3 Select query concurrency limit reached"
        );
        assert_eq!(
            SelectError::QueryTimeout { seconds: 300 }.to_string(),
            "S3 Select query exceeded the 300-second execution limit"
        );

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
    fn query_error_variants_remain_source_compatible() {
        fn exhaustive_match(err: QueryError) {
            match err {
                QueryError::Datafusion { .. }
                | QueryError::NotImplemented { .. }
                | QueryError::MultiStatement { .. }
                | QueryError::BuildQueryDispatcher { .. }
                | QueryError::Cancel
                | QueryError::Parser { .. }
                | QueryError::FunctionNotExists { .. }
                | QueryError::FunctionExists { .. }
                | QueryError::StoreError { .. } => {}
            }
        }

        exhaustive_match(QueryError::Cancel);
    }

    #[test]
    fn policy_error_is_recoverable_from_query_error() {
        let err: QueryError = SelectError::QueryTimeout { seconds: 300 }.into();

        assert!(matches!(err.s3_select_policy_error(), Some(SelectError::QueryTimeout { seconds: 300 })));
    }

    #[test]
    fn query_error_classifies_data_errors_without_display_matching() {
        let cases = [
            (
                DataFusionError::ArrowError(Box::new(ArrowError::CsvError("private csv detail".to_string())), None),
                SelectError::CsvParsingError,
            ),
            (
                DataFusionError::ArrowError(Box::new(ArrowError::JsonError("private json detail".to_string())), None),
                SelectError::JsonParsingError,
            ),
            (
                DataFusionError::ParquetError(Box::new(ParquetError::General("private parquet detail".to_string()))),
                SelectError::ParquetParsingError,
            ),
            (
                DataFusionError::External(Box::new(SelectError::TruncatedInput)),
                SelectError::TruncatedInput,
            ),
            (
                DataFusionError::ArrowError(
                    Box::new(ArrowError::InvalidArgumentError("private implementation detail".to_string())),
                    None,
                ),
                SelectError::InternalError,
            ),
            (
                DataFusionError::ArrowError(Box::new(ArrowError::CastError("invalid cast".to_string())), None),
                SelectError::InvalidDataType,
            ),
            (
                DataFusionError::ArrowError(Box::new(ArrowError::MemoryError("query memory limit".to_string())), None),
                SelectError::ResourceExhausted,
            ),
            (
                DataFusionError::Execution("private execution detail".to_string()),
                SelectError::InternalError,
            ),
            (DataFusionError::Plan("invalid expression".to_string()), SelectError::InvalidQuery),
            (
                DataFusionError::NotImplemented("unsupported expression".to_string()),
                SelectError::UnsupportedSqlOperation,
            ),
            (
                DataFusionError::SchemaError(
                    Box::new(SchemaError::FieldNotFound {
                        field: Box::new(datafusion::common::Column::from_name("missing")),
                        valid_fields: Vec::new(),
                    }),
                    Box::new(None),
                ),
                SelectError::EvaluatorBindingDoesNotExist,
            ),
            (
                DataFusionError::SchemaError(
                    Box::new(SchemaError::AmbiguousReference {
                        field: Box::new(datafusion::common::Column::from_name("duplicate")),
                    }),
                    Box::new(None),
                ),
                SelectError::AmbiguousFieldName,
            ),
        ];

        for (source, expected) in cases {
            let error = QueryError::from(source);
            assert_eq!(error.select_error(), expected, "wrong classification for {error:?}");
        }
    }

    #[test]
    fn query_error_preserves_typed_object_store_classification() {
        let bucket_error = QueryError::from(DataFusionError::ObjectStore(Box::new(datafusion::object_store::Error::NotFound {
            path: "private-bucket/private-object".to_string(),
            source: Box::new(object_store::SelectObjectStoreError::BucketNotFound {
                source: SelectStorageError::BucketNotFound("private-bucket".to_string()),
            }),
        })));
        let object_error = QueryError::from(DataFusionError::ObjectStore(Box::new(datafusion::object_store::Error::NotFound {
            path: "private-bucket/private-object".to_string(),
            source: Box::new(object_store::SelectObjectStoreError::ObjectNotFound {
                source: SelectStorageError::ObjectNotFound("private-bucket".to_string(), "private-object".to_string()),
            }),
        })));
        let scan_range_error =
            QueryError::from(DataFusionError::ObjectStore(Box::new(datafusion::object_store::Error::Generic {
                store: "test",
                source: Box::new(object_store::SelectObjectStoreError::InvalidScanRange),
            })));
        let storage_error = QueryError::from(DataFusionError::ObjectStore(Box::new(datafusion::object_store::Error::Generic {
            store: "test",
            source: Box::new(object_store::SelectObjectStoreError::Storage {
                source: SelectStorageError::LessData,
            }),
        })));

        assert_eq!(bucket_error.select_error(), SelectError::BucketNotFound);
        assert_eq!(object_error.select_error(), SelectError::ObjectNotFound);
        assert_eq!(scan_range_error.select_error(), SelectError::InvalidScanRange);
        assert_eq!(storage_error.select_error(), SelectError::InternalError);
    }

    #[test]
    fn select_error_source_traversal_stops_at_the_depth_bound() {
        #[derive(Debug)]
        struct CyclicError;

        impl std::fmt::Display for CyclicError {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str("cyclic error")
            }
        }

        impl StdError for CyclicError {
            fn source(&self) -> Option<&(dyn StdError + 'static)> {
                Some(self)
            }
        }

        let error = QueryError::from(DataFusionError::External(Box::new(CyclicError)));
        assert_eq!(error.select_error(), SelectError::InternalError);
    }

    #[test]
    fn snapshot_consistency_error_is_recoverable_without_string_matching() {
        let err = QueryError::Datafusion {
            source: Box::new(DataFusionError::External(Box::new(SelectObjectSnapshotReadError::Consistency(
                SnapshotConsistencyError::LockLost,
            )))),
        };

        assert!(err.is_snapshot_consistency_error());
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
