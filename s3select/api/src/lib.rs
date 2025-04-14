use std::fmt::Display;

use datafusion::{common::DataFusionError, sql::sqlparser::parser::ParserError};
use snafu::{Backtrace, Location, Snafu};

pub mod object_store;
pub mod query;
pub mod server;

pub type QueryResult<T> = Result<T, QueryError>;

#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum QueryError {
    Datafusion {
        source: DataFusionError,
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
                source: v,
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
