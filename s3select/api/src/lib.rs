use std::fmt::Display;

use datafusion::common::DataFusionError;
use snafu::{Backtrace, Location, Snafu};

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
