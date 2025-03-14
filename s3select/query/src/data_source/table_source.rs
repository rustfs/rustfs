use std::any::Any;
use std::borrow::Cow;
use std::fmt::Display;
use std::sync::Arc;
use std::write;

use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::Result as DFResult;
use datafusion::datasource::listing::ListingTable;
use datafusion::datasource::{provider_as_source, TableProvider};
use datafusion::error::DataFusionError;
use datafusion::logical_expr::{LogicalPlan, LogicalPlanBuilder, TableProviderFilterPushDown, TableSource};
use datafusion::prelude::Expr;
use datafusion::sql::TableReference;
use tracing::debug;

pub const TEMP_LOCATION_TABLE_NAME: &str = "external_location_table";

pub struct TableSourceAdapter {
    database_name: String,
    table_name: String,
    table_handle: TableHandle,

    plan: LogicalPlan,
}

impl TableSourceAdapter {
    pub fn try_new(
        table_ref: impl Into<TableReference>,
        table_name: impl Into<String>,
        table_handle: impl Into<TableHandle>,
    ) -> Result<Self, DataFusionError> {
        let table_name: String = table_name.into();

        let table_handle = table_handle.into();
        let plan = match &table_handle {
            // TableScan
            TableHandle::External(t) => {
                let table_source = provider_as_source(t.clone());
                LogicalPlanBuilder::scan(table_ref, table_source, None)?.build()?
            }
            // TableScan
            TableHandle::TableProvider(t) => {
                let table_source = provider_as_source(t.clone());
                if let Some(plan) = table_source.get_logical_plan() {
                    LogicalPlanBuilder::from(plan.into_owned()).build()?
                } else {
                    LogicalPlanBuilder::scan(table_ref, table_source, None)?.build()?
                }
            }
        };

        debug!("Table source logical plan node of {}:\n{}", table_name, plan.display_indent_schema());

        Ok(Self {
            database_name: "default_db".to_string(),
            table_name,
            table_handle,
            plan,
        })
    }

    pub fn database_name(&self) -> &str {
        &self.database_name
    }

    pub fn table_name(&self) -> &str {
        &self.table_name
    }

    pub fn table_handle(&self) -> &TableHandle {
        &self.table_handle
    }
}

#[async_trait]
impl TableSource for TableSourceAdapter {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.table_handle.schema()
    }

    fn supports_filters_pushdown(&self, filter: &[&Expr]) -> DFResult<Vec<TableProviderFilterPushDown>> {
        self.table_handle.supports_filters_pushdown(filter)
    }

    /// Called by [`InlineTableScan`]
    fn get_logical_plan(&self) -> Option<Cow<LogicalPlan>> {
        Some(Cow::Owned(self.plan.clone()))
    }
}

#[derive(Clone)]
pub enum TableHandle {
    TableProvider(Arc<dyn TableProvider>),
    External(Arc<ListingTable>),
}

impl TableHandle {
    pub fn schema(&self) -> SchemaRef {
        match self {
            Self::External(t) => t.schema(),
            Self::TableProvider(t) => t.schema(),
        }
    }

    pub fn supports_filters_pushdown(&self, filter: &[&Expr]) -> DFResult<Vec<TableProviderFilterPushDown>> {
        match self {
            Self::External(t) => t.supports_filters_pushdown(filter),
            Self::TableProvider(t) => t.supports_filters_pushdown(filter),
        }
    }
}

impl From<Arc<dyn TableProvider>> for TableHandle {
    fn from(value: Arc<dyn TableProvider>) -> Self {
        TableHandle::TableProvider(value)
    }
}

impl From<Arc<ListingTable>> for TableHandle {
    fn from(value: Arc<ListingTable>) -> Self {
        TableHandle::External(value)
    }
}

impl Display for TableHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::External(e) => write!(f, "External({:?})", e.table_paths()),
            Self::TableProvider(_) => write!(f, "TableProvider"),
        }
    }
}
