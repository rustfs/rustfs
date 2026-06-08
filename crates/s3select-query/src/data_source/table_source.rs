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

use std::borrow::Cow;
use std::fmt::Display;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::Result as DFResult;
use datafusion::datasource::{TableProvider, provider_as_source};
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
        let table_source = provider_as_source(table_handle.provider());
        let plan = if let Some(plan) = table_source.get_logical_plan() {
            LogicalPlanBuilder::from(plan.into_owned()).build()?
        } else {
            LogicalPlanBuilder::scan(table_ref, table_source, None)?.build()?
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
    fn schema(&self) -> SchemaRef {
        self.table_handle.schema()
    }

    fn supports_filters_pushdown(&self, filter: &[&Expr]) -> DFResult<Vec<TableProviderFilterPushDown>> {
        self.table_handle.supports_filters_pushdown(filter)
    }

    /// Called by [`InlineTableScan`]
    fn get_logical_plan(&self) -> Option<Cow<'_, LogicalPlan>> {
        Some(Cow::Owned(self.plan.clone()))
    }
}

#[derive(Clone)]
pub struct TableHandle(Arc<dyn TableProvider>);

impl TableHandle {
    fn provider(&self) -> Arc<dyn TableProvider> {
        Arc::clone(&self.0)
    }

    pub fn schema(&self) -> SchemaRef {
        self.0.schema()
    }

    pub fn supports_filters_pushdown(&self, filter: &[&Expr]) -> DFResult<Vec<TableProviderFilterPushDown>> {
        self.0.supports_filters_pushdown(filter)
    }
}

impl From<Arc<dyn TableProvider>> for TableHandle {
    fn from(value: Arc<dyn TableProvider>) -> Self {
        Self(value)
    }
}

impl Display for TableHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let provider_name = std::any::type_name_of_val(self.0.as_ref());
        let short_name = provider_name.rsplit("::").next().unwrap_or(provider_name);
        f.write_str(short_name)
    }
}
