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

use async_trait::async_trait;
use datafusion::arrow::datatypes::DataType;
use datafusion::common::Result as DFResult;
use datafusion::datasource::listing::ListingTable;
use datafusion::logical_expr::var_provider::is_system_variables;
use datafusion::logical_expr::{AggregateUDF, ScalarUDF, TableSource, WindowUDF};
use datafusion::variable::VarType;
use datafusion::{
    config::ConfigOptions,
    sql::{TableReference, planner::ContextProvider},
};
use rustfs_s3select_api::query::{function::FuncMetaManagerRef, session::SessionCtx};

use crate::data_source::table_source::{TableHandle, TableSourceAdapter};

pub mod base_table;

#[async_trait]
pub trait ContextProviderExtension: ContextProvider {
    fn get_table_source_(&self, name: TableReference) -> datafusion::common::Result<Arc<TableSourceAdapter>>;
}

pub type TableHandleProviderRef = Arc<dyn TableHandleProvider + Send + Sync>;

pub trait TableHandleProvider {
    fn build_table_handle(&self, provider: Arc<ListingTable>) -> DFResult<TableHandle>;
}

pub struct MetadataProvider {
    provider: Arc<ListingTable>,
    session: SessionCtx,
    config_options: ConfigOptions,
    func_manager: FuncMetaManagerRef,
    current_session_table_provider: TableHandleProviderRef,
}

impl MetadataProvider {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        provider: Arc<ListingTable>,
        current_session_table_provider: TableHandleProviderRef,
        func_manager: FuncMetaManagerRef,
        session: SessionCtx,
    ) -> Self {
        Self {
            provider,
            current_session_table_provider,
            config_options: session.inner().config_options().as_ref().clone(),
            session,
            func_manager,
        }
    }

    fn build_table_handle(&self) -> datafusion::common::Result<TableHandle> {
        self.current_session_table_provider.build_table_handle(self.provider.clone())
    }
}

impl ContextProviderExtension for MetadataProvider {
    fn get_table_source_(&self, table_ref: TableReference) -> datafusion::common::Result<Arc<TableSourceAdapter>> {
        let name = table_ref.clone().resolve("", "");
        let table_name = &*name.table;

        let table_handle = self.build_table_handle()?;

        Ok(Arc::new(TableSourceAdapter::try_new(table_ref.clone(), table_name, table_handle)?))
    }
}

impl ContextProvider for MetadataProvider {
    fn get_function_meta(&self, name: &str) -> Option<Arc<ScalarUDF>> {
        self.func_manager
            .udf(name)
            .ok()
            .or(self.session.inner().scalar_functions().get(name).cloned())
    }

    fn get_aggregate_meta(&self, name: &str) -> Option<Arc<AggregateUDF>> {
        self.func_manager.udaf(name).ok()
    }

    fn get_variable_type(&self, variable_names: &[String]) -> Option<DataType> {
        if variable_names.is_empty() {
            return None;
        }

        let var_type = if is_system_variables(variable_names) {
            VarType::System
        } else {
            VarType::UserDefined
        };

        self.session
            .inner()
            .execution_props()
            .get_var_provider(var_type)
            .and_then(|p| p.get_type(variable_names))
    }

    fn options(&self) -> &ConfigOptions {
        // TODO refactor
        &self.config_options
    }

    fn get_window_meta(&self, name: &str) -> Option<Arc<WindowUDF>> {
        self.func_manager.udwf(name).ok()
    }

    fn get_table_source(&self, name: TableReference) -> DFResult<Arc<dyn TableSource>> {
        Ok(self.get_table_source_(name)?)
    }

    fn udf_names(&self) -> Vec<String> {
        self.func_manager.udfs()
    }

    fn udaf_names(&self) -> Vec<String> {
        self.func_manager.udafs()
    }

    fn udwf_names(&self) -> Vec<String> {
        self.func_manager.udwfs()
    }
}
