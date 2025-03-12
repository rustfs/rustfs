use std::sync::Arc;

use api::query::{function::FuncMetaManagerRef, session::SessionCtx};
use api::ResolvedTable;
use async_trait::async_trait;
use datafusion::arrow::datatypes::DataType;
use datafusion::common::Result as DFResult;
use datafusion::error::DataFusionError;
use datafusion::execution::SessionState;
use datafusion::logical_expr::var_provider::is_system_variables;
use datafusion::logical_expr::{AggregateUDF, ScalarUDF, TableSource, WindowUDF};
use datafusion::variable::VarType;
use datafusion::{
    config::ConfigOptions,
    sql::{planner::ContextProvider, TableReference},
};

use crate::data_source::data_source::{TableHandle, TableSourceAdapter};

pub mod base_table;

#[async_trait]
pub trait ContextProviderExtension: ContextProvider {
    fn get_table_source(&self, name: TableReference) -> datafusion::common::Result<Arc<TableSourceAdapter>>;
}

pub type TableHandleProviderRef = Arc<dyn TableHandleProvider + Send + Sync>;

pub trait TableHandleProvider {
    fn build_table_handle(&self, session_state: &SessionState, table_name: &str) -> DFResult<TableHandle>;
}

pub struct MetadataProvider {
    session: SessionCtx,
    config_options: ConfigOptions,
    func_manager: FuncMetaManagerRef,
    current_session_table_provider: TableHandleProviderRef,
}

impl MetadataProvider {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        current_session_table_provider: TableHandleProviderRef,
        func_manager: FuncMetaManagerRef,
        session: SessionCtx,
    ) -> Self {
        Self {
            current_session_table_provider,
            config_options: session.inner().config_options().clone(),
            session,
            func_manager,
        }
    }

    fn build_table_handle(&self, name: &ResolvedTable) -> datafusion::common::Result<TableHandle> {
        let table_name = name.table();

        self.current_session_table_provider.build_table_handle(table_name)
    }
}

#[async_trait::async_trait]
impl ContextProviderExtension for MetadataProvider {
    fn get_table_source(&self, table_ref: TableReference) -> datafusion::common::Result<Arc<TableSourceAdapter>> {
        let name = table_ref
            .clone()
            .resolve_object(self.session.tenant(), self.session.default_database())?;

        let table_name = name.table();
        let database_name = name.database();
        let tenant_name = name.tenant();

        // Cannot query across tenants
        if self.session.tenant() != tenant_name {
            return Err(DataFusionError::Plan(format!(
                "Tenant conflict, the current connection's tenant is {}",
                self.session.tenant()
            )));
        }

        // save access table
        self.access_databases.write().push_table(database_name, table_name);

        let table_handle = self.build_table_handle(&name)?;

        Ok(Arc::new(TableSourceAdapter::try_new(
            table_ref.to_owned_reference(),
            database_name,
            table_name,
            table_handle,
        )?))
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
        Ok(self.get_table_source(name)?)
    }

    fn udf_names(&self) -> Vec<String> {
        todo!()
    }

    fn udaf_names(&self) -> Vec<String> {
        todo!()
    }

    fn udwf_names(&self) -> Vec<String> {
        todo!()
    }
}
