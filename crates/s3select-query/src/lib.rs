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

pub mod data_source;
pub mod dispatcher;
pub mod execution;
pub mod function;
pub mod instance;
pub mod metadata;
pub mod sql;

use rustfs_s3select_api::{QueryResult, server::dbms::DatabaseManagerSystem};
use s3s::dto::SelectObjectContentInput;
use std::sync::{Arc, LazyLock};

use crate::{
    execution::{factory::SqlQueryExecutionFactory, scheduler::local::LocalScheduler},
    function::simple_func_manager::SimpleFunctionMetadataManager,
    metadata::base_table::BaseTableProvider,
    sql::{optimizer::CascadeOptimizerBuilder, parser::DefaultParser},
};

// Global cached components that can be reused across database instances
struct GlobalComponents {
    func_manager: Arc<SimpleFunctionMetadataManager>,
    parser: Arc<DefaultParser>,
    query_execution_factory: Arc<SqlQueryExecutionFactory>,
    default_table_provider: Arc<BaseTableProvider>,
}

static GLOBAL_COMPONENTS: LazyLock<GlobalComponents> = LazyLock::new(|| {
    let func_manager = Arc::new(SimpleFunctionMetadataManager::default());
    let parser = Arc::new(DefaultParser::default());
    let optimizer = Arc::new(CascadeOptimizerBuilder::default().build());
    let scheduler = Arc::new(LocalScheduler {});
    let query_execution_factory = Arc::new(SqlQueryExecutionFactory::new(optimizer, scheduler));
    let default_table_provider = Arc::new(BaseTableProvider::default());

    GlobalComponents {
        func_manager,
        parser,
        query_execution_factory,
        default_table_provider,
    }
});

/// Get or create database instance with cached components
pub async fn get_global_db(
    input: SelectObjectContentInput,
    enable_debug: bool,
) -> QueryResult<Arc<dyn DatabaseManagerSystem + Send + Sync>> {
    let components = &*GLOBAL_COMPONENTS;
    let db = crate::instance::make_rustfsms_with_components(
        Arc::new(input),
        enable_debug,
        components.func_manager.clone(),
        components.parser.clone(),
        components.query_execution_factory.clone(),
        components.default_table_provider.clone(),
    )
    .await?;

    Ok(Arc::new(db) as Arc<dyn DatabaseManagerSystem + Send + Sync>)
}

/// Create a fresh database instance without using cached components (for testing)
pub async fn create_fresh_db(
    input: SelectObjectContentInput,
    enable_debug: bool,
) -> QueryResult<Arc<dyn DatabaseManagerSystem + Send + Sync>> {
    let db = crate::instance::make_rustfsms(Arc::new(input), enable_debug).await?;
    Ok(Arc::new(db) as Arc<dyn DatabaseManagerSystem + Send + Sync>)
}
