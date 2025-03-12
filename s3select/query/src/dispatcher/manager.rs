use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use api::{
    query::{
        dispatcher::QueryDispatcher,
        execution::{Output, QueryStateMachine},
        function::FuncMetaManagerRef,
        logical_planner::Plan,
        parser::Parser,
        Query,
    },
    QueryError, QueryResult,
};
use async_trait::async_trait;
use tokio::task::JoinHandle;

use crate::metadata::TableHandleProviderRef;

#[derive(Clone)]
pub struct SimpleQueryDispatcher {
    // client for default tenant
    default_table_provider: TableHandleProviderRef,
    // memory pool
    // memory_pool: MemoryPoolRef,
    // query tracker
    // parser
    parser: Arc<dyn Parser + Send + Sync>,
    // get query execution factory
    query_execution_factory: QueryExecutionFactoryRef,
    func_manager: FuncMetaManagerRef,

    async_task_joinhandle: Arc<Mutex<HashMap<String, JoinHandle<()>>>>,
    failed_task_joinhandle: Arc<Mutex<HashMap<String, JoinHandle<()>>>>,
}

#[async_trait]
impl QueryDispatcher for SimpleQueryDispatcher {
    async fn start(&self) -> QueryResult<()> {
        self.execute_persister_query(self.coord.node_id()).await
    }

    fn stop(&self) {
        // TODO
    }

    async fn execute_query(&self, query: &Query) -> QueryResult<Output> {
        let query_state_machine = { self.build_query_state_machine(query.clone()).await? };

        let logical_plan = self.build_logical_plan(query_state_machine.clone()).await?;
        let logical_plan = match logical_plan {
            Some(plan) => plan,
            None => return Ok(Output::Nil(())),
        };
        let result = self.execute_logical_plan(logical_plan, query_state_machine).await?;
        Ok(result)
    }

    async fn build_logical_plan(&self, query_state_machine: Arc<QueryStateMachine>) -> QueryResult<Option<Plan>> {
        let session = &query_state_machine.session;
        let query = &query_state_machine.query;

        let scheme_provider = self.build_scheme_provider(session).await?;

        let logical_planner = DefaultLogicalPlanner::new(&scheme_provider);

        let span_recorder = session.get_child_span("parse sql");
        let statements = self.parser.parse(query.content())?;

        // not allow multi statement
        if statements.len() > 1 {
            return Err(QueryError::MultiStatement {
                num: statements.len(),
                sql: query_state_machine.query.content().to_string(),
            });
        }

        let stmt = match statements.front() {
            Some(stmt) => stmt.clone(),
            None => return Ok(None),
        };

        drop(span_recorder);

        let logical_plan = self
            .statement_to_logical_plan(stmt, &logical_planner, query_state_machine)
            .await?;
        Ok(Some(logical_plan))
    }

    async fn execute_logical_plan(&self, logical_plan: Plan, query_state_machine: Arc<QueryStateMachine>) -> QueryResult<Output> {
        self.execute_logical_plan(logical_plan, query_state_machine).await
    }

    async fn build_query_state_machine(&self, query: Query) -> QueryResult<Arc<QueryStateMachine>> {
        let session = self
            .session_factory
            .create_session_ctx(query.context(), self.memory_pool.clone(), self.coord.clone())?;

        let query_state_machine = Arc::new(QueryStateMachine::begin(query, session));
        Ok(query_state_machine)
    }
}

impl SimpleQueryDispatcher {
    async fn statement_to_logical_plan<S: ContextProviderExtension + Send + Sync>(
        &self,
        stmt: ExtStatement,
        logical_planner: &DefaultLogicalPlanner<'_, S>,
        query_state_machine: Arc<QueryStateMachine>,
    ) -> QueryResult<Plan> {
        // begin analyze
        query_state_machine.begin_analyze();
        let logical_plan = logical_planner
            .create_logical_plan(stmt, &query_state_machine.session, self.coord.get_config().query.auth_enabled)
            .await?;
        query_state_machine.end_analyze();

        Ok(logical_plan)
    }

    async fn execute_logical_plan(&self, logical_plan: Plan, query_state_machine: Arc<QueryStateMachine>) -> QueryResult<Output> {
        let execution = self
            .query_execution_factory
            .create_query_execution(logical_plan, query_state_machine.clone())
            .await?;

        // TrackedQuery.drop() is called implicitly when the value goes out of scope,
        self.query_tracker
            .try_track_query(query_state_machine.query_id, execution)
            .await?
            .start()
            .await
    }

    async fn build_scheme_provider(&self, session: &SessionCtx) -> QueryResult<MetadataProvider> {
        let meta_client = self.build_current_session_meta_client(session).await?;
        let current_session_table_provider = self.build_table_handle_provider(meta_client.clone())?;
        let metadata_provider = MetadataProvider::new(
            self.coord.clone(),
            meta_client,
            current_session_table_provider,
            self.default_table_provider.clone(),
            self.func_manager.clone(),
            self.query_tracker.clone(),
            session.clone(),
        );

        Ok(metadata_provider)
    }

    async fn build_current_session_meta_client(&self, session: &SessionCtx) -> QueryResult<MetaClientRef> {
        let meta_client = self
            .coord
            .tenant_meta(session.tenant())
            .await
            .ok_or_else(|| MetaError::TenantNotFound {
                tenant: session.tenant().to_string(),
            })
            .context(MetaSnafu)?;

        Ok(meta_client)
    }

    fn build_table_handle_provider(&self, meta_client: MetaClientRef) -> QueryResult<TableHandleProviderRef> {
        let current_session_table_provider: Arc<BaseTableProvider> = Arc::new(BaseTableProvider::new(
            self.coord.clone(),
            self.split_manager.clone(),
            meta_client,
            self.stream_provider_manager.clone(),
        ));

        Ok(current_session_table_provider)
    }
}

#[derive(Default, Clone)]
pub struct SimpleQueryDispatcherBuilder {
    coord: Option<CoordinatorRef>,
    default_table_provider: Option<TableHandleProviderRef>,
    split_manager: Option<SplitManagerRef>,
    session_factory: Option<Arc<SessionCtxFactory>>,
    parser: Option<Arc<dyn Parser + Send + Sync>>,

    query_execution_factory: Option<QueryExecutionFactoryRef>,
    query_tracker: Option<Arc<QueryTracker>>,
    memory_pool: Option<MemoryPoolRef>, // memory

    func_manager: Option<FuncMetaManagerRef>,
    stream_provider_manager: Option<StreamProviderManagerRef>,
    span_ctx: Option<SpanContext>,
    auth_cache: Option<Arc<AuthCache<AuthCacheKey, User>>>,
}

impl SimpleQueryDispatcherBuilder {
    pub fn with_coord(mut self, coord: CoordinatorRef) -> Self {
        self.coord = Some(coord);
        self
    }

    pub fn with_default_table_provider(mut self, default_table_provider: TableHandleProviderRef) -> Self {
        self.default_table_provider = Some(default_table_provider);
        self
    }

    pub fn with_session_factory(mut self, session_factory: Arc<SessionCtxFactory>) -> Self {
        self.session_factory = Some(session_factory);
        self
    }

    pub fn with_split_manager(mut self, split_manager: SplitManagerRef) -> Self {
        self.split_manager = Some(split_manager);
        self
    }

    pub fn with_parser(mut self, parser: Arc<dyn Parser + Send + Sync>) -> Self {
        self.parser = Some(parser);
        self
    }

    pub fn with_query_execution_factory(mut self, query_execution_factory: QueryExecutionFactoryRef) -> Self {
        self.query_execution_factory = Some(query_execution_factory);
        self
    }

    pub fn with_query_tracker(mut self, query_tracker: Arc<QueryTracker>) -> Self {
        self.query_tracker = Some(query_tracker);
        self
    }

    pub fn with_memory_pool(mut self, memory_pool: MemoryPoolRef) -> Self {
        self.memory_pool = Some(memory_pool);
        self
    }

    pub fn with_func_manager(mut self, func_manager: FuncMetaManagerRef) -> Self {
        self.func_manager = Some(func_manager);
        self
    }

    pub fn with_stream_provider_manager(mut self, stream_provider_manager: StreamProviderManagerRef) -> Self {
        self.stream_provider_manager = Some(stream_provider_manager);
        self
    }

    pub fn with_span_ctx(mut self, span_ctx: Option<SpanContext>) -> Self {
        self.span_ctx = span_ctx;
        self
    }

    pub fn with_auth_cache(mut self, auth_cache: Arc<AuthCache<AuthCacheKey, User>>) -> Self {
        self.auth_cache = Some(auth_cache);
        self
    }

    pub fn build(self) -> QueryResult<Arc<SimpleQueryDispatcher>> {
        let coord = self.coord.ok_or_else(|| QueryError::BuildQueryDispatcher {
            err: "lost of coord".to_string(),
        })?;

        let split_manager = self.split_manager.ok_or_else(|| QueryError::BuildQueryDispatcher {
            err: "lost of split manager".to_string(),
        })?;

        let session_factory = self.session_factory.ok_or_else(|| QueryError::BuildQueryDispatcher {
            err: "lost of session_factory".to_string(),
        })?;

        let parser = self.parser.ok_or_else(|| QueryError::BuildQueryDispatcher {
            err: "lost of parser".to_string(),
        })?;

        let query_execution_factory = self.query_execution_factory.ok_or_else(|| QueryError::BuildQueryDispatcher {
            err: "lost of query_execution_factory".to_string(),
        })?;

        let query_tracker = self.query_tracker.ok_or_else(|| QueryError::BuildQueryDispatcher {
            err: "lost of query_tracker".to_string(),
        })?;

        let func_manager = self.func_manager.ok_or_else(|| QueryError::BuildQueryDispatcher {
            err: "lost of func_manager".to_string(),
        })?;

        let stream_provider_manager = self.stream_provider_manager.ok_or_else(|| QueryError::BuildQueryDispatcher {
            err: "lost of stream_provider_manager".to_string(),
        })?;

        let memory_pool = self.memory_pool.ok_or_else(|| QueryError::BuildQueryDispatcher {
            err: "lost of memory pool".to_string(),
        })?;

        let default_table_provider = self.default_table_provider.ok_or_else(|| QueryError::BuildQueryDispatcher {
            err: "lost of default_table_provider".to_string(),
        })?;

        let span_ctx = self.span_ctx;

        let auth_cache = self.auth_cache.ok_or_else(|| QueryError::BuildQueryDispatcher {
            err: "lost of auth_cache".to_string(),
        })?;

        let dispatcher = Arc::new(SimpleQueryDispatcher {
            coord,
            default_table_provider,
            split_manager,
            session_factory,
            memory_pool,
            parser,
            query_execution_factory,
            query_tracker,
            func_manager,
            stream_provider_manager,
            span_ctx,
            async_task_joinhandle: Arc::new(Mutex::new(HashMap::new())),
            failed_task_joinhandle: Arc::new(Mutex::new(HashMap::new())),
            auth_cache,
        });

        let meta_task_receiver = dispatcher
            .coord
            .meta_manager()
            .take_resourceinfo_rx()
            .expect("meta resource channel only has one consumer");
        tokio::spawn(SimpleQueryDispatcher::recv_meta_modify(dispatcher.clone(), meta_task_receiver));

        Ok(dispatcher)
    }
}
