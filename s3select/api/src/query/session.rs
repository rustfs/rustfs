use std::sync::Arc;

use datafusion::{
    execution::{context::SessionState, runtime_env::RuntimeEnvBuilder, SessionStateBuilder},
    prelude::SessionContext,
};

use crate::{object_store::EcObjectStore, QueryError, QueryResult};

use super::Context;

#[derive(Clone)]
pub struct SessionCtx {
    _desc: Arc<SessionCtxDesc>,
    inner: SessionState,
}

impl SessionCtx {
    pub fn inner(&self) -> &SessionState {
        &self.inner
    }
}

#[derive(Clone)]
pub struct SessionCtxDesc {
    // maybe we need some info
}

#[derive(Default)]
pub struct SessionCtxFactory {}

impl SessionCtxFactory {
    pub fn create_session_ctx(&self, context: &Context) -> QueryResult<SessionCtx> {
        let df_session_ctx = self.build_df_session_context(context)?;

        Ok(SessionCtx {
            _desc: Arc::new(SessionCtxDesc {}),
            inner: df_session_ctx.state(),
        })
    }

    fn build_df_session_context(&self, context: &Context) -> QueryResult<SessionContext> {
        let path = format!("s3://{}", context.input.bucket);
        let store_url = url::Url::parse(&path).unwrap();
        let store = EcObjectStore::new(context.input.clone()).map_err(|_| QueryError::NotImplemented { err: String::new() })?;

        let rt = RuntimeEnvBuilder::new().build()?;
        let df_session_state = SessionStateBuilder::new()
            .with_runtime_env(Arc::new(rt))
            .with_object_store(&store_url, Arc::new(store))
            .with_default_features()
            .build();
        let df_session_ctx = SessionContext::new_with_state(df_session_state);

        Ok(df_session_ctx)
    }
}
