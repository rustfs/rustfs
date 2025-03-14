use std::sync::Arc;

use bytes::Bytes;
use datafusion::{
    execution::{context::SessionState, runtime_env::RuntimeEnvBuilder, SessionStateBuilder},
    prelude::SessionContext,
};
use object_store::{memory::InMemory, path::Path, ObjectStore};
use tracing::error;

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
pub struct SessionCtxFactory {
    pub is_test: bool,
}

impl SessionCtxFactory {
    pub async fn create_session_ctx(&self, context: &Context) -> QueryResult<SessionCtx> {
        let df_session_ctx = self.build_df_session_context(context).await?;

        Ok(SessionCtx {
            _desc: Arc::new(SessionCtxDesc {}),
            inner: df_session_ctx.state(),
        })
    }

    async fn build_df_session_context(&self, context: &Context) -> QueryResult<SessionContext> {
        let path = format!("s3://{}", context.input.bucket);
        let store_url = url::Url::parse(&path).unwrap();
        let rt = RuntimeEnvBuilder::new().build()?;
        let df_session_state = SessionStateBuilder::new()
            .with_runtime_env(Arc::new(rt))
            .with_default_features();

        let df_session_state = if self.is_test {
            let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
            let data = b"id,name,age,department,salary
            1,Alice,25,HR,5000
            2,Bob,30,IT,6000
            3,Charlie,35,Finance,7000
            4,Diana,22,Marketing,4500
            5,Eve,28,IT,5500
            6,Frank,40,Finance,8000
            7,Grace,26,HR,5200
            8,Henry,32,IT,6200
            9,Ivy,24,Marketing,4800
            10,Jack,38,Finance,7500";
            let data_bytes = Bytes::from(data.to_vec());
            let path = Path::from(context.input.key.clone());
            store.put(&path, data_bytes.into()).await.map_err(|e| {
                error!("put data into memory failed: {}", e.to_string());
                QueryError::StoreError { e: e.to_string() }
            })?;

            df_session_state.with_object_store(&store_url, Arc::new(store)).build()
        } else {
            let store =
                EcObjectStore::new(context.input.clone()).map_err(|_| QueryError::NotImplemented { err: String::new() })?;
            df_session_state.with_object_store(&store_url, Arc::new(store)).build()
        };

        let df_session_ctx = SessionContext::new_with_state(df_session_state);

        Ok(df_session_ctx)
    }
}
