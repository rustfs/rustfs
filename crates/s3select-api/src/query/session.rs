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

use crate::query::Context;
use crate::{QueryError, QueryResult, object_store::EcObjectStore};
use datafusion::{
    execution::{SessionStateBuilder, context::SessionState, runtime_env::RuntimeEnvBuilder},
    parquet::data_type::AsBytes,
    prelude::SessionContext,
};
use object_store::{ObjectStore, memory::InMemory, path::Path};
use std::sync::Arc;
use tracing::error;

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
            let data_bytes = data.as_bytes();
            // let data = r#""year"╦"gender"╦"ethnicity"╦"firstname"╦"count"╦"rank"
            // "2011"╦"FEMALE"╦"ASIAN AND PACIFIC ISLANDER"╦"SOPHIA"╦"119"╦"1"
            // "2011"╦"FEMALE"╦"ASIAN AND PACIFIC ISLANDER"╦"CHLOE"╦"106"╦"2"
            // "2011"╦"FEMALE"╦"ASIAN AND PACIFIC ISLANDER"╦"EMILY"╦"93"╦"3"
            // "2011"╦"FEMALE"╦"ASIAN AND PACIFIC ISLANDER"╦"OLIVIA"╦"89"╦"4"
            // "2011"╦"FEMALE"╦"ASIAN AND PACIFIC ISLANDER"╦"EMMA"╦"75"╦"5"
            // "2011"╦"FEMALE"╦"ASIAN AND PACIFIC ISLANDER"╦"ISABELLA"╦"67"╦"6"
            // "2011"╦"FEMALE"╦"ASIAN AND PACIFIC ISLANDER"╦"TIFFANY"╦"54"╦"7"
            // "2011"╦"FEMALE"╦"ASIAN AND PACIFIC ISLANDER"╦"ASHLEY"╦"52"╦"8"
            // "2011"╦"FEMALE"╦"ASIAN AND PACIFIC ISLANDER"╦"FIONA"╦"48"╦"9"
            // "2011"╦"FEMALE"╦"ASIAN AND PACIFIC ISLANDER"╦"ANGELA"╦"47"╦"10""#;
            // let data_bytes = Bytes::from(data);
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
