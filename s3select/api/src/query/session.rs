use std::sync::Arc;

use datafusion::execution::context::SessionState;

#[derive(Clone)]
pub struct SessionCtx {
    desc: Arc<SessionCtxDesc>,
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
