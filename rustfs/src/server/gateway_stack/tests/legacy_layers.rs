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
//! The gateway stack answers, without any of RustFS's tower patch layers, what those layers make
//! the legacy stack answer (rustfs/backlog#2771, the six layers ruled "replace by the gateway").
//!
//! Responsible for: the fixture the per-layer modules share, a stack assembled by the production
//! builder whose handlers are probes. Nothing in front of [`GatewayStack::call`] runs here, so
//! every answer below is the gateway's alone: the layers are installed by `server/http.rs` and
//! the other harnesses of this crate drive the stack through them.
//! NOT responsible for: S3 behaviour behind the handlers (the probes answer an empty output).
//! Upstream: `server/legacy_compat`, whose unit tests state what each layer answers. Downstream:
//! `GatewayStack::configured_builder`.

use super::*;
use rustfs_gateway::{Handler, HandlerResult, Req, Resp, allow_when, dto};
use std::sync::atomic::{AtomicUsize, Ordering};

mod sigv4_header_guard;

/// A handler for every operation these tests reach. It records that it ran and answers an empty
/// output, so a request that arrives is `200` and one that is refused first never runs it.
pub(super) struct Probe {
    calls: AtomicUsize,
    /// The tag every answer that carries one reports.
    etag: Option<rustfs_gateway_types::ETag>,
}

impl Probe {
    pub(super) fn new() -> Arc<Self> {
        Arc::new(Self {
            calls: AtomicUsize::new(0),
            etag: None,
        })
    }

    pub(super) fn calls(&self) -> usize {
        self.calls.load(Ordering::SeqCst)
    }
}

macro_rules! probe_handlers {
    ($($operation:ident => $output:ident),+ $(,)?) => {$(
        impl Handler<dto::$operation> for Probe {
            async fn call(&self, _request: Req<dto::$operation>) -> HandlerResult<dto::$operation> {
                self.calls.fetch_add(1, Ordering::SeqCst);
                Ok(Resp::new(dto::$output::default()))
            }
        }
    )+};
}

probe_handlers! {
    CopyObject => CopyObjectOutput,
    HeadObject => HeadObjectOutput,
    DeleteObject => DeleteObjectOutput,
    ListBuckets => ListBucketsOutput,
}

impl Handler<dto::PutObject> for Probe {
    async fn call(&self, _request: Req<dto::PutObject>) -> HandlerResult<dto::PutObject> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Ok(Resp::new(dto::PutObjectOutput {
            e_tag: self.etag.clone().unwrap_or_default(),
            ..Default::default()
        }))
    }
}

impl Handler<dto::GetObject> for Probe {
    async fn call(&self, _request: Req<dto::GetObject>) -> HandlerResult<dto::GetObject> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Ok(Resp::new(dto::GetObjectOutput {
            e_tag: self.etag.clone(),
            ..Default::default()
        }))
    }
}

impl Handler<dto::GetObjectAttributes> for Probe {
    async fn call(&self, _request: Req<dto::GetObjectAttributes>) -> HandlerResult<dto::GetObjectAttributes> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Ok(Resp::new(dto::GetObjectAttributesOutput {
            e_tag: self.etag.clone(),
            object_size: Some(7),
            ..Default::default()
        }))
    }
}

/// The production builder, with the early gate replaced by "any authenticated caller" and `probe`
/// registered for the operations above. Everything else, the RustFS profile included, is what
/// `GatewayStack::assemble` configures.
pub(super) fn probe_stack(probe: &Arc<Probe>) -> GatewayStack {
    let builder = GatewayStack::configured_builder(
        &assembly(),
        Arc::new(ScriptedIam { ready: true }),
        root().source(),
        allow_when(|request| request.identity.is_some()),
    )
    .expect("the production builder assembles");
    let service = builder
        .register::<dto::PutObject, _>(Arc::clone(probe))
        .register::<dto::CopyObject, _>(Arc::clone(probe))
        .register::<dto::GetObject, _>(Arc::clone(probe))
        .register::<dto::HeadObject, _>(Arc::clone(probe))
        .register::<dto::DeleteObject, _>(Arc::clone(probe))
        .register::<dto::ListBuckets, _>(Arc::clone(probe))
        .register::<dto::GetObjectAttributes, _>(Arc::clone(probe))
        .build()
        .expect("probe registration");
    GatewayStack {
        service,
        keystone_auth: None,
    }
}

/// What a client reads of an answer.
pub(super) struct Observed {
    pub(super) status: StatusCode,
    pub(super) headers: HeaderMap,
    pub(super) body: String,
}

/// Sends `request` through `stack` and reads the whole answer.
pub(super) async fn observe<B>(stack: &GatewayStack, request: Request<B>) -> Observed
where
    B: http_body::Body + Send + 'static,
    B::Data: Send,
    B::Error: Into<Box<dyn std::error::Error + Send + Sync>>,
{
    let (parts, body) = stack.call(request).await.into_parts();
    let body = body.collect().await.expect("the answer is read").to_bytes();
    Observed {
        status: parts.status,
        headers: parts.headers,
        body: String::from_utf8_lossy(&body).into_owned(),
    }
}
