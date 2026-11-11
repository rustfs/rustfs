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

//! The S3 operations the gateway stack serves, one file per operation (rustfs/backlog#2734).
//!
//! Responsible for: [`GatewayBackend`], the one backend every operation is implemented on, and
//! [`register`], the explicit list of what is registered. An operation that is not in that list
//! has no handler, and the gateway answers it `501 NotImplemented`.
//! NOT responsible for: assembling the service (`server/gateway_stack.rs`), authenticating or
//! routing a request (the gateway), or any S3 behaviour: a handler converts and delegates, and
//! the RustFS use case it calls is the one the legacy stack calls.
//! Upstream: the gateway's dispatch. Downstream: `crate::app` use cases.
//!
//! # The shape every handler has
//!
//! `<family>/<snake_op>.rs` holds exactly one `impl Handler<Op> for GatewayBackend`, in four
//! steps and nothing else:
//!
//! 1. build the [`RequestEnvelope`](crate::app::RequestEnvelope) with `from_gateway`;
//! 2. run the access phase: [`access::check`], then the operation's own access hook, which is
//!    the authoritative authorization and runs exactly where the legacy stack runs it;
//! 3. call the use case with the envelope and the gateway's own input type;
//! 4. map a refusal with [`error::handler_error`], the only place an error is converted.
//!
//! The gateway's `Authorizer` (`server/gateway_stack/authorizer.rs`) is an earlier gate in front
//! of step 2 and never a replacement for it.

pub(crate) mod access;
mod bucket;
mod envelope;
pub(crate) mod error;

use crate::runtime_sources::ServerContextSlot;
use rustfs_gateway::{ServiceBuilder, dto};
use std::sync::Arc;

/// The backend every gateway operation is implemented on: the server whose use cases answer.
pub(crate) struct GatewayBackend {
    server_ctx: Arc<ServerContextSlot>,
}

impl GatewayBackend {
    pub(crate) fn new(server_ctx: Arc<ServerContextSlot>) -> Self {
        Self { server_ctx }
    }

    pub(crate) fn server_ctx(&self) -> &Arc<ServerContextSlot> {
        &self.server_ctx
    }
}

/// Registers every operation the gateway stack serves. Each slice adds one line here and one row
/// to the authorization table; the census in `server/gateway_stack/authorizer.rs` holds the two
/// lists equal.
pub(crate) fn register(builder: ServiceBuilder, backend: &Arc<GatewayBackend>) -> ServiceBuilder {
    builder.register::<dto::ListBuckets, _>(Arc::clone(backend))
}
