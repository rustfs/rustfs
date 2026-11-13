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

//! The S3 operations the gateway stack serves, and the one way an operation is served
//! (rustfs/backlog#2734).
//!
//! Responsible for: [`Served`], what one operation supplies; the only `impl Handler` in RustFS,
//! which runs every request through the same sequence; and [`OPERATIONS`], the table that both
//! registers the handlers and is what the gateway's early gate consults. An operation without a
//! row has no handler, and the gateway answers it `501 NotImplemented`.
//! NOT responsible for: assembling the service (`server/gateway_stack.rs`), authenticating or
//! routing a request (the gateway), or any S3 behaviour (the use cases in `crate::app`).
//! Upstream: the gateway's dispatch. Downstream: `crate::app` use cases.
//!
//! An operation is one `impl Served` and one row of `ROWS` in the family file named after its
//! migration batch, the stem `storage/legacy_edge/` uses for the same operations.

mod access;
mod envelope;
mod error;

mod bucket_access;
mod bucket_configs;
mod bucket_core;
mod bucket_replication_lifecycle;
mod listing_multipart;
mod object_commit;
mod object_meta;
mod object_misc;
mod object_read;
mod object_write;

#[cfg(test)]
pub(crate) mod test_support;

use self::error::handler_error;
use crate::app::RequestEnvelope;
use crate::runtime_sources::{AppContext, ServerContextSlot};
use rustfs_gateway::{Handler, HandlerResult, OperationCodec, Req, Resp, ServiceBuilder};
use rustfs_io_metrics::record_s3_op;
use rustfs_s3_ops::S3Operation;
use rustfs_s3_types::S3Result;
use std::future::Future;
use std::sync::Arc;

/// The backend every gateway operation is served by: the server whose use cases answer.
pub(crate) struct GatewayBackend {
    server_ctx: Arc<ServerContextSlot>,
}

impl GatewayBackend {
    pub(crate) fn new(server_ctx: Arc<ServerContextSlot>) -> Self {
        Self { server_ctx }
    }

    /// The fixed sequence: the shared access phase, the operation's own authorization, the count,
    /// the use case. Nothing is skipped and nothing is reordered per operation.
    async fn serve<O: Served>(&self, request: Req<O>) -> S3Result<Resp<O>> {
        let mut envelope = access::admit(&self.server_ctx, request.context()).await?;
        O::authorize(&mut envelope, &request).await?;
        record_s3_op(O::METRIC);
        O::execute(self.server_ctx.app_context(), envelope, request).await
    }
}

/// One S3 operation the gateway stack serves.
pub(crate) trait Served: OperationCodec + Sized {
    /// The label the S3 request counter counts the operation under, the legacy stack's.
    const METRIC: S3Operation;

    /// The operation's own authorization, after the shared access phase installed the caller: the
    /// owned form of its legacy access hook, calling the function that hook calls. There is no
    /// default, so an operation cannot be served without stating what it authorizes.
    fn authorize(envelope: &mut RequestEnvelope, request: &Req<Self>) -> impl Future<Output = S3Result<()>> + Send;

    /// The use case, with the envelope the access phase completed.
    fn execute(
        context: Option<Arc<AppContext>>,
        envelope: RequestEnvelope,
        request: Req<Self>,
    ) -> impl Future<Output = S3Result<Resp<Self>>> + Send;
}

impl<O: Served> Handler<O> for GatewayBackend {
    async fn call(&self, request: Req<O>) -> HandlerResult<O> {
        self.serve(request).await.map_err(handler_error)
    }
}

/// One served operation: its name and the registration of its handler.
pub(crate) struct OpRow {
    pub(crate) name: &'static str,
    register: fn(ServiceBuilder, &Arc<GatewayBackend>) -> ServiceBuilder,
}

const fn row<O: Served>() -> OpRow {
    OpRow {
        name: O::NAME,
        register: register_one::<O>,
    }
}

fn register_one<O: Served>(builder: ServiceBuilder, backend: &Arc<GatewayBackend>) -> ServiceBuilder {
    builder.register::<O, _>(Arc::clone(backend))
}

/// A table of served operations, one slice per family.
pub(crate) type OperationTable = &'static [&'static [OpRow]];

/// Every operation the gateway stack serves, in the order of the migration batches. A batch
/// appends to its own family's `ROWS`; this list does not change.
pub(crate) static OPERATIONS: OperationTable = &[
    bucket_core::ROWS,
    bucket_configs::ROWS,
    bucket_access::ROWS,
    bucket_replication_lifecycle::ROWS,
    object_meta::ROWS,
    listing_multipart::ROWS,
    object_read::ROWS,
    object_write::ROWS,
    object_commit::ROWS,
    object_misc::ROWS,
];

/// The rows of `table`.
pub(crate) fn rows(table: OperationTable) -> impl Iterator<Item = &'static OpRow> {
    table.iter().flat_map(|family| family.iter())
}

/// Whether `table` serves `operation`.
pub(crate) fn serves(table: OperationTable, operation: &str) -> bool {
    rows(table).any(|row| row.name == operation)
}

/// Registers every operation of `table` on `backend`.
pub(crate) fn register(table: OperationTable, builder: ServiceBuilder, backend: &Arc<GatewayBackend>) -> ServiceBuilder {
    rows(table).fold(builder, |builder, row| (row.register)(builder, backend))
}

#[cfg(test)]
mod tests {
    use super::*;
    use rustfs_gateway::OperationSet;
    use std::collections::BTreeSet;

    /// Every operation is served by exactly one row, and the gateway knows each one; every other
    /// operation the gateway knows has no row, so the early gate refuses it and dispatch has no
    /// handler for it.
    #[test]
    fn every_operation_the_gateway_knows_is_served_by_one_row_or_by_none() {
        let names: Vec<&str> = rows(OPERATIONS).map(|row| row.name).collect();
        let served: BTreeSet<&str> = names.iter().copied().collect();
        let known: BTreeSet<&str> = OperationSet::aws_full().names().collect();

        assert_eq!(names.len(), served.len(), "an operation has two rows: {names:?}");
        assert!(!served.is_empty(), "the table serves nothing");
        assert!(served.is_subset(&known), "a row names an unknown operation: {:?}", &served - &known);
        for operation in &known {
            assert_eq!(serves(OPERATIONS, operation), served.contains(operation), "{operation}");
        }
        assert!(
            known.iter().any(|operation| !serves(OPERATIONS, operation)),
            "the probe needs an unserved operation"
        );
    }

    #[test]
    fn registration_registers_exactly_the_rows() {
        let backend = Arc::new(GatewayBackend::new(ServerContextSlot::new()));

        let registered: BTreeSet<&str> = register(OPERATIONS, ServiceBuilder::new(), &backend).registered().collect();

        assert_eq!(registered, rows(OPERATIONS).map(|row| row.name).collect());
    }

    /// An invalid header value does not alter the response status or other headers.
    #[test]
    fn a_stored_tag_a_header_cannot_hold_is_left_out_and_the_answer_stands() {
        for stored in ["a\rb", "a\nb", "a\0b"] {
            let etag = crate::app::dto_values::stored_etag(stored);
            let rendered = etag.render(rustfs_gateway_types::EtagRender::HeaderQuoted);
            let mut response = rustfs_gateway::EncodedResponse::of(200);
            response.set_header("etag", &rendered);
            response.set_header("x-amz-version-id", "null");

            assert_eq!(response.status, http::StatusCode::OK, "{stored:?}");
            assert!(response.headers.get(http::header::ETAG).is_none(), "{stored:?}");
            assert_eq!(response.headers.get("x-amz-version-id").map(|value| value.as_bytes()), Some(&b"null"[..]));
        }
    }
}
