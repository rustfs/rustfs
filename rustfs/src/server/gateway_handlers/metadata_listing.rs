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

//! RustFS's two metadata listings on the gateway stack: `GET /{bucket}?list-type=2&metadata=true`
//! and `GET /{bucket}?versions&metadata=true` (rustfs/gateway#1588).
//!
//! Responsible for: [`dialect`], the two rows of the gateway's RustFS dialect that select them
//! ahead of the standard listings, and [`register`], their handler: the shared access phase, the
//! listing authorization and the listing answer both stacks share (`app/metadata_listing.rs`).
//! NOT responsible for: authenticating the caller (the gateway), the listing rules (the shared
//! module), or the assembly (`server/gateway_stack.rs`).
//! Upstream: the gateway's dispatch. Downstream: `app/metadata_listing.rs`.

use super::{access, handler_error};
use crate::app::metadata_listing::{self, MetadataListing};
use crate::runtime_sources::ServerContextSlot;
use rustfs_gateway::{Handler, HandlerResult, Req, Resp, ServiceBuilder};
use rustfs_gateway_core::dialect::{Dialect, DialectError, DialectOverlay};
use rustfs_gateway_dialect_rustfs_admin::native::extensions::s3;
use rustfs_gateway_dialect_rustfs_admin::ops::list_object_versions_with_metadata::{
    self as versions, ListObjectVersionsWithMetadata,
};
use rustfs_gateway_dialect_rustfs_admin::ops::list_objects_v2_with_metadata::{self as objects, ListObjectsV2WithMetadata};
use rustfs_gateway_dialect_rustfs_admin::{AdminResponse, ExtensionOperation};
use rustfs_s3_types::{S3Result, s3_error};
use std::sync::Arc;

/// The two operations, as the gateway names them.
pub(crate) const NAMES: [&str; 2] = [versions::NAME, objects::NAME];

static RECORD: DialectOverlay = DialectOverlay {
    name: "rustfs-metadata-listings",
    vendor: "rustfs",
    operations: s3::ROWS,
    claims: &[],
};

/// The dialect carrying the two listings' rows, as the gateway's RustFS dialect declares them.
///
/// # Errors
///
/// What the gateway refuses in the record or the declarations.
pub(crate) fn dialect() -> Result<Dialect, Vec<DialectError>> {
    s3::declare(Dialect::assemble(&RECORD)).build()
}

/// Registers the handler of both listings on `builder`.
pub(crate) fn register(builder: ServiceBuilder, server_ctx: &Arc<ServerContextSlot>) -> ServiceBuilder {
    let listings = Arc::new(Listings {
        server_ctx: Arc::clone(server_ctx),
    });
    builder
        .register::<ListObjectVersionsWithMetadata, _>(Arc::clone(&listings))
        .register::<ListObjectsV2WithMetadata, _>(listings)
}

/// One metadata listing operation and the listing it is.
trait Listing: ExtensionOperation<Input = ()> {
    const LISTING: MetadataListing;
}

impl Listing for ListObjectVersionsWithMetadata {
    const LISTING: MetadataListing = MetadataListing::ListObjectVersions;
}

impl Listing for ListObjectsV2WithMetadata {
    const LISTING: MetadataListing = MetadataListing::ListObjectsV2;
}

struct Listings {
    server_ctx: Arc<ServerContextSlot>,
}

impl Listings {
    /// The access phase every gateway operation runs, then the listing check and the listing.
    async fn serve<O: Listing>(&self, request: Req<O>) -> S3Result<Resp<O>> {
        let mut envelope = access::admit(&self.server_ctx, request.context()).await?;
        let bucket = request
            .context()
            .bucket()
            .ok_or_else(|| s3_error!(InternalError, "the listing names no bucket"))?
            .as_str()
            .to_owned();
        metadata_listing::authorize(&mut envelope, O::LISTING, &bucket).await?;
        let body = metadata_listing::answer(&envelope, O::LISTING, bucket).await?;
        Ok(Resp::new(AdminResponse::bytes("application/xml", body)))
    }
}

impl<O: Listing> Handler<O> for Listings {
    async fn call(&self, request: Req<O>) -> HandlerResult<O> {
        self.serve(request).await.map_err(handler_error)
    }
}
