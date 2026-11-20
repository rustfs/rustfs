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
use rustfs_gateway_core::dialect::{Dialect, DialectError, DialectOverlay, DialectRoute, OverlayRow};
use rustfs_gateway_core::route::ShadowingDecl;
use rustfs_gateway_dialect_rustfs_admin::ops::list_object_versions_with_metadata::{
    self as versions, ListObjectVersionsWithMetadata,
};
use rustfs_gateway_dialect_rustfs_admin::ops::list_objects_v2_with_metadata::{self as objects, ListObjectsV2WithMetadata};
use rustfs_gateway_dialect_rustfs_admin::{AdminResponse, ExtensionOperation};
use rustfs_s3_types::{S3Result, s3_error};
use std::sync::{Arc, OnceLock};

/// The two operations, as the gateway names them.
pub(crate) const NAMES: [&str; 2] = [versions::NAME, objects::NAME];

static ROWS: [OverlayRow; 2] = [versions::OVERLAY_ROW, objects::OVERLAY_ROW];

static RECORD: DialectOverlay = DialectOverlay {
    name: "rustfs-metadata-listings",
    vendor: "rustfs",
    operations: &ROWS,
    claims: &[],
};

/// The dialect carrying the two listings' rows, as the gateway's RustFS dialect declares them.
///
/// # Errors
///
/// What the gateway refuses in the record or the declarations.
pub(crate) fn dialect() -> Result<Dialect, Vec<DialectError>> {
    static VERSIONS: OnceLock<Vec<ShadowingDecl>> = OnceLock::new();
    static OBJECTS: OnceLock<Vec<ShadowingDecl>> = OnceLock::new();
    Dialect::assemble(&RECORD)
        .declare::<ListObjectVersionsWithMetadata>(on_the_s3_table(ListObjectVersionsWithMetadata::ROUTE, &VERSIONS))
        .declare::<ListObjectsV2WithMetadata>(on_the_s3_table(ListObjectsV2WithMetadata::ROUTE, &OBJECTS))
        .build()
}

/// `route` as the S3 table carries it: the same row, declaring the overlaps with the standard
/// operations and the other listing it stands in front of. The admin router's extension rows it
/// also shadows in the
/// gateway's one-table dialect are served by the control-plane host, not by this table, and a
/// declaration naming an operation the table does not hold is refused as stale.
fn on_the_s3_table(route: DialectRoute, shadows: &'static OnceLock<Vec<ShadowingDecl>>) -> DialectRoute {
    let shadows = shadows.get_or_init(|| {
        route
            .shadows
            .iter()
            .filter(|decl| !decl.shadowed.starts_with("rustfs:") || NAMES.contains(&decl.shadowed))
            .copied()
            .collect()
    });
    DialectRoute { shadows, ..route }
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
