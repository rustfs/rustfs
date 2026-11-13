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

//! Authorization shapes that only migration batch B2 (rustfs/backlog#2765) uses: object listings, multipart listings, and multipart create and abort.
//!
//! Responsible for: the owned authorization function of each such operation, generic over
//! `AuthorizationRequest`, so the legacy `S3Access` hook and `Served::authorize` call the same code.
//! Not responsible for: shapes more than one batch shares (those stay in `storage/access.rs`), the
//! policy evaluation itself (`authorize_request`), or the operation's use case.
//! Upstream: the legacy hook in `storage/access.rs` and the batch's `server/gateway_handlers/listing_multipart.rs`.
//! Downstream: the shared shapes in `storage/access.rs`.

// The table snapshot reads the access module's private table-catalog state.
use super::super::*;
use rustfs_s3_types::S3Result;
use rustfs_utils::http::SUFFIX_INCLUDE_DELETED;

const TABLE_DATA_PLANE_LIST_CURSOR_KEY_CONTEXT: &[u8] = b"rustfs-table-list-cursor-key:v1";

/// The authorization of an object listing (ListObjects, ListObjectsV2, ListObjectVersions and the
/// metadata listings): `action` on `bucket`; `ListBucketVersions` as well when a `ListBucket`
/// listing asks for delete markers through `include-deleted: true`; then the snapshot of the
/// tables of a table bucket the caller may list, which the use case pages through.
///
/// The repeated-query refusal (`InvalidRequest`) and the anonymous `ListBucket` fallback for
/// `ListBucketVersions` are part of the policy decision itself (`authorize_request`).
///
/// # Errors
///
/// `InternalError` when the access phase installed no `ReqInfo`; otherwise what the policy
/// decision or the table catalog refuses.
pub(crate) async fn authorize_bucket_listing<R: AuthorizationRequest + ?Sized>(
    req: &mut R,
    bucket: &str,
    action: S3Action,
) -> S3Result<()> {
    authorize_bucket_request(req, bucket, Action::S3Action(action)).await?;
    if action == S3Action::ListBucketAction && get_header(req.headers(), SUFFIX_INCLUDE_DELETED).as_deref() == Some("true") {
        authorize_request(req, Action::S3Action(S3Action::ListBucketVersionsAction)).await?;
    }
    Ok(prepare_table_data_plane_list_access(req, bucket, action).await?)
}

/// The authorization of ListObjectsV2: the object listing's, with the bucket generation an
/// on-demand migration merge binds to captured before the decision and handed on after an allow.
///
/// # Errors
///
/// As [`authorize_bucket_listing`]. A generation that could not be read is handed on, not raised:
/// the use case surfaces it only if it has to consult the source.
pub(crate) async fn authorize_list_objects_v2<R: AuthorizationRequest + ?Sized>(
    server_ctx: &ServerContextSlot,
    req: &mut R,
    bucket: &str,
) -> S3Result<()> {
    let source_generation = capture_odm_read_generation(server_ctx, req, bucket).await;
    authorize_bucket_listing(req, bucket, S3Action::ListBucketAction).await?;
    req.install_odm_read_generation(source_generation);
    Ok(())
}

fn update_table_list_fingerprint_field(hasher: &mut Sha256, value: &[u8]) {
    hasher.update((value.len() as u64).to_be_bytes());
    hasher.update(value);
}

fn table_list_principal_fingerprint(cred: Option<&rustfs_credentials::Credentials>, is_owner: bool) -> String {
    let mut hasher = Sha256::new();
    update_table_list_fingerprint_field(
        &mut hasher,
        cred.map_or(b"anonymous".as_slice(), |credential| credential.access_key.as_bytes()),
    );
    hasher.update([u8::from(is_owner)]);
    hex_simd::encode_to_string(hasher.finalize(), hex_simd::AsciiCase::Lower)
}

fn table_list_snapshot_fingerprint(resources: &[TableDataPlaneListResource]) -> String {
    let mut hasher = Sha256::new();
    for entry in resources {
        for value in [
            entry.resource.table_bucket.as_bytes(),
            entry.resource.namespace.as_bytes(),
            entry.resource.table.as_bytes(),
            entry.resource.table_id.as_bytes(),
            entry.resource.warehouse_object_prefix.as_bytes(),
        ] {
            update_table_list_fingerprint_field(&mut hasher, value);
        }
        hasher.update([u8::from(entry.allowed)]);
    }
    hex_simd::encode_to_string(hasher.finalize(), hex_simd::AsciiCase::Lower)
}

fn table_list_cursor_key<R: AuthorizationRequest + ?Sized>(req: &R) -> super::super::S3Result<[u8; 32]> {
    let credentials = match req.server_context() {
        Some(server_ctx) => server_ctx
            .installed_app_context()
            .and_then(|context| context.action_credentials().get()),
        None => runtime_sources::current_action_credentials(),
    }
    .ok_or_else(|| table_data_plane_list_internal_error("action credentials are not initialized"))?;
    let mut hasher = Sha256::new();
    update_table_list_fingerprint_field(&mut hasher, TABLE_DATA_PLANE_LIST_CURSOR_KEY_CONTEXT);
    update_table_list_fingerprint_field(&mut hasher, credentials.access_key.as_bytes());
    update_table_list_fingerprint_field(&mut hasher, credentials.secret_key.as_bytes());
    Ok(hasher.finalize().into())
}

/// Snapshots which tables of a table bucket the caller may list, for the listing's use case. A
/// bucket that is not a table bucket installs nothing.
pub(crate) async fn prepare_table_data_plane_list_access<R: AuthorizationRequest + ?Sized>(
    req: &mut R,
    bucket: &str,
    action: S3Action,
) -> super::super::S3Result<()> {
    if !table_bucket_enabled_for_data_plane(req, bucket).await? {
        return Ok(());
    }
    retain_table_bucket_publication_guard(req, bucket).await?;
    if !table_bucket_enabled_for_data_plane(req, bucket).await? {
        return Err(S3Error::from(ApiError::access_denied()));
    }

    let (cred, is_owner) = {
        let req_info = req.req_info().ok_or_else(missing_req_info_error)?;
        (req_info.cred.clone(), req_info.is_owner)
    };
    let default_cred = rustfs_credentials::Credentials::default();
    let condition_cred = cred.as_ref().unwrap_or(&default_cred);
    let root_access_key = root_access_key();
    let principal = AuthzPrincipal::new(condition_cred, &root_access_key);
    let facts = request_authz_facts(
        req,
        AuthzTarget {
            action: Action::S3Action(action),
            bucket,
            object: "",
            version_id: None,
            location_constraint: None,
        },
    );
    let conditions = authorization_conditions(req, &principal, &facts)?;
    let iam_store = cred.as_ref().map(|_| request_iam_store(req)).transpose()?;
    let store = table_catalog_store_for_data_plane(req)?;
    let tables = crate::table_catalog::TableCatalogStore::list_all_tables(&store, bucket)
        .await
        .map_err(table_list_catalog_error)?;
    let default_claims = HashMap::new();
    let claims = cred
        .as_ref()
        .and_then(|credential| credential.claims.as_ref())
        .unwrap_or(&default_claims);
    let mut resources = Vec::with_capacity(tables.len());
    for table in tables {
        let warehouse_object_prefix =
            crate::table_catalog::table_warehouse_object_prefix(&table).map_err(table_list_catalog_error)?;
        let resource = crate::table_catalog::table_data_plane_resource_from_entry(table.clone(), warehouse_object_prefix);
        let allowed = match (cred.as_ref(), iam_store.as_ref()) {
            (Some(credential), Some(iam_store)) => {
                let resource_object = resource.catalog_resource_object();
                iam_store
                    .is_allowed(&Args {
                        account: &credential.access_key,
                        groups: &credential.groups,
                        action: Action::AdminAction(AdminAction::GetTableMetadataAction),
                        bucket,
                        conditions: &conditions,
                        is_owner,
                        object: &resource_object,
                        claims,
                        deny_only: false,
                    })
                    .await
            }
            _ => false,
        };
        resources.push(TableDataPlaneListResource { resource, allowed });
    }
    resources.sort_by(|left, right| {
        left.resource
            .warehouse_object_prefix
            .cmp(&right.resource.warehouse_object_prefix)
    });
    if resources.windows(2).any(|window| {
        crate::table_catalog::warehouse_object_prefixes_overlap(
            &window[0].resource.warehouse_object_prefix,
            &window[1].resource.warehouse_object_prefix,
        )
    }) {
        return Err(S3Error::from(ApiError::access_denied()));
    }
    let access = TableDataPlaneListAccess {
        bucket: bucket.to_string(),
        principal_fingerprint: table_list_principal_fingerprint(cred.as_ref(), is_owner),
        snapshot_fingerprint: table_list_snapshot_fingerprint(&resources),
        cursor_key: table_list_cursor_key(req)?,
        resources,
    };
    req.install_table_list_access(access);
    Ok(())
}
