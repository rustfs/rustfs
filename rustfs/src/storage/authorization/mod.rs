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

//! The authorization core both S3 request edges share: the request facts it reads, the access
//! phase, and the policy decisions over them.
//!
//! Responsible for: everything an authorization decision needs that is not tied to the legacy
//! s3s request type, so it survives the deletion of the legacy stack (rustfs/backlog#2783).
//! Not responsible for: the legacy `impl S3Access for FS` hooks, the `AuthorizationRequest`
//! implementation for the s3s request type, or the conversions from s3s DTOs; those stay in
//! `storage::access`. Per-operation authorization shapes live in `storage::access_owned`.
//! Upstream: the legacy access hooks and the gateway edge's access phase. Downstream: the
//! policy, IAM and table-catalog systems.

mod admission;
mod conditions;
mod decision;
mod discovery;
#[cfg(test)]
pub(crate) mod envelope_payloads;
mod guards;
#[cfg(test)]
mod permission_probe_test;
mod request;
mod table_data_plane;
#[cfg(test)]
pub(crate) mod tests;

pub(crate) use admission::{AccessMarkers, VerifiedSignature, admit_operation, resolve_req_info};
#[cfg(test)]
pub(crate) use conditions::{
    BucketPolicyExistingObjectTagHint, BucketPolicyRawLoadErrorKind, classify_bucket_policy_raw_load_error,
    merge_request_object_tag_conditions, remaining_retention_days,
};
pub(crate) use conditions::{
    ObjectTagConditions, authorization_conditions, get_or_fetch_object_tag_conditions, request_authz_facts, request_principal,
    resource_free_condition_values, root_access_key,
};
#[cfg(test)]
pub(crate) use conditions::{authorization_conditions_with_internal, existing_object_tag_conditions};
#[cfg(test)]
pub(crate) use conditions::{
    bucket_policy_needs_existing_object_tag_from_hint, load_bucket_policy_existing_object_tag_hint,
    maybe_merge_object_tag_conditions,
};
#[cfg(test)]
pub(crate) use decision::{DenialContext, owner_can_bypass_policy_deny};
pub(crate) use decision::{
    audit_request_denial, authorize_bucket_config_mutation, authorize_bucket_request, authorize_bucket_tag_deletion,
    authorize_object_request, authorize_replication_only_put_headers, authorize_request, authorize_request_core,
    authorize_request_with_location_constraint, authorize_staged_multipart_part, authorize_write_etag_condition,
    iam_authorization_error,
};
pub(crate) use discovery::{
    log_list_buckets_iam_implicit_deny, prepare_list_buckets_iam_authorization, query_list_buckets_permission,
    s3_action_potential,
};
pub(crate) use guards::{
    BucketConfigMutationSnapshot, BucketGenerationGuard, CopySourceBucketGenerationGuard, OdmReadGenerationGuard,
    PendingDeleteBucketGenerationGuard, apply_bucket_generation_guard, apply_copy_source_bucket_generation_guard,
    bucket_config_mutation_incarnation, capture_odm_read_generation, load_bucket_generation, load_bucket_generation_from_store,
    odm_read_generation, prepare_odm_read_generation, retained_table_data_plane_publication_guards,
};
#[cfg(test)]
pub(crate) use guards::{
    PUT_GENERATION_READ_QUORUM_FAILURE, install_object_generation_test_hook, wait_for_object_generation_test_hook,
};
#[cfg(test)]
pub(crate) use request::AMZ_WRITE_OFFSET_BYTES_HEADER;
pub(crate) use request::{
    AuthorizationRequest, has_bypass_governance_header, has_write_offset_bytes_header, invalid_force_delete_header,
    missing_req_info_error, object_store_not_initialized_error, recursive_force_delete_has_authenticated_caller,
    replication_request_authorized, req_info_mut, req_info_ref, request_context_from_req, request_iam_store,
    request_object_store, set_requested_object_lock_retain_until,
};
#[cfg(test)]
pub(crate) use table_data_plane::{
    TABLE_DATA_PLANE_LIST_CURSOR_MAX_ENCODED_LEN, require_owned_reserved_table_object, table_catalog_backend_for_data_plane,
    table_data_plane_admin_action, table_data_plane_content_mutation, table_data_plane_resource_for_request,
    table_publication_guard_error,
};
pub(crate) use table_data_plane::{
    TABLE_DATA_PLANE_LIST_CURSOR_PREFIX, TableDataPlaneListAccess, TableDataPlaneListCursorPosition, TableDataPlaneListResource,
    TableDataPlanePublicationGuards, retain_table_bucket_publication_guard, table_bucket_enabled_for_data_plane,
    table_catalog_store_for_data_plane, table_data_plane_list_internal_error, table_list_catalog_error,
};

#[cfg(test)]
pub(crate) mod parity_test_support {
    pub(crate) use super::envelope_payloads::bucket_config_mutation;
    use super::{AuthorizationRequest, ObjectTagConditions, TableDataPlanePublicationGuards};
    use crate::auth::facts::{AuthzFacts, AuthzPrincipal, AuthzTarget};
    use crate::storage::storage_api::ECStore;
    use rustfs_s3_types::S3Result;
    use std::collections::HashMap;
    use std::sync::Arc;

    /// Whether `guards` retains `key`, and how many lock guards it holds.
    pub(crate) fn publication_guard_holds(guards: &TableDataPlanePublicationGuards, key: &(String, String)) -> (bool, usize) {
        let state = guards.state.lock();
        (state.keys.contains(key), state.guards.len())
    }

    pub(crate) fn request_authz_facts<R: AuthorizationRequest + ?Sized>(req: &R, target: AuthzTarget<'_>) -> AuthzFacts {
        super::request_authz_facts(req, target)
    }

    pub(crate) fn authorization_conditions<R: AuthorizationRequest + ?Sized>(
        req: &R,
        principal: &AuthzPrincipal<'_>,
        facts: &AuthzFacts,
    ) -> S3Result<HashMap<String, Vec<String>>> {
        super::authorization_conditions(req, principal, facts)
    }

    pub(crate) fn request_object_store<R: AuthorizationRequest + ?Sized>(req: &R) -> S3Result<Arc<ECStore>> {
        super::request_object_store(req)
    }

    /// The cached tag evidence as `(bucket, object, version_id, values)`.
    pub(crate) fn tag_conditions_parts(
        conditions: &ObjectTagConditions,
    ) -> (&str, &str, Option<&str>, &HashMap<String, Vec<String>>) {
        (
            conditions.bucket.as_str(),
            conditions.object.as_str(),
            conditions.version_id.as_deref(),
            &conditions.values,
        )
    }
}
