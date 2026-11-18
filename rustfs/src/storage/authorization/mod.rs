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
mod request;

pub(crate) use admission::{AccessMarkers, VerifiedSignature, admit_operation, resolve_req_info};
#[cfg(test)]
pub(crate) use request::AMZ_WRITE_OFFSET_BYTES_HEADER;
pub(crate) use request::{
    AuthorizationRequest, has_bypass_governance_header, has_write_offset_bytes_header, invalid_force_delete_header,
    missing_req_info_error, object_store_not_initialized_error, recursive_force_delete_has_authenticated_caller,
    replication_request_authorized, req_info_mut, req_info_ref, request_context_from_req, request_iam_store,
    request_object_store, set_requested_object_lock_retain_until,
};
