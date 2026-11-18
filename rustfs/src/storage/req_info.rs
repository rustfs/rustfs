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

//! The per-request caller facts both S3 stacks attach to a request's extensions.
//!
//! Not responsible for resolving them (`storage::access` does) or for authorizing with
//! them. Upstream: the access phase that fills it; downstream: every use case that reads
//! the caller, bucket or object of the request.

use crate::storage::request_context::RequestContext;

#[derive(Default, Clone, Debug)]
pub(crate) struct ReqInfo {
    pub cred: Option<rustfs_credentials::Credentials>,
    pub is_owner: bool,
    /// Original session presence of a host-verified Keystone identity; its token bytes were erased.
    pub keystone_session_present: Option<bool>,
    pub bucket: Option<String>,
    pub object: Option<String>,
    pub version_id: Option<String>,
    pub replication_request_authorized: bool,
    #[allow(dead_code, reason = "written but never read back (backlog#1823)")]
    pub region: Option<rustfs_s3_types::Region>,
    pub request_context: Option<RequestContext>,
    /// Set by probe-style callers that treat AccessDenied as an expected filter
    /// outcome (ListBuckets per-bucket fallback, ListObjects metadata permission
    /// collection) so `authorize_request` logs those routine denials at `debug`
    /// instead of `warn` (issue #5740).
    pub suppress_denial_log: bool,
    /// Set by callers that audit their denials in aggregate (DeleteObjects), so each per-key
    /// denial is not audited on its own. The denial is still logged.
    pub suppress_denial_audit: bool,
}
