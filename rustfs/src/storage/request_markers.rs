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

//! Plain request-extension markers and the values an owned edge records for authorization.
//!
//! Not responsible for reading them back (`storage::access` does) or for the conversions
//! from the legacy s3s request bodies, which stay at the legacy edge. Upstream: the access
//! phase and the use cases that insert them; downstream: policy condition evaluation.

use time::OffsetDateTime;

#[derive(Clone, Debug)]
pub(crate) struct PostObjectRequestMarker;

/// Retain-until date the request asks to set. `authorization_conditions` turns it
/// into `object-lock-remaining-retention-days`.
#[derive(Clone, Debug)]
pub(crate) struct RequestedObjectLockRetainUntil(pub(crate) OffsetDateTime);

/// Retention a standalone PutObjectRetention applies from its XML body. Owned, so both request
/// forms carry it; each edge converts its decoded body once.
#[derive(Clone, Debug, Default)]
pub(crate) struct RequestedObjectLockRetention {
    pub(crate) mode: Option<String>,
    pub(crate) retain_until: Option<OffsetDateTime>,
}

/// Legal hold status a standalone PutObjectLegalHold applies from its XML body.
#[derive(Clone, Debug, Default)]
pub(crate) struct RequestedObjectLegalHold(pub(crate) Option<String>);

/// Tag set a standalone PutObjectTagging applies from its XML body, as the key/value pairs a
/// policy condition can name: a tag without a key or a value names none.
#[derive(Clone, Debug, Default)]
pub(crate) struct RequestedObjectTags(pub(crate) Vec<(String, String)>);

/// Request headers with the object policy inputs a POST upload applies from its
/// form fields, so header-derived condition keys describe what is stored.
#[derive(Clone)]
pub(crate) struct PostObjectPolicyHeaders(pub(crate) http::HeaderMap);

// The map is a clone of the request headers, so it can carry `Authorization`
// and session tokens; its debug form names the headers without their values.
impl std::fmt::Debug for PostObjectPolicyHeaders {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("PostObjectPolicyHeaders")
            .field(&self.0.keys().collect::<Vec<_>>())
            .finish()
    }
}

/// Copy source as the handler resolves it (percent-decoded, versionId canonical).
#[derive(Clone)]
pub(crate) struct RequestedCopySource(pub(crate) String);

impl RequestedCopySource {
    pub(crate) fn from_object(bucket: &str, key: &str, version_id: Option<&str>) -> Self {
        Self(match version_id {
            Some(version) => format!("{bucket}/{key}?versionId={}", crate::auth::canonical_policy_version_id(version)),
            None => format!("{bucket}/{key}"),
        })
    }
}

/// These operations stage upload state without publishing an object. Keep this
/// separate from the marker controlling multipart publication fences.
#[derive(Clone, Copy, Debug)]
pub(crate) struct MultipartStagingOperation;

impl MultipartStagingOperation {
    pub(crate) fn for_operation(operation: &str) -> Option<Self> {
        matches!(operation, "CreateMultipartUpload" | "UploadPart" | "UploadPartCopy").then_some(Self)
    }
}

#[derive(Clone, Debug)]
pub(crate) struct StagedMultipartPartAuthorization;
