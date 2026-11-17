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

//! Method/subresource validation shared by both S3 edges at request admission.

use http::Method;

#[derive(Debug, PartialEq, Eq)]
pub(super) enum RequestViolation {
    DeleteAcl,
}

pub(super) fn validate_method_subresource(method: &Method, raw_query: Option<&str>) -> Result<(), RequestViolation> {
    // S3 exposes Get/PutBucketAcl and Get/PutObjectAcl, but no ACL deletion.
    // Routers may ignore acl for DELETE and select object or bucket deletion.
    // Check decoded keys before authorization/dispatch, including duplicates;
    // other query parameters retain their existing interpretation.
    if method == Method::DELETE
        && raw_query.is_some_and(|query| url::form_urlencoded::parse(query.as_bytes()).any(|(key, _)| key == "acl"))
    {
        return Err(RequestViolation::DeleteAcl);
    }
    Ok(())
}
