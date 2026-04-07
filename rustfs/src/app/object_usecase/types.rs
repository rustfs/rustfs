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

use super::*;

#[derive(Clone)]
pub(super) struct GetObjectRequestContext {
    pub(super) bucket: String,
    pub(super) key: String,
    pub(super) cache_key: String,
    pub(super) version_id_for_event: String,
    pub(super) part_number: Option<usize>,
    pub(super) rs: Option<HTTPRangeSpec>,
    pub(super) opts: ObjectOptions,
    pub(super) headers: HeaderMap,
    pub(super) method: hyper::Method,
    pub(super) sse_customer_key: Option<String>,
    pub(super) sse_customer_key_md5: Option<String>,
}

pub(super) type PutObjectChecksums = rustfs_object_io::put::PutObjectChecksums;

#[derive(Clone)]
pub(super) struct PutObjectRequestContext {
    pub(super) headers: HeaderMap,
    pub(super) trailing_headers: Option<s3s::TrailingHeaders>,
    pub(super) uri_query: Option<String>,
    pub(super) is_post_object: bool,
    pub(super) method: hyper::Method,
    pub(super) uri: hyper::Uri,
    pub(super) extensions: http::Extensions,
    pub(super) credentials: Option<s3s::auth::Credentials>,
    pub(super) region: Option<s3s::region::Region>,
    pub(super) service: Option<String>,
}

pub(super) struct PutObjectFlowResult {
    pub(super) output: PutObjectOutput,
    pub(super) helper_object: ObjectInfo,
    pub(super) helper_version_id: Option<String>,
}
