// Copyright 2026 RustFS Team
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

//! S3 Vectors REST JSON boundary. Authentication remains in the HTTP service.

pub(crate) mod dto;
mod error;
mod route;
mod validation;

pub(crate) use error::{ErrorCode, ProtocolError};
pub(crate) use route::Route;
pub(crate) use validation::MAX_REQUEST_BODY_BYTES;

use dto::*;
use http::{HeaderValue, StatusCode};
use validation::Validate;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Operation {
    CreateIndex,
    CreateVectorBucket,
    DeleteIndex,
    DeleteVectorBucket,
    DeleteVectorBucketPolicy,
    DeleteVectors,
    GetIndex,
    GetVectorBucket,
    GetVectorBucketPolicy,
    GetVectors,
    ListIndexes,
    ListTagsForResource,
    ListVectorBuckets,
    ListVectors,
    PutVectorBucketDefaultIndexMode,
    PutVectorBucketPolicy,
    PutVectors,
    QueryVectors,
    TagResource,
    UntagResource,
    UpdateIndexMode,
}

impl Operation {
    pub(crate) const ALL: [Self; 21] = [
        Self::CreateIndex,
        Self::CreateVectorBucket,
        Self::DeleteIndex,
        Self::DeleteVectorBucket,
        Self::DeleteVectorBucketPolicy,
        Self::DeleteVectors,
        Self::GetIndex,
        Self::GetVectorBucket,
        Self::GetVectorBucketPolicy,
        Self::GetVectors,
        Self::ListIndexes,
        Self::ListTagsForResource,
        Self::ListVectorBuckets,
        Self::ListVectors,
        Self::PutVectorBucketDefaultIndexMode,
        Self::PutVectorBucketPolicy,
        Self::PutVectors,
        Self::QueryVectors,
        Self::TagResource,
        Self::UntagResource,
        Self::UpdateIndexMode,
    ];
    pub(crate) fn name(self) -> &'static str {
        match self {
            Self::CreateIndex => "CreateIndex",
            Self::CreateVectorBucket => "CreateVectorBucket",
            Self::DeleteIndex => "DeleteIndex",
            Self::DeleteVectorBucket => "DeleteVectorBucket",
            Self::DeleteVectorBucketPolicy => "DeleteVectorBucketPolicy",
            Self::DeleteVectors => "DeleteVectors",
            Self::GetIndex => "GetIndex",
            Self::GetVectorBucket => "GetVectorBucket",
            Self::GetVectorBucketPolicy => "GetVectorBucketPolicy",
            Self::GetVectors => "GetVectors",
            Self::ListIndexes => "ListIndexes",
            Self::ListTagsForResource => "ListTagsForResource",
            Self::ListVectorBuckets => "ListVectorBuckets",
            Self::ListVectors => "ListVectors",
            Self::PutVectorBucketDefaultIndexMode => "PutVectorBucketDefaultIndexMode",
            Self::PutVectorBucketPolicy => "PutVectorBucketPolicy",
            Self::PutVectors => "PutVectors",
            Self::QueryVectors => "QueryVectors",
            Self::TagResource => "TagResource",
            Self::UntagResource => "UntagResource",
            Self::UpdateIndexMode => "UpdateIndexMode",
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum Request {
    CreateIndex(CreateIndexInput),
    CreateVectorBucket(CreateVectorBucketInput),
    DeleteIndex(DeleteIndexInput),
    DeleteVectorBucket(DeleteVectorBucketInput),
    DeleteVectorBucketPolicy(DeleteVectorBucketPolicyInput),
    DeleteVectors(DeleteVectorsInput),
    GetIndex(GetIndexInput),
    GetVectorBucket(GetVectorBucketInput),
    GetVectorBucketPolicy(GetVectorBucketPolicyInput),
    GetVectors(GetVectorsInput),
    ListIndexes(ListIndexesInput),
    ListTagsForResource(ListTagsForResourceInput),
    ListVectorBuckets(ListVectorBucketsInput),
    ListVectors(ListVectorsInput),
    PutVectorBucketDefaultIndexMode(PutVectorBucketDefaultIndexModeInput),
    PutVectorBucketPolicy(PutVectorBucketPolicyInput),
    PutVectors(PutVectorsInput),
    QueryVectors(QueryVectorsInput),
    TagResource(TagResourceInput),
    UntagResource(UntagResourceInput),
    UpdateIndexMode(UpdateIndexModeInput),
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum Response {
    CreateIndex(CreateIndexOutput),
    CreateVectorBucket(CreateVectorBucketOutput),
    DeleteIndex(DeleteIndexOutput),
    DeleteVectorBucket(DeleteVectorBucketOutput),
    DeleteVectorBucketPolicy(DeleteVectorBucketPolicyOutput),
    DeleteVectors(DeleteVectorsOutput),
    GetIndex(GetIndexOutput),
    GetVectorBucket(GetVectorBucketOutput),
    GetVectorBucketPolicy(GetVectorBucketPolicyOutput),
    GetVectors(GetVectorsOutput),
    ListIndexes(ListIndexesOutput),
    ListTagsForResource(ListTagsForResourceOutput),
    ListVectorBuckets(ListVectorBucketsOutput),
    ListVectors(ListVectorsOutput),
    PutVectorBucketDefaultIndexMode(PutVectorBucketDefaultIndexModeOutput),
    PutVectorBucketPolicy(PutVectorBucketPolicyOutput),
    PutVectors(PutVectorsOutput),
    QueryVectors(QueryVectorsOutput),
    TagResource(TagResourceOutput),
    UntagResource(UntagResourceOutput),
    UpdateIndexMode(UpdateIndexModeOutput),
}

impl Request {
    pub(crate) fn operation(&self) -> Operation {
        match self {
            Self::CreateIndex(_) => Operation::CreateIndex,
            Self::CreateVectorBucket(_) => Operation::CreateVectorBucket,
            Self::DeleteIndex(_) => Operation::DeleteIndex,
            Self::DeleteVectorBucket(_) => Operation::DeleteVectorBucket,
            Self::DeleteVectorBucketPolicy(_) => Operation::DeleteVectorBucketPolicy,
            Self::DeleteVectors(_) => Operation::DeleteVectors,
            Self::GetIndex(_) => Operation::GetIndex,
            Self::GetVectorBucket(_) => Operation::GetVectorBucket,
            Self::GetVectorBucketPolicy(_) => Operation::GetVectorBucketPolicy,
            Self::GetVectors(_) => Operation::GetVectors,
            Self::ListIndexes(_) => Operation::ListIndexes,
            Self::ListTagsForResource(_) => Operation::ListTagsForResource,
            Self::ListVectorBuckets(_) => Operation::ListVectorBuckets,
            Self::ListVectors(_) => Operation::ListVectors,
            Self::PutVectorBucketDefaultIndexMode(_) => Operation::PutVectorBucketDefaultIndexMode,
            Self::PutVectorBucketPolicy(_) => Operation::PutVectorBucketPolicy,
            Self::PutVectors(_) => Operation::PutVectors,
            Self::QueryVectors(_) => Operation::QueryVectors,
            Self::TagResource(_) => Operation::TagResource,
            Self::UntagResource(_) => Operation::UntagResource,
            Self::UpdateIndexMode(_) => Operation::UpdateIndexMode,
        }
    }
    fn validate(&self, region: &str) -> Result<(), ProtocolError> {
        match self {
            Self::CreateIndex(input) => input.validate("", region),
            Self::CreateVectorBucket(input) => input.validate("", region),
            Self::DeleteIndex(input) => input.validate("", region),
            Self::DeleteVectorBucket(input) => input.validate("", region),
            Self::DeleteVectorBucketPolicy(input) => input.validate("", region),
            Self::DeleteVectors(input) => input.validate("", region),
            Self::GetIndex(input) => input.validate("", region),
            Self::GetVectorBucket(input) => input.validate("", region),
            Self::GetVectorBucketPolicy(input) => input.validate("", region),
            Self::GetVectors(input) => input.validate("", region),
            Self::ListIndexes(input) => input.validate("", region),
            Self::ListTagsForResource(input) => input.validate("", region),
            Self::ListVectorBuckets(input) => input.validate("", region),
            Self::ListVectors(input) => input.validate("", region),
            Self::PutVectorBucketDefaultIndexMode(input) => input.validate("", region),
            Self::PutVectorBucketPolicy(input) => input.validate("", region),
            Self::PutVectors(input) => input.validate("", region),
            Self::QueryVectors(input) => input.validate("", region),
            Self::TagResource(input) => input.validate("", region),
            Self::UntagResource(input) => input.validate("", region),
            Self::UpdateIndexMode(input) => input.validate("", region),
        }
    }
}

impl Response {
    pub(crate) fn body_bytes(&self) -> Result<Vec<u8>, ProtocolError> {
        let result = match self {
            Self::CreateIndex(output) => serde_json::to_vec(output),
            Self::CreateVectorBucket(output) => serde_json::to_vec(output),
            Self::DeleteIndex(_) => Ok(Vec::new()),
            Self::DeleteVectorBucket(_) => Ok(Vec::new()),
            Self::DeleteVectorBucketPolicy(_) => Ok(Vec::new()),
            Self::DeleteVectors(_) => Ok(Vec::new()),
            Self::GetIndex(output) => serde_json::to_vec(output),
            Self::GetVectorBucket(output) => serde_json::to_vec(output),
            Self::GetVectorBucketPolicy(output) => serde_json::to_vec(output),
            Self::GetVectors(output) => serde_json::to_vec(output),
            Self::ListIndexes(output) => serde_json::to_vec(output),
            Self::ListTagsForResource(output) => serde_json::to_vec(output),
            Self::ListVectorBuckets(output) => serde_json::to_vec(output),
            Self::ListVectors(output) => serde_json::to_vec(output),
            Self::PutVectorBucketDefaultIndexMode(_) => Ok(Vec::new()),
            Self::PutVectorBucketPolicy(_) => Ok(Vec::new()),
            Self::PutVectors(_) => Ok(Vec::new()),
            Self::QueryVectors(output) => serde_json::to_vec(output),
            Self::TagResource(_) => Ok(Vec::new()),
            Self::UntagResource(_) => Ok(Vec::new()),
            Self::UpdateIndexMode(_) => Ok(Vec::new()),
        };
        result.map_err(|_| ProtocolError::new(ErrorCode::InternalServer, "Unable to serialize response"))
    }
    pub(crate) fn into_response(self) -> Result<http::Response<Vec<u8>>, ProtocolError> {
        let mut response = http::Response::new(self.body_bytes()?);
        *response.status_mut() = StatusCode::OK;
        response
            .headers_mut()
            .insert(http::header::CONTENT_TYPE, HeaderValue::from_static("application/json"));
        Ok(response)
    }
}

#[cfg(test)]
mod tests;
