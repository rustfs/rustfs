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

use super::dto::*;
use super::validation::validate_body_length;
use super::{Operation, ProtocolError, Request};
use http::Method;
use serde::Deserialize;
use serde::de::DeserializeOwned;
use std::collections::BTreeMap;

/// A modeled method/path match, without authentication or authorization.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Route {
    operation: Operation,
    resource_label: Option<String>,
}

impl Route {
    /// The HTTP adapter must identify the signing service before claiming tags paths.
    /// Matching a route provides no evidence that a request is authenticated.
    pub(crate) fn recognize(method: &Method, raw_path: &str) -> Option<Self> {
        if let Some(label) = raw_path.strip_prefix("/tags/") {
            if label.is_empty() || label.contains('/') {
                return None;
            }
            let operation = match *method {
                Method::POST => Operation::TagResource,
                Method::GET => Operation::ListTagsForResource,
                Method::DELETE => Operation::UntagResource,
                _ => return None,
            };
            return Some(Self {
                operation,
                resource_label: Some(label.to_owned()),
            });
        }
        if *method != Method::POST {
            return None;
        }
        let name = raw_path.strip_prefix('/')?;
        let operation = Operation::ALL.into_iter().find(|operation| {
            !matches!(
                operation,
                Operation::TagResource | Operation::ListTagsForResource | Operation::UntagResource
            ) && operation.name() == name
        })?;
        Some(Self {
            operation,
            resource_label: None,
        })
    }

    pub(crate) fn operation(&self) -> Operation {
        self.operation
    }

    /// Decode the authenticated bytes without changing the signed URI or payload.
    /// `region` is the deployment region supplied by the HTTP adapter.
    pub(crate) fn parse(&self, raw_query: Option<&str>, body: &[u8], region: &str) -> Result<Request, ProtocolError> {
        validate_body_length(body.len())?;
        let request = match self.operation {
            Operation::CreateIndex => Request::CreateIndex(deserialize_body(body)?),
            Operation::CreateVectorBucket => Request::CreateVectorBucket(deserialize_body(body)?),
            Operation::DeleteIndex => Request::DeleteIndex(deserialize_body(body)?),
            Operation::DeleteVectorBucket => Request::DeleteVectorBucket(deserialize_body(body)?),
            Operation::DeleteVectorBucketPolicy => Request::DeleteVectorBucketPolicy(deserialize_body(body)?),
            Operation::DeleteVectors => Request::DeleteVectors(deserialize_body(body)?),
            Operation::GetIndex => Request::GetIndex(deserialize_body(body)?),
            Operation::GetVectorBucket => Request::GetVectorBucket(deserialize_body(body)?),
            Operation::GetVectorBucketPolicy => Request::GetVectorBucketPolicy(deserialize_body(body)?),
            Operation::GetVectors => Request::GetVectors(deserialize_body(body)?),
            Operation::ListIndexes => Request::ListIndexes(deserialize_body(body)?),
            Operation::ListVectorBuckets => Request::ListVectorBuckets(deserialize_body(body)?),
            Operation::ListVectors => Request::ListVectors(deserialize_body(body)?),
            Operation::PutVectorBucketDefaultIndexMode => Request::PutVectorBucketDefaultIndexMode(deserialize_body(body)?),
            Operation::PutVectorBucketPolicy => Request::PutVectorBucketPolicy(deserialize_body(body)?),
            Operation::PutVectors => Request::PutVectors(deserialize_body(body)?),
            Operation::QueryVectors => Request::QueryVectors(deserialize_body(body)?),
            Operation::UpdateIndexMode => Request::UpdateIndexMode(deserialize_body(body)?),
            Operation::TagResource => {
                #[derive(Deserialize)]
                struct TagBody {
                    tags: BTreeMap<String, String>,
                }
                let input: TagBody = deserialize_body(body)?;
                Request::TagResource(TagResourceInput {
                    resource_arn: self.resource_arn()?,
                    tags: input.tags,
                })
            }
            Operation::ListTagsForResource => {
                require_empty_body(body)?;
                Request::ListTagsForResource(ListTagsForResourceInput {
                    resource_arn: self.resource_arn()?,
                })
            }
            Operation::UntagResource => {
                require_empty_body(body)?;
                let tag_keys = query_tag_keys(raw_query.unwrap_or_default())?;
                if tag_keys.is_empty() {
                    return Err(ProtocolError::validation("tagKeys", "At least one tagKeys query parameter is required"));
                }
                Request::UntagResource(UntagResourceInput {
                    resource_arn: self.resource_arn()?,
                    tag_keys,
                })
            }
        };
        request.validate(region)?;
        Ok(request)
    }

    fn resource_arn(&self) -> Result<String, ProtocolError> {
        let label = self
            .resource_label
            .as_deref()
            .ok_or_else(|| ProtocolError::validation("resourceArn", "Missing resource ARN"))?;
        decode_component(label, "resourceArn")
    }
}

fn deserialize_body<T: DeserializeOwned>(body: &[u8]) -> Result<T, ProtocolError> {
    let body = if body.iter().all(|byte| matches!(byte, b' ' | b'\t' | b'\n' | b'\r')) {
        b"{}"
    } else {
        body
    };
    let mut deserializer = serde_json::Deserializer::from_slice(body);
    deserialize_object(&mut deserializer)
        .and_then(|value| {
            deserializer.end()?;
            Ok(value)
        })
        .map_err(|_| ProtocolError::validation("request", "Request body does not match the operation input"))
}

fn require_empty_body(body: &[u8]) -> Result<(), ProtocolError> {
    if body.is_empty() {
        return Ok(());
    }
    Err(ProtocolError::validation("request", "This operation does not accept a request body"))
}

fn query_tag_keys(query: &str) -> Result<Vec<String>, ProtocolError> {
    let mut keys = Vec::new();
    for part in query.split('&').filter(|part| !part.is_empty()) {
        let (key, value) = part.split_once('=').unwrap_or((part, ""));
        if decode_query_component(key, "query")? == "tagKeys" {
            keys.push(decode_query_component(value, "tagKeys")?);
        }
    }
    Ok(keys)
}

fn decode_query_component(value: &str, path: &str) -> Result<String, ProtocolError> {
    // s3s OrderedQs uses form decoding for the signed query, including literal '+'.
    if value.contains('+') {
        decode_component(&value.replace('+', " "), path)
    } else {
        decode_component(value, path)
    }
}

fn decode_component(value: &str, path: &str) -> Result<String, ProtocolError> {
    let bytes = value.as_bytes();
    let mut index = 0;
    while index < bytes.len() {
        if bytes[index] == b'%' {
            if bytes.get(index + 1).is_none_or(|byte| !byte.is_ascii_hexdigit())
                || bytes.get(index + 2).is_none_or(|byte| !byte.is_ascii_hexdigit())
            {
                return Err(ProtocolError::validation(path, "Malformed percent encoding"));
            }
            index += 3;
        } else {
            index += 1;
        }
    }
    percent_encoding::percent_decode_str(value)
        .decode_utf8()
        .map(|value| value.into_owned())
        .map_err(|_| ProtocolError::validation(path, "URI parameter must contain valid UTF-8"))
}
