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

mod net;

use hashbrown::HashMap;
use hyper::HeaderMap;
use s3s::{S3Request, S3Response};

pub use net::*;

/// Extract request parameters from S3Request, mainly header information.
#[allow(dead_code)]
pub fn extract_req_params<T>(req: &S3Request<T>) -> HashMap<String, String> {
    let mut params = HashMap::new();
    for (key, value) in req.headers.iter() {
        if let Ok(val_str) = value.to_str() {
            params.insert(key.as_str().to_string(), val_str.to_string());
        }
    }
    params
}

/// Extract request parameters from hyper::HeaderMap, mainly header information.
/// This function is useful when you have a raw HTTP request and need to extract parameters.
pub fn extract_req_params_header(head: &HeaderMap) -> HashMap<String, String> {
    let mut params = HashMap::new();
    for (key, value) in head.iter() {
        if let Ok(val_str) = value.to_str() {
            params.insert(key.as_str().to_string(), val_str.to_string());
        }
    }
    params
}

/// Extract response elements from S3Response, mainly header information.
#[allow(dead_code)]
pub fn extract_resp_elements<T>(resp: &S3Response<T>) -> HashMap<String, String> {
    let mut params = HashMap::new();
    for (key, value) in resp.headers.iter() {
        if let Ok(val_str) = value.to_str() {
            params.insert(key.as_str().to_string(), val_str.to_string());
        }
    }
    params
}

/// Get host from header information.
#[allow(dead_code)]
pub fn get_request_host(headers: &HeaderMap) -> String {
    headers
        .get("host")
        .and_then(|v| v.to_str().ok())
        .unwrap_or_default()
        .to_string()
}

/// Get user-agent from header information.
#[allow(dead_code)]
pub fn get_request_user_agent(headers: &HeaderMap) -> String {
    headers
        .get("user-agent")
        .and_then(|v| v.to_str().ok())
        .unwrap_or_default()
        .to_string()
}
