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
pub fn extract_req_params<T>(req: &S3Request<T>) -> HashMap<String, String> {
    extract_params_header(&req.headers)
}

/// Extract request parameters from hyper::HeaderMap, mainly header information.
/// This function is useful when you have a raw HTTP request and need to extract parameters.
#[deprecated(since = "0.1.0", note = "Use extract_params_header instead")]
pub fn extract_req_params_header(head: &HeaderMap) -> HashMap<String, String> {
    extract_params_header(head)
}

/// Extract parameters from hyper::HeaderMap, mainly header information.
/// This function is useful when you have a raw HTTP request and need to extract parameters.
pub fn extract_params_header(head: &HeaderMap) -> HashMap<String, String> {
    let mut params = HashMap::new();
    for (key, value) in head.iter() {
        if let Ok(val_str) = value.to_str() {
            params.insert(key.as_str().to_string(), val_str.to_string());
        }
    }
    params
}

/// Extract response elements from S3Response, mainly header information.
pub fn extract_resp_elements<T>(resp: &S3Response<T>) -> HashMap<String, String> {
    extract_params_header(&resp.headers)
}

/// Get host from header information.
pub fn get_request_host(headers: &HeaderMap) -> String {
    headers
        .get("host")
        .and_then(|v| v.to_str().ok())
        .unwrap_or_default()
        .to_string()
}

/// Get Port from header information.
/// Priority:
/// 1. x-forwarded-port
/// 2. host header (parse port)
///    If host has no port, try to deduce from x-forwarded-proto (http->80, https->443)
/// 3. port header
///
/// If the port cannot be determined, returns 0.
pub fn get_request_port(headers: &HeaderMap) -> u16 {
    // 1. Try x-forwarded-port
    if let Some(port) = headers
        .get("x-forwarded-port")
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.parse::<u16>().ok())
    {
        return port;
    }

    // 2. Try host header
    if let Some(host) = headers.get("host").and_then(|v| v.to_str().ok()) {
        if let Some(idx) = host.rfind(':') {
            // Check if it's an IPv6 address with port, e.g., [::1]:8080
            // If ']' is present, the colon must be after it.
            let valid_colon = match host.rfind(']') {
                Some(close_bracket_idx) => idx > close_bracket_idx,
                None => true,
            };

            if valid_colon
                && let Ok(port) = host[idx + 1..].parse::<u16>()
                && port > 0
            {
                return port;
            }
        }

        // If host is present but no port found (or parsing failed, or port is 0),
        // try to deduce from x-forwarded-proto
        if let Some(proto) = headers.get("x-forwarded-proto").and_then(|v| v.to_str().ok()) {
            match proto {
                "http" => return 80,
                "https" => return 443,
                _ => {}
            }
        }
    }

    // 3. Fallback to "port" header
    headers
        .get("port")
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.parse::<u16>().ok())
        .unwrap_or(0)
}

/// Get content-length from header information.
pub fn get_request_content_length(headers: &HeaderMap) -> u64 {
    headers
        .get("content-length")
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or(0)
}

/// Get referer from header information.
/// If the referer header is not present, returns an empty string.
pub fn get_request_referer(headers: &HeaderMap) -> String {
    headers
        .get("referer")
        .and_then(|v| v.to_str().ok())
        .unwrap_or_default()
        .to_string()
}

/// Get user-agent from header information.
pub fn get_request_user_agent(headers: &HeaderMap) -> String {
    headers
        .get("user-agent")
        .and_then(|v| v.to_str().ok())
        .unwrap_or_default()
        .to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use hyper::header::HeaderValue;

    #[test]
    fn test_get_request_port() {
        let mut headers = HeaderMap::new();

        // Case 1: No port info
        assert_eq!(get_request_port(&headers), 0);

        // Case 2: port header
        headers.insert("port", HeaderValue::from_static("8080"));
        assert_eq!(get_request_port(&headers), 8080);

        // Case 3: host header with port
        headers.remove("port");
        headers.insert("host", HeaderValue::from_static("example.com:9000"));
        assert_eq!(get_request_port(&headers), 9000);

        // Case 4: host header without port, no proto
        headers.insert("host", HeaderValue::from_static("example.com"));
        assert_eq!(get_request_port(&headers), 0);

        // Case 5: IPv6 host with port
        headers.insert("host", HeaderValue::from_static("[::1]:9001"));
        assert_eq!(get_request_port(&headers), 9001);

        // Case 6: IPv6 host without port
        headers.insert("host", HeaderValue::from_static("[::1]"));
        assert_eq!(get_request_port(&headers), 0);

        // Case 7: x-forwarded-port
        headers.insert("x-forwarded-port", HeaderValue::from_static("7000"));
        // Even if host is present, x-forwarded-port takes precedence
        assert_eq!(get_request_port(&headers), 7000);

        // Case 8: host without port, but x-forwarded-proto is http
        headers.remove("x-forwarded-port");
        headers.insert("host", HeaderValue::from_static("example.com"));
        headers.insert("x-forwarded-proto", HeaderValue::from_static("http"));
        assert_eq!(get_request_port(&headers), 80);

        // Case 9: host without port, but x-forwarded-proto is https
        headers.insert("x-forwarded-proto", HeaderValue::from_static("https"));
        assert_eq!(get_request_port(&headers), 443);

        // Case 10: host without port, unknown proto
        headers.insert("x-forwarded-proto", HeaderValue::from_static("ftp"));
        assert_eq!(get_request_port(&headers), 0);

        // Case 11: host with port 0, should fallback to proto
        headers.insert("host", HeaderValue::from_static("example.com:0"));
        headers.insert("x-forwarded-proto", HeaderValue::from_static("https"));
        assert_eq!(get_request_port(&headers), 443);

        // Case 12: host with port 0, no proto
        headers.remove("x-forwarded-proto");
        assert_eq!(get_request_port(&headers), 0);
    }
}
