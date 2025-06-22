use hyper::HeaderMap;
use s3s::{S3Request, S3Response};
use std::collections::HashMap;

/// Extract request parameters from S3Request, mainly header information.
pub fn extract_req_params<T>(req: &S3Request<T>) -> HashMap<String, String> {
    let mut params = HashMap::new();
    for (key, value) in req.headers.iter() {
        if let Ok(val_str) = value.to_str() {
            params.insert(key.as_str().to_string(), val_str.to_string());
        }
    }
    params
}

/// Extract response elements from S3Response, mainly header information.
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
pub fn get_request_host(headers: &HeaderMap) -> String {
    headers
        .get("host")
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
