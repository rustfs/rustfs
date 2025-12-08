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

//! File history and point-in-time retrieval handlers.
//!
//! This module provides admin API endpoints for viewing object version history
//! and retrieving objects at specific points in time.

use super::Operation;
use axum::http::StatusCode;
use matchit::Params;
use rustfs_ecstore::StorageAPI;
use rustfs_ecstore::bucket::versioning_sys::BucketVersioningSys;
use rustfs_ecstore::new_object_layer_fn;
use rustfs_ecstore::point_in_time::{
    FileHistoryResponse, ObjectVersionHistory, find_version_at_time, get_version_history, sort_versions_by_time,
};
use s3s::header::CONTENT_TYPE;
use s3s::{Body, S3Error, S3ErrorCode, S3Request, S3Response, S3Result, s3_error};
use serde::Serialize;
use time::OffsetDateTime;
use time::format_description::well_known::Iso8601;
use tracing::{error, warn};
use url::form_urlencoded;

/// Response for point-in-time query
#[derive(Debug, Serialize)]
pub struct ObjectAtTimeResponse {
    pub bucket: String,
    pub object: String,
    pub requested_time: String,
    pub version_found: bool,
    pub version: Option<ObjectVersionHistory>,
    pub is_deleted: bool,
}

/// Handler for getting file version history
pub struct GetFileHistoryHandler {}

impl GetFileHistoryHandler {
    pub fn new() -> Self {
        Self {}
    }
}

impl Default for GetFileHistoryHandler {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait::async_trait]
impl Operation for GetFileHistoryHandler {
    async fn call(&self, req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        // Parse query parameters
        let query = req.uri.query().unwrap_or("");
        let params: std::collections::HashMap<String, String> = form_urlencoded::parse(query.as_bytes()).into_owned().collect();

        let bucket = match params.get("bucket") {
            Some(b) => b.clone(),
            None => {
                return Err(s3_error!(InvalidRequest, "Missing 'bucket' parameter"));
            }
        };

        let object = match params.get("object") {
            Some(o) => o.clone(),
            None => {
                return Err(s3_error!(InvalidRequest, "Missing 'object' parameter"));
            }
        };

        let max_versions: i32 = params.get("max_versions").and_then(|v| v.parse().ok()).unwrap_or(100);

        // Check if versioning is enabled for the bucket
        if !BucketVersioningSys::enabled(&bucket).await {
            return Err(s3_error!(InvalidRequest, "Versioning is not enabled for this bucket"));
        }

        // Get object layer
        let Some(store) = new_object_layer_fn() else {
            return Err(S3Error::with_message(
                S3ErrorCode::InternalError,
                "Object layer not initialized".to_string(),
            ));
        };

        // Get all versions of the object
        let versions_result = store
            .clone()
            .list_object_versions(&bucket, &object, None, None, None, max_versions)
            .await;

        let mut versions = match versions_result {
            Ok(info) => info.objects,
            Err(e) => {
                error!("Failed to list object versions: {:?}", e);
                return Err(s3_error!(InternalError, "Failed to list object versions"));
            }
        };

        // Filter to only include versions of the specific object
        versions.retain(|v| v.name == object);

        // Sort versions by time (newest first)
        sort_versions_by_time(&mut versions);

        // Convert to history response
        let history = get_version_history(versions);
        let response = FileHistoryResponse::new(bucket, object, history);

        let body = serde_json::to_vec(&response).map_err(|e| {
            error!("Failed to serialize response: {:?}", e);
            s3_error!(InternalError, "Failed to serialize response")
        })?;

        let mut headers = http::HeaderMap::new();
        headers.insert(CONTENT_TYPE, "application/json".parse().unwrap());

        Ok(S3Response::with_headers((StatusCode::OK, Body::from(body)), headers))
    }
}

/// Handler for getting object at a specific point in time
pub struct GetObjectAtTimeHandler {}

impl GetObjectAtTimeHandler {
    pub fn new() -> Self {
        Self {}
    }
}

impl Default for GetObjectAtTimeHandler {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait::async_trait]
impl Operation for GetObjectAtTimeHandler {
    async fn call(&self, req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        // Parse query parameters
        let query = req.uri.query().unwrap_or("");
        let params: std::collections::HashMap<String, String> = form_urlencoded::parse(query.as_bytes()).into_owned().collect();

        let bucket = match params.get("bucket") {
            Some(b) => b.clone(),
            None => {
                return Err(s3_error!(InvalidRequest, "Missing 'bucket' parameter"));
            }
        };

        let object = match params.get("object") {
            Some(o) => o.clone(),
            None => {
                return Err(s3_error!(InvalidRequest, "Missing 'object' parameter"));
            }
        };

        let at_time_str = match params.get("at") {
            Some(t) => t.clone(),
            None => {
                return Err(s3_error!(InvalidRequest, "Missing 'at' parameter (ISO 8601 datetime)"));
            }
        };

        // Parse the datetime
        let at_time = match OffsetDateTime::parse(&at_time_str, &Iso8601::DEFAULT) {
            Ok(t) => t,
            Err(e) => {
                warn!("Failed to parse datetime '{}': {:?}", at_time_str, e);
                return Err(s3_error!(
                    InvalidRequest,
                    "Invalid datetime format. Use ISO 8601 (e.g., 2025-01-15T10:30:00Z)"
                ));
            }
        };

        // Check if versioning is enabled for the bucket
        if !BucketVersioningSys::enabled(&bucket).await {
            return Err(s3_error!(InvalidRequest, "Versioning is not enabled for this bucket"));
        }

        // Get object layer
        let Some(store) = new_object_layer_fn() else {
            return Err(S3Error::with_message(
                S3ErrorCode::InternalError,
                "Object layer not initialized".to_string(),
            ));
        };

        // Get all versions of the object
        let versions_result = store
            .clone()
            .list_object_versions(&bucket, &object, None, None, None, 1000)
            .await;

        let mut versions = match versions_result {
            Ok(info) => info.objects,
            Err(e) => {
                error!("Failed to list object versions: {:?}", e);
                return Err(s3_error!(InternalError, "Failed to list object versions"));
            }
        };

        // Filter to only include versions of the specific object
        versions.retain(|v| v.name == object);

        // Sort versions by time (newest first)
        sort_versions_by_time(&mut versions);

        // Find the version at the requested time
        let version_at_time = find_version_at_time(&versions, at_time);

        let response = match version_at_time {
            Some(version) => ObjectAtTimeResponse {
                bucket,
                object,
                requested_time: at_time_str,
                version_found: true,
                is_deleted: version.delete_marker,
                version: Some(ObjectVersionHistory::from_object_info(version, false)),
            },
            None => ObjectAtTimeResponse {
                bucket,
                object,
                requested_time: at_time_str,
                version_found: false,
                is_deleted: false,
                version: None,
            },
        };

        let body = serde_json::to_vec(&response).map_err(|e| {
            error!("Failed to serialize response: {:?}", e);
            s3_error!(InternalError, "Failed to serialize response")
        })?;

        let mut headers = http::HeaderMap::new();
        headers.insert(CONTENT_TYPE, "application/json".parse().unwrap());

        Ok(S3Response::with_headers((StatusCode::OK, Body::from(body)), headers))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_iso8601_datetime() {
        let time_str = "2025-01-15T10:30:00Z";
        let result = OffsetDateTime::parse(time_str, &Iso8601::DEFAULT);
        assert!(result.is_ok());
    }

    #[test]
    fn test_parse_iso8601_datetime_with_offset() {
        let time_str = "2025-01-15T10:30:00+05:00";
        let result = OffsetDateTime::parse(time_str, &Iso8601::DEFAULT);
        assert!(result.is_ok());
    }
}
