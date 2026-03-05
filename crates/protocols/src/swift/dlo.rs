// Copyright 2024 RustFS Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Dynamic Large Objects (DLO) support for Swift API
//!
//! DLO provides prefix-based automatic segment discovery and assembly.
//! Segments are discovered at download time using lexicographic ordering
//! based on a container metadata manifest pointer.

use super::{SwiftError, container, object};
use axum::http::{HeaderMap, Response, StatusCode};
use rustfs_credentials::Credentials;
use s3s::Body;
use std::collections::HashMap;

/// ObjectInfo represents metadata about an object (from container listings)
#[derive(Debug, Clone)]
pub struct ObjectInfo {
    pub name: String,
    pub size: i64,
    pub content_type: Option<String>,
    pub etag: Option<String>,
}

/// Check if object is a DLO by checking for manifest metadata
pub async fn is_dlo_object(
    account: &str,
    container: &str,
    object: &str,
    credentials: &Option<Credentials>,
) -> Result<Option<String>, SwiftError> {
    // Require credentials
    let creds = credentials.as_ref()
        .ok_or_else(|| SwiftError::Unauthorized("Credentials required".to_string()))?;

    // Get object metadata to check for DLO manifest header
    let info = object::head_object(account, container, object, creds).await?;

    // Check for X-Object-Manifest metadata
    Ok(info.user_defined.get("x-object-manifest").cloned())
}

/// List DLO segments in lexicographic order
pub async fn list_dlo_segments(
    account: &str,
    container: &str,
    prefix: &str,
    credentials: &Option<Credentials>,
) -> Result<Vec<ObjectInfo>, SwiftError> {
    // Require credentials for DLO operations
    let creds = credentials.as_ref()
        .ok_or_else(|| SwiftError::Unauthorized("Credentials required for DLO operations".to_string()))?;

    // List objects with prefix using the container module's list_objects function
    let objects = container::list_objects(
        account,
        container,
        creds,
        None, // limit
        None, // marker
        Some(prefix.to_string()),
        None, // delimiter
    ).await?;

    // Convert to ObjectInfo and sort lexicographically
    let mut object_infos: Vec<ObjectInfo> = objects.iter().map(|obj| ObjectInfo {
        name: obj.name.clone(),
        size: obj.bytes as i64,
        content_type: Some(obj.content_type.clone()),
        etag: Some(obj.hash.clone()),
    }).collect();

    // Sort lexicographically (critical for correct assembly)
    object_infos.sort_by(|a, b| a.name.cmp(&b.name));

    Ok(object_infos)
}

/// Parse DLO manifest value "container/prefix" into (container, prefix)
fn parse_dlo_manifest(manifest: &str) -> Result<(String, String), SwiftError> {
    let parts: Vec<&str> = manifest.splitn(2, '/').collect();

    if parts.len() != 2 {
        return Err(SwiftError::BadRequest(format!("Invalid DLO manifest format: {}", manifest)));
    }

    Ok((parts[0].to_string(), parts[1].to_string()))
}

/// Generate transaction ID for response headers
fn generate_trans_id() -> String {
    format!("tx{}", uuid::Uuid::new_v4().as_simple())
}

/// Parse Range header (e.g., "bytes=0-1023")
fn parse_range_header(range_str: &str, total_size: u64) -> Result<(u64, u64), SwiftError> {
    if !range_str.starts_with("bytes=") {
        return Err(SwiftError::BadRequest("Invalid Range header format".to_string()));
    }

    let range_part = &range_str[6..];
    let parts: Vec<&str> = range_part.split('-').collect();

    if parts.len() != 2 {
        return Err(SwiftError::BadRequest("Invalid Range header format".to_string()));
    }

    let (start, end) = if parts[0].is_empty() {
        // Suffix range (last N bytes): bytes=-500
        let suffix: u64 = parts[1].parse()
            .map_err(|_| SwiftError::BadRequest("Invalid Range header".to_string()))?;
        if suffix >= total_size {
            (0, total_size - 1)
        } else {
            (total_size - suffix, total_size - 1)
        }
    } else {
        // Regular range: bytes=0-999 or bytes=0-
        let start = parts[0].parse()
            .map_err(|_| SwiftError::BadRequest("Invalid Range header".to_string()))?;

        let end = if parts[1].is_empty() {
            total_size - 1
        } else {
            let parsed: u64 = parts[1].parse()
                .map_err(|_| SwiftError::BadRequest("Invalid Range header".to_string()))?;
            std::cmp::min(parsed, total_size - 1)
        };

        (start, end)
    };

    if start > end {
        return Err(SwiftError::BadRequest("Invalid Range: start > end".to_string()));
    }

    Ok((start, end))
}

/// Calculate which segments and byte ranges to fetch for a given range request
fn calculate_dlo_segments_for_range(
    segments: &[ObjectInfo],
    start: u64,
    end: u64,
) -> Result<Vec<(usize, u64, u64, ObjectInfo)>, SwiftError> {
    let mut result = Vec::new();
    let mut current_offset = 0u64;

    for (idx, segment) in segments.iter().enumerate() {
        let segment_start = current_offset;
        let segment_end = current_offset + segment.size as u64 - 1;

        // Check if this segment overlaps with requested range
        if segment_end >= start && segment_start <= end {
            // Calculate byte range within this segment
            let byte_start = if start > segment_start {
                start - segment_start
            } else {
                0
            };

            let byte_end = if end < segment_end {
                end - segment_start
            } else {
                segment.size as u64 - 1
            };

            result.push((idx, byte_start, byte_end, segment.clone()));
        }

        current_offset += segment.size as u64;

        // Stop if we've passed the requested range
        if current_offset > end {
            break;
        }
    }

    Ok(result)
}

/// Handle GET for DLO (discover segments and stream)
pub async fn handle_dlo_get(
    account: &str,
    _container: &str,
    _object: &str,
    headers: &HeaderMap,
    credentials: &Option<Credentials>,
    manifest_value: String, // "container/prefix"
) -> Result<Response<Body>, SwiftError> {
    // 1. Parse manifest value to get segment container and prefix
    let (segment_container, prefix) = parse_dlo_manifest(&manifest_value)?;

    // 2. List segments
    let segments = list_dlo_segments(account, &segment_container, &prefix, credentials).await?;

    if segments.is_empty() {
        return Err(SwiftError::NotFound("No DLO segments found".to_string()));
    }

    // 3. Calculate total size
    let total_size: u64 = segments.iter().map(|s| s.size as u64).sum();

    // 4. Parse range header if present
    let range = headers.get("range")
        .and_then(|v| v.to_str().ok())
        .and_then(|r| parse_range_header(r, total_size).ok());

    // 5. Collect segment data
    let segment_data = collect_dlo_segments(account, &segment_container, &segments, credentials, range).await?;

    // 6. Build response
    let trans_id = generate_trans_id();
    let mut response = Response::builder()
        .header("x-object-manifest", &manifest_value)
        .header("x-trans-id", &trans_id)
        .header("x-openstack-request-id", &trans_id);

    if let Some((start, end)) = range {
        response = response
            .status(StatusCode::PARTIAL_CONTENT)
            .header("content-range", format!("bytes {}-{}/{}", start, end, total_size))
            .header("content-length", segment_data.len().to_string());
    } else {
        response = response
            .status(StatusCode::OK)
            .header("content-length", segment_data.len().to_string());
    }

    // Get content-type from first segment
    if let Some(first) = segments.first() {
        if let Some(ct) = &first.content_type {
            response = response.header("content-type", ct);
        }
    }

    Ok(response.body(Body::from(segment_data))
        .map_err(|e| SwiftError::InternalServerError(format!("Failed to build response: {}", e)))?)
}

/// Collect segment data by fetching and concatenating segments
async fn collect_dlo_segments(
    account: &str,
    container: &str,
    segments: &[ObjectInfo],
    credentials: &Option<Credentials>,
    range: Option<(u64, u64)>,
) -> Result<Vec<u8>, SwiftError> {
    // Require credentials
    let creds = credentials.as_ref()
        .ok_or_else(|| SwiftError::Unauthorized("Credentials required".to_string()))?;

    // Determine which segments to fetch based on range
    let segments_to_fetch = if let Some((start, end)) = range {
        calculate_dlo_segments_for_range(segments, start, end)?
    } else {
        segments.iter().enumerate().map(|(i, s)| {
            (i, 0, s.size as u64 - 1, s.clone())
        }).collect()
    };

    let mut result = Vec::new();

    // Fetch each segment
    for (_seg_idx, byte_start, byte_end, segment) in segments_to_fetch {
        let range_spec = if byte_start > 0 || byte_end < segment.size as u64 - 1 {
            Some(rustfs_ecstore::store_api::HTTPRangeSpec {
                is_suffix_length: false,
                start: byte_start as i64,
                end: byte_end as i64,
            })
        } else {
            None
        };

        let mut reader = object::get_object(account, container, &segment.name, creds, range_spec).await?;

        use tokio::io::AsyncReadExt;
        reader.stream.read_to_end(&mut result).await
            .map_err(|e| SwiftError::InternalServerError(format!("Failed to read segment: {}", e)))?;
    }

    Ok(result)
}

/// Register DLO by setting object metadata with manifest pointer
pub async fn handle_dlo_register(
    account: &str,
    container: &str,
    object: &str,
    manifest_value: &str,
    credentials: &Option<Credentials>,
) -> Result<Response<Body>, SwiftError> {
    // Validate manifest format
    let _ = parse_dlo_manifest(manifest_value)?;

    // Create/update object with DLO manifest metadata
    // For DLO, we store a zero-byte marker object with metadata
    let mut metadata = HashMap::new();
    metadata.insert("x-object-manifest".to_string(), manifest_value.to_string());

    // Use put_object_with_metadata to store the marker
    object::put_object_with_metadata(
        account,
        container,
        object,
        credentials,
        std::io::Cursor::new(Vec::new()),
        &metadata,
    )
    .await?;

    let trans_id = generate_trans_id();
    Ok(Response::builder()
        .status(StatusCode::CREATED)
        .header("x-trans-id", &trans_id)
        .header("x-openstack-request-id", trans_id)
        .body(Body::empty())
        .map_err(|e| SwiftError::InternalServerError(format!("Failed to build response: {}", e)))?)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_dlo_manifest() {
        let (container, prefix) = parse_dlo_manifest("mycontainer/segments/").unwrap();
        assert_eq!(container, "mycontainer");
        assert_eq!(prefix, "segments/");

        let (container, prefix) = parse_dlo_manifest("mycontainer/a/b/c").unwrap();
        assert_eq!(container, "mycontainer");
        assert_eq!(prefix, "a/b/c");

        assert!(parse_dlo_manifest("invalid").is_err());
    }

    #[test]
    fn test_calculate_dlo_segments_for_range() {
        let segments = vec![
            ObjectInfo {
                name: "seg001".to_string(),
                size: 1000,
                content_type: None,
                etag: None,
            },
            ObjectInfo {
                name: "seg002".to_string(),
                size: 1000,
                content_type: None,
                etag: None,
            },
            ObjectInfo {
                name: "seg003".to_string(),
                size: 1000,
                content_type: None,
                etag: None,
            },
        ];

        // Request bytes 500-1500 (spans seg1 and seg2)
        let result = calculate_dlo_segments_for_range(&segments, 500, 1500).unwrap();
        assert_eq!(result.len(), 2);
        assert_eq!(result[0].1, 500); // Start at byte 500 of seg1
        assert_eq!(result[0].2, 999); // End at byte 999 of seg1
        assert_eq!(result[1].1, 0);   // Start at byte 0 of seg2
        assert_eq!(result[1].2, 500); // End at byte 500 of seg2
    }

    #[test]
    fn test_parse_range_header() {
        assert_eq!(parse_range_header("bytes=0-999", 10000).unwrap(), (0, 999));
        assert_eq!(parse_range_header("bytes=1000-1999", 10000).unwrap(), (1000, 1999));
        assert_eq!(parse_range_header("bytes=0-", 10000).unwrap(), (0, 9999));
        assert_eq!(parse_range_header("bytes=-500", 10000).unwrap(), (9500, 9999));
    }
}
