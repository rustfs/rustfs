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

//! Static Large Objects (SLO) support for Swift API
//!
//! SLO provides manifest-based multi-part file uploads with validation.
//! Large files (>5GB) are split into segments, and a manifest defines
//! how segments are assembled on download.

use super::{SwiftError, object};
use axum::http::{HeaderMap, Response, StatusCode};
use rustfs_credentials::Credentials;
use s3s::Body;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::io::Cursor;

/// SLO manifest segment descriptor
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SLOSegment {
    /// Segment path: "/container/object"
    pub path: String,

    /// Segment size in bytes (must match actual)
    #[serde(rename = "size_bytes")]
    pub size_bytes: u64,

    /// MD5 ETag of segment (must match actual)
    pub etag: String,

    /// Optional: byte range within segment
    #[serde(skip_serializing_if = "Option::is_none")]
    pub range: Option<String>,
}

/// SLO manifest document
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SLOManifest {
    /// List of segments in assembly order
    #[serde(default)]
    pub segments: Vec<SLOSegment>,

    /// Manifest creation timestamp
    #[serde(skip_serializing_if = "Option::is_none")]
    pub created_at: Option<String>,
}

impl SLOManifest {
    /// Parse manifest from JSON body
    pub fn from_json(data: &[u8]) -> Result<Self, SwiftError> {
        serde_json::from_slice(data)
            .map_err(|e| SwiftError::BadRequest(format!("Invalid SLO manifest: {}", e)))
    }

    /// Calculate total assembled size
    pub fn total_size(&self) -> u64 {
        self.segments.iter().map(|s| s.size_bytes).sum()
    }

    /// Calculate SLO ETag: "{MD5(concat_etags)}-{count}"
    pub fn calculate_etag(&self) -> String {
        // Concatenate all ETags
        let mut etag_concat = String::new();
        for seg in &self.segments {
            // Remove quotes from etag if present
            let etag = seg.etag.trim_matches('"');
            etag_concat.push_str(etag);
        }

        // Calculate MD5 hash
        let hash = md5::compute(etag_concat.as_bytes());
        format!("\"{:x}-{}\"", hash, self.segments.len())
    }

    /// Validate manifest against actual segments
    pub async fn validate(&self, account: &str, credentials: &Credentials) -> Result<(), SwiftError> {
        if self.segments.is_empty() {
            return Err(SwiftError::BadRequest("SLO manifest must contain at least one segment".to_string()));
        }

        for segment in &self.segments {
            // Parse path: "/container/object"
            let (container, object_name) = parse_segment_path(&segment.path)?;

            // HEAD segment to get actual ETag and size
            let info = object::head_object(account, &container, &object_name, credentials).await?;

            // Validate ETag match (remove quotes for comparison)
            let expected_etag = segment.etag.trim_matches('"');
            let actual_etag = info.etag.as_deref().unwrap_or("").trim_matches('"');

            if actual_etag != expected_etag {
                return Err(SwiftError::Conflict(format!(
                    "Segment {} ETag mismatch: expected {}, got {}",
                    segment.path, expected_etag, actual_etag
                )));
            }

            // Validate size match
            if info.size != segment.size_bytes as i64 {
                return Err(SwiftError::Conflict(format!(
                    "Segment {} size mismatch: expected {}, got {}",
                    segment.path, segment.size_bytes, info.size
                )));
            }
        }
        Ok(())
    }
}

/// Parse segment path "/container/object" into (container, object)
fn parse_segment_path(path: &str) -> Result<(String, String), SwiftError> {
    let path = path.trim_start_matches('/');
    let parts: Vec<&str> = path.splitn(2, '/').collect();

    if parts.len() != 2 {
        return Err(SwiftError::BadRequest(format!("Invalid segment path: {}", path)));
    }

    Ok((parts[0].to_string(), parts[1].to_string()))
}

/// Check if object is an SLO by querying metadata
pub async fn is_slo_object(
    account: &str,
    container: &str,
    object: &str,
    credentials: &Option<Credentials>,
) -> Result<bool, SwiftError> {
    // Require credentials
    let creds = credentials.as_ref()
        .ok_or_else(|| SwiftError::Unauthorized("Credentials required".to_string()))?;

    let info = object::head_object(account, container, object, creds).await?;
    Ok(info.user_defined.get("x-swift-slo").map(|v| v == "true").unwrap_or(false))
}

/// Generate transaction ID for response headers
fn generate_trans_id() -> String {
    format!("tx{}", uuid::Uuid::new_v4().as_simple())
}

/// Calculate which segments and byte ranges to fetch for a given range request
fn calculate_segments_for_range(
    manifest: &SLOManifest,
    start: u64,
    end: u64,
) -> Result<Vec<(usize, u64, u64, SLOSegment)>, SwiftError> {
    let mut result = Vec::new();
    let mut current_offset = 0u64;

    for (idx, segment) in manifest.segments.iter().enumerate() {
        let segment_start = current_offset;
        let segment_end = current_offset + segment.size_bytes - 1;

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
                segment.size_bytes - 1
            };

            result.push((idx, byte_start, byte_end, segment.clone()));
        }

        current_offset += segment.size_bytes;

        // Stop if we've passed the requested range
        if current_offset > end {
            break;
        }
    }

    Ok(result)
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

    let start = if parts[0].is_empty() {
        // Suffix range (last N bytes)
        let suffix: u64 = parts[1].parse()
            .map_err(|_| SwiftError::BadRequest("Invalid Range header".to_string()))?;
        if suffix >= total_size {
            0
        } else {
            total_size - suffix
        }
    } else {
        parts[0].parse()
            .map_err(|_| SwiftError::BadRequest("Invalid Range header".to_string()))?
    };

    let end = if parts[1].is_empty() {
        total_size - 1
    } else {
        let parsed: u64 = parts[1].parse()
            .map_err(|_| SwiftError::BadRequest("Invalid Range header".to_string()))?;
        std::cmp::min(parsed, total_size - 1)
    };

    if start > end {
        return Err(SwiftError::BadRequest("Invalid Range: start > end".to_string()));
    }

    Ok((start, end))
}

/// Handle PUT /v1/{account}/{container}/{object}?multipart-manifest=put
pub async fn handle_slo_put<B>(
    account: &str,
    container: &str,
    object: &str,
    body: B,
    headers: &HeaderMap,
    credentials: &Option<Credentials>,
) -> Result<Response<Body>, SwiftError>
where
    B: http_body::Body,
    B::Error: std::fmt::Display,
{
    use http_body_util::BodyExt;

    // Require credentials
    let creds = credentials.as_ref()
        .ok_or_else(|| SwiftError::Unauthorized("Credentials required for SLO operations".to_string()))?;

    // 1. Read manifest JSON from body
    let manifest_bytes = body
        .collect()
        .await
        .map_err(|e| SwiftError::BadRequest(format!("Failed to read manifest: {}", e)))?
        .to_bytes();

    // 2. Parse manifest
    let manifest = SLOManifest::from_json(&manifest_bytes)?;

    // 3. Validate manifest size (2MB limit)
    if manifest_bytes.len() > 2 * 1024 * 1024 {
        return Err(SwiftError::BadRequest("Manifest exceeds 2MB".to_string()));
    }

    // 4. Validate segments exist and match ETags/sizes
    manifest.validate(account, creds).await?;

    // 5. Store manifest as S3 object with metadata marker
    let mut metadata = HashMap::new();
    metadata.insert("x-swift-slo".to_string(), "true".to_string());
    metadata.insert("x-slo-etag".to_string(), manifest.calculate_etag().trim_matches('"').to_string());
    metadata.insert("x-slo-size".to_string(), manifest.total_size().to_string());

    // Extract custom headers (X-Object-Meta-*)
    for (key, value) in headers {
        if key.as_str().starts_with("x-object-meta-") {
            if let Ok(v) = value.to_str() {
                metadata.insert(key.to_string(), v.to_string());
            }
        }
    }

    // Store manifest JSON as object content with special key
    let manifest_key = format!("{}.slo-manifest", object);
    object::put_object_with_metadata(
        account,
        container,
        &manifest_key,
        credentials,
        Cursor::new(manifest_bytes.to_vec()),
        &metadata,
    )
    .await?;

    // 6. Create zero-byte marker object at original path
    object::put_object_with_metadata(
        account,
        container,
        object,
        credentials,
        Cursor::new(Vec::new()),
        &metadata,
    )
    .await?;

    // 7. Return response
    let trans_id = generate_trans_id();
    Ok(Response::builder()
        .status(StatusCode::CREATED)
        .header("etag", manifest.calculate_etag())
        .header("x-trans-id", &trans_id)
        .header("x-openstack-request-id", trans_id)
        .body(Body::empty())
        .map_err(|e| SwiftError::InternalServerError(format!("Failed to build response: {}", e)))?)
}

/// Handle GET /v1/{account}/{container}/{object} for SLO
pub async fn handle_slo_get(
    account: &str,
    container: &str,
    object: &str,
    headers: &HeaderMap,
    credentials: &Option<Credentials>,
) -> Result<Response<Body>, SwiftError> {
    // Require credentials
    let creds = credentials.as_ref()
        .ok_or_else(|| SwiftError::Unauthorized("Credentials required for SLO operations".to_string()))?;

    // 1. Load manifest
    let manifest_key = format!("{}.slo-manifest", object);
    let mut manifest_reader = object::get_object(account, container, &manifest_key, creds, None).await?;

    // Read manifest bytes
    let mut manifest_bytes = Vec::new();
    use tokio::io::AsyncReadExt;
    manifest_reader.stream.read_to_end(&mut manifest_bytes).await
        .map_err(|e| SwiftError::InternalServerError(format!("Failed to read manifest: {}", e)))?;

    let manifest = SLOManifest::from_json(&manifest_bytes)?;

    // 2. Parse Range header if present
    let range = headers.get("range")
        .and_then(|v| v.to_str().ok())
        .and_then(|r| parse_range_header(r, manifest.total_size()).ok());

    // 3. Collect segment data
    let segment_data = collect_slo_segments(account, &manifest, credentials, range).await?;

    // 4. Build response
    let trans_id = generate_trans_id();
    let mut response = Response::builder()
        .header("x-static-large-object", "true")
        .header("etag", manifest.calculate_etag())
        .header("x-trans-id", &trans_id)
        .header("x-openstack-request-id", &trans_id);

    if let Some((start, end)) = range {
        response = response
            .status(StatusCode::PARTIAL_CONTENT)
            .header("content-range", format!("bytes {}-{}/{}", start, end, manifest.total_size()))
            .header("content-length", segment_data.len().to_string());
    } else {
        response = response
            .status(StatusCode::OK)
            .header("content-length", segment_data.len().to_string());
    }

    Ok(response.body(Body::from(segment_data))
        .map_err(|e| SwiftError::InternalServerError(format!("Failed to build response: {}", e)))?)
}

/// Collect segment data by fetching and concatenating segments
async fn collect_slo_segments(
    account: &str,
    manifest: &SLOManifest,
    credentials: &Option<Credentials>,
    range: Option<(u64, u64)>,
) -> Result<Vec<u8>, SwiftError> {
    // Require credentials
    let creds = credentials.as_ref()
        .ok_or_else(|| SwiftError::Unauthorized("Credentials required".to_string()))?;

    // Determine which segments to fetch based on range
    let segments_to_fetch = if let Some((start, end)) = range {
        calculate_segments_for_range(manifest, start, end)?
    } else {
        // All segments, full range
        manifest.segments.iter().enumerate().map(|(i, s)| {
            (i, 0, s.size_bytes - 1, s.clone())
        }).collect()
    };

    let mut result = Vec::new();

    // Fetch each segment
    for (_seg_idx, byte_start, byte_end, segment) in segments_to_fetch {
        let (container, object_name) = parse_segment_path(&segment.path)?;

        // Fetch segment with range
        let range_spec = if byte_start > 0 || byte_end < segment.size_bytes - 1 {
            Some(rustfs_ecstore::store_api::HTTPRangeSpec {
                is_suffix_length: false,
                start: byte_start as i64,
                end: byte_end as i64,
            })
        } else {
            None
        };

        let mut reader = object::get_object(account, &container, &object_name, creds, range_spec).await?;

        // Read segment into buffer
        use tokio::io::AsyncReadExt;
        reader.stream.read_to_end(&mut result).await
            .map_err(|e| SwiftError::InternalServerError(format!("Failed to read segment: {}", e)))?;
    }

    Ok(result)
}

/// Handle GET /v1/{account}/{container}/{object}?multipart-manifest=get (return manifest JSON)
pub async fn handle_slo_get_manifest(
    account: &str,
    container: &str,
    object: &str,
    credentials: &Option<Credentials>,
) -> Result<Response<Body>, SwiftError> {
    // Require credentials
    let creds = credentials.as_ref()
        .ok_or_else(|| SwiftError::Unauthorized("Credentials required for SLO operations".to_string()))?;

    // Load and return the manifest JSON directly
    let manifest_key = format!("{}.slo-manifest", object);
    let mut manifest_reader = object::get_object(account, container, &manifest_key, creds, None).await?;

    // Read manifest bytes
    let mut manifest_bytes = Vec::new();
    use tokio::io::AsyncReadExt;
    manifest_reader.stream.read_to_end(&mut manifest_bytes).await
        .map_err(|e| SwiftError::InternalServerError(format!("Failed to read manifest: {}", e)))?;

    let trans_id = generate_trans_id();
    Ok(Response::builder()
        .status(StatusCode::OK)
        .header("content-type", "application/json; charset=utf-8")
        .header("content-length", manifest_bytes.len().to_string())
        .header("x-trans-id", &trans_id)
        .header("x-openstack-request-id", trans_id)
        .body(Body::from(manifest_bytes))
        .map_err(|e| SwiftError::InternalServerError(format!("Failed to build response: {}", e)))?)
}

/// Handle DELETE ?multipart-manifest=delete (remove manifest + all segments)
pub async fn handle_slo_delete(
    account: &str,
    container: &str,
    object: &str,
    credentials: &Option<Credentials>,
) -> Result<Response<Body>, SwiftError> {
    // Require credentials for delete operations
    let creds = credentials.as_ref()
        .ok_or_else(|| SwiftError::Unauthorized("Credentials required for SLO delete".to_string()))?;

    // 1. Load manifest
    let manifest_key = format!("{}.slo-manifest", object);
    let mut manifest_reader = object::get_object(account, container, &manifest_key, creds, None).await?;

    // Read manifest bytes
    let mut manifest_bytes = Vec::new();
    use tokio::io::AsyncReadExt;
    manifest_reader.stream.read_to_end(&mut manifest_bytes).await
        .map_err(|e| SwiftError::InternalServerError(format!("Failed to read manifest: {}", e)))?;

    let manifest = SLOManifest::from_json(&manifest_bytes)?;

    // 2. Delete all segments
    for segment in &manifest.segments {
        let (seg_container, seg_object) = parse_segment_path(&segment.path)?;
        // Ignore errors if segment doesn't exist (idempotent delete)
        let _ = object::delete_object(account, &seg_container, &seg_object, creds).await;
    }

    // 3. Delete manifest object
    object::delete_object(account, container, &manifest_key, creds).await?;

    // 4. Delete marker object
    object::delete_object(account, container, object, creds).await?;

    let trans_id = generate_trans_id();
    Ok(Response::builder()
        .status(StatusCode::NO_CONTENT)
        .header("x-trans-id", &trans_id)
        .header("x-openstack-request-id", trans_id)
        .body(Body::empty())
        .map_err(|e| SwiftError::InternalServerError(format!("Failed to build response: {}", e)))?)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_segment_path() {
        let (container, object) = parse_segment_path("/mycontainer/myobject").unwrap();
        assert_eq!(container, "mycontainer");
        assert_eq!(object, "myobject");

        let (container, object) = parse_segment_path("mycontainer/path/to/object").unwrap();
        assert_eq!(container, "mycontainer");
        assert_eq!(object, "path/to/object");

        assert!(parse_segment_path("invalid").is_err());
    }

    #[test]
    fn test_slo_manifest_total_size() {
        let manifest = SLOManifest {
            segments: vec![
                SLOSegment {
                    path: "/container/seg1".to_string(),
                    size_bytes: 1000,
                    etag: "abc123".to_string(),
                    range: None,
                },
                SLOSegment {
                    path: "/container/seg2".to_string(),
                    size_bytes: 2000,
                    etag: "def456".to_string(),
                    range: None,
                },
            ],
            created_at: None,
        };

        assert_eq!(manifest.total_size(), 3000);
    }

    #[test]
    fn test_calculate_etag() {
        let manifest = SLOManifest {
            segments: vec![
                SLOSegment {
                    path: "/container/seg1".to_string(),
                    size_bytes: 1000,
                    etag: "abc123".to_string(),
                    range: None,
                },
            ],
            created_at: None,
        };

        let etag = manifest.calculate_etag();
        assert!(etag.starts_with('"'));
        assert!(etag.ends_with("-1\""));
    }

    #[test]
    fn test_parse_range_header() {
        assert_eq!(parse_range_header("bytes=0-999", 10000).unwrap(), (0, 999));
        assert_eq!(parse_range_header("bytes=1000-1999", 10000).unwrap(), (1000, 1999));
        assert_eq!(parse_range_header("bytes=0-", 10000).unwrap(), (0, 9999));
        assert_eq!(parse_range_header("bytes=-500", 10000).unwrap(), (9500, 9999));
    }

    #[test]
    fn test_calculate_segments_for_range() {
        let manifest = SLOManifest {
            segments: vec![
                SLOSegment {
                    path: "/c/s1".to_string(),
                    size_bytes: 1000,
                    etag: "e1".to_string(),
                    range: None,
                },
                SLOSegment {
                    path: "/c/s2".to_string(),
                    size_bytes: 1000,
                    etag: "e2".to_string(),
                    range: None,
                },
                SLOSegment {
                    path: "/c/s3".to_string(),
                    size_bytes: 1000,
                    etag: "e3".to_string(),
                    range: None,
                },
            ],
            created_at: None,
        };

        // Request bytes 500-1500 (spans seg1 and seg2)
        let segments = calculate_segments_for_range(&manifest, 500, 1500).unwrap();
        assert_eq!(segments.len(), 2);
        assert_eq!(segments[0].1, 500); // Start at byte 500 of seg1
        assert_eq!(segments[0].2, 999); // End at byte 999 of seg1
        assert_eq!(segments[1].1, 0);   // Start at byte 0 of seg2
        assert_eq!(segments[1].2, 500); // End at byte 500 of seg2
    }
}
