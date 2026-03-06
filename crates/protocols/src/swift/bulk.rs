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

//! Bulk Operations for Swift API
//!
//! This module implements bulk operations that allow batch processing of
//! multiple objects in a single request, improving efficiency for operations
//! that affect many files.
//!
//! # Operations
//!
//! ## Bulk Delete
//!
//! Delete multiple objects in a single request.
//!
//! **Endpoint**: `DELETE /?bulk-delete`
//!
//! **Request Body**: Newline-separated list of object paths
//! ```text
//! /container1/object1.txt
//! /container2/folder/object2.txt
//! /container1/object3.txt
//! ```
//!
//! **Response**: JSON with results for each object
//! ```json
//! {
//!   "Number Deleted": 2,
//!   "Number Not Found": 1,
//!   "Errors": [],
//!   "Response Status": "200 OK",
//!   "Response Body": ""
//! }
//! ```
//!
//! ## Bulk Extract
//!
//! Extract files from an uploaded archive into a container.
//!
//! **Endpoint**: `PUT /{container}?extract-archive=tar` (or tar.gz, tar.bz2)
//!
//! **Request Body**: Archive file contents
//!
//! **Response**: JSON with results for each extracted file
//! ```json
//! {
//!   "Number Files Created": 10,
//!   "Errors": [],
//!   "Response Status": "201 Created",
//!   "Response Body": ""
//! }
//! ```
//!
//! # Examples
//!
//! ```bash
//! # Bulk delete
//! echo -e "/container/file1.txt\n/container/file2.txt" | \
//!   swift delete --bulk
//!
//! # Bulk extract
//! tar czf archive.tar.gz files/
//! swift upload container --extract-archive archive.tar.gz
//! ```

use super::{SwiftError, SwiftResult, container, object};
use axum::http::{Response, StatusCode};
use rustfs_credentials::Credentials;
use s3s::Body;
use serde::{Deserialize, Serialize};
use std::io::Read;
use tracing::{debug, error};

/// Result of a single delete operation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeleteResult {
    /// Object path that was deleted
    pub path: String,

    /// HTTP status code for this operation
    pub status: u16,

    /// Error message if deletion failed
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

/// Bulk delete response
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BulkDeleteResponse {
    /// Number of objects successfully deleted
    #[serde(rename = "Number Deleted")]
    pub number_deleted: usize,

    /// Number of objects not found
    #[serde(rename = "Number Not Found")]
    pub number_not_found: usize,

    /// List of errors encountered
    #[serde(rename = "Errors")]
    pub errors: Vec<Vec<String>>,

    /// Overall response status
    #[serde(rename = "Response Status")]
    pub response_status: String,

    /// Response body (usually empty)
    #[serde(rename = "Response Body")]
    pub response_body: String,
}

impl Default for BulkDeleteResponse {
    fn default() -> Self {
        Self {
            number_deleted: 0,
            number_not_found: 0,
            errors: Vec::new(),
            response_status: "200 OK".to_string(),
            response_body: String::new(),
        }
    }
}

/// Result of a single file extraction
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExtractResult {
    /// File path that was extracted
    pub path: String,

    /// HTTP status code for this operation
    pub status: u16,

    /// Error message if extraction failed
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

/// Bulk extract response
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BulkExtractResponse {
    /// Number of files successfully created
    #[serde(rename = "Number Files Created")]
    pub number_files_created: usize,

    /// List of errors encountered
    #[serde(rename = "Errors")]
    pub errors: Vec<Vec<String>>,

    /// Overall response status
    #[serde(rename = "Response Status")]
    pub response_status: String,

    /// Response body (usually empty)
    #[serde(rename = "Response Body")]
    pub response_body: String,
}

impl Default for BulkExtractResponse {
    fn default() -> Self {
        Self {
            number_files_created: 0,
            errors: Vec::new(),
            response_status: "201 Created".to_string(),
            response_body: String::new(),
        }
    }
}

/// Parse object path from bulk delete request
///
/// Paths should be in format: /container/object
fn parse_object_path(path: &str) -> SwiftResult<(String, String)> {
    let path = path.trim();

    if path.is_empty() {
        return Err(SwiftError::BadRequest("Empty path in bulk delete".to_string()));
    }

    // Remove leading slash
    let path = path.trim_start_matches('/');

    // Split into container and object
    let parts: Vec<&str> = path.splitn(2, '/').collect();

    if parts.len() != 2 {
        return Err(SwiftError::BadRequest(format!(
            "Invalid path format: {}. Expected /container/object",
            path
        )));
    }

    if parts[0].is_empty() || parts[1].is_empty() {
        return Err(SwiftError::BadRequest(format!("Empty container or object name in path: {}", path)));
    }

    Ok((parts[0].to_string(), parts[1].to_string()))
}

/// Handle bulk delete request
///
/// Deletes multiple objects specified in the request body
pub async fn handle_bulk_delete(account: &str, body: String, credentials: &Credentials) -> SwiftResult<Response<Body>> {
    debug!("Bulk delete request for account: {}", account);

    let mut response = BulkDeleteResponse::default();
    let mut delete_results = Vec::new();

    // Parse paths from body (newline-separated)
    let paths: Vec<&str> = body.lines().filter(|line| !line.trim().is_empty()).collect();

    if paths.is_empty() {
        return Err(SwiftError::BadRequest("No paths provided for bulk delete".to_string()));
    }

    debug!("Processing {} delete requests", paths.len());

    // Process each path
    for path in paths {
        let result = match parse_object_path(path) {
            Ok((container, object_key)) => {
                // Attempt to delete the object
                match object::delete_object(account, &container, &object_key, credentials).await {
                    Ok(_) => {
                        response.number_deleted += 1;
                        DeleteResult {
                            path: path.to_string(),
                            status: 204,
                            error: None,
                        }
                    }
                    Err(SwiftError::NotFound(_)) => {
                        response.number_not_found += 1;
                        DeleteResult {
                            path: path.to_string(),
                            status: 404,
                            error: Some("Not Found".to_string()),
                        }
                    }
                    Err(e) => {
                        error!("Error deleting {}: {}", path, e);
                        response.errors.push(vec![path.to_string(), e.to_string()]);
                        DeleteResult {
                            path: path.to_string(),
                            status: 500,
                            error: Some(e.to_string()),
                        }
                    }
                }
            }
            Err(e) => {
                error!("Invalid path {}: {}", path, e);
                response.errors.push(vec![path.to_string(), e.to_string()]);
                DeleteResult {
                    path: path.to_string(),
                    status: 400,
                    error: Some(e.to_string()),
                }
            }
        };

        delete_results.push(result);
    }

    // Determine overall status
    if !response.errors.is_empty() {
        response.response_status = "400 Bad Request".to_string();
    }

    // Serialize response
    let json = serde_json::to_string(&response)
        .map_err(|e| SwiftError::InternalServerError(format!("JSON serialization failed: {}", e)))?;

    let trans_id = super::handler::generate_trans_id();
    Response::builder()
        .status(StatusCode::OK)
        .header("content-type", "application/json; charset=utf-8")
        .header("content-length", json.len().to_string())
        .header("x-trans-id", trans_id.clone())
        .header("x-openstack-request-id", trans_id)
        .body(Body::from(json))
        .map_err(|e| SwiftError::InternalServerError(format!("Failed to build response: {}", e)))
}

/// Archive format supported for bulk extract
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ArchiveFormat {
    /// Uncompressed tar
    Tar,
    /// Gzip-compressed tar
    TarGz,
    /// Bzip2-compressed tar
    TarBz2,
}

impl ArchiveFormat {
    /// Parse archive format from query parameter
    pub fn from_query(query: &str) -> SwiftResult<Self> {
        match query {
            "tar" => Ok(ArchiveFormat::Tar),
            "tar.gz" | "tgz" => Ok(ArchiveFormat::TarGz),
            "tar.bz2" | "tbz2" | "tbz" => Ok(ArchiveFormat::TarBz2),
            _ => Err(SwiftError::BadRequest(format!(
                "Unsupported archive format: {}. Supported: tar, tar.gz, tar.bz2",
                query
            ))),
        }
    }
}

/// Handle bulk extract request
///
/// Extracts files from an uploaded archive into the specified container
pub async fn handle_bulk_extract(
    account: &str,
    container: &str,
    format: ArchiveFormat,
    body: Vec<u8>,
    credentials: &Credentials,
) -> SwiftResult<Response<Body>> {
    debug!("Bulk extract request for container: {}, format: {:?}", container, format);

    let mut response = BulkExtractResponse::default();

    // Verify container exists
    if container::get_container_metadata(account, container, credentials)
        .await
        .is_err()
    {
        return Err(SwiftError::NotFound(format!("Container not found: {}", container)));
    }

    // Parse archive and collect entries (without holding the archive)
    let entries = extract_tar_entries(format, body)?;

    // Now upload each entry (async operations)
    for (path_str, contents) in entries {
        // Upload file to container
        match object::put_object(
            account,
            container,
            &path_str,
            credentials,
            std::io::Cursor::new(contents),
            &axum::http::HeaderMap::new(),
        )
        .await
        {
            Ok(_) => {
                response.number_files_created += 1;
                debug!("Extracted: {}", path_str);
            }
            Err(e) => {
                error!("Failed to upload {}: {}", path_str, e);
                response.errors.push(vec![path_str.clone(), e.to_string()]);
            }
        }
    }

    // Determine overall status
    if response.number_files_created == 0 {
        response.response_status = "400 Bad Request".to_string();
    } else if !response.errors.is_empty() {
        response.response_status = "201 Created".to_string();
    }

    // Serialize response
    let json = serde_json::to_string(&response)
        .map_err(|e| SwiftError::InternalServerError(format!("JSON serialization failed: {}", e)))?;

    let trans_id = super::handler::generate_trans_id();
    let status = if response.number_files_created > 0 {
        StatusCode::CREATED
    } else {
        StatusCode::BAD_REQUEST
    };

    Response::builder()
        .status(status)
        .header("content-type", "application/json; charset=utf-8")
        .header("content-length", json.len().to_string())
        .header("x-trans-id", trans_id.clone())
        .header("x-openstack-request-id", trans_id)
        .body(Body::from(json))
        .map_err(|e| SwiftError::InternalServerError(format!("Failed to build response: {}", e)))
}

/// Extract tar entries synchronously to avoid Send issues
fn extract_tar_entries(format: ArchiveFormat, body: Vec<u8>) -> SwiftResult<Vec<(String, Vec<u8>)>> {
    // Create appropriate reader based on format
    let reader: Box<dyn Read> = match format {
        ArchiveFormat::Tar => Box::new(std::io::Cursor::new(body)),
        ArchiveFormat::TarGz => {
            let cursor = std::io::Cursor::new(body);
            Box::new(flate2::read::GzDecoder::new(cursor))
        }
        ArchiveFormat::TarBz2 => {
            let cursor = std::io::Cursor::new(body);
            Box::new(bzip2::read::BzDecoder::new(cursor))
        }
    };

    // Parse tar archive
    let mut archive = tar::Archive::new(reader);
    let mut entries = Vec::new();

    // Extract each entry
    for entry in archive
        .entries()
        .map_err(|e| SwiftError::BadRequest(format!("Failed to read tar archive: {}", e)))?
    {
        let mut entry = entry.map_err(|e| SwiftError::BadRequest(format!("Failed to read tar entry: {}", e)))?;

        // Get entry path
        let path = entry
            .path()
            .map_err(|e| SwiftError::BadRequest(format!("Invalid path in tar entry: {}", e)))?;

        let path_str = path.to_string_lossy().to_string();

        // Skip directories
        if entry.header().entry_type().is_dir() {
            debug!("Skipping directory: {}", path_str);
            continue;
        }

        // Read file contents
        let mut contents = Vec::new();
        if let Err(e) = entry.read_to_end(&mut contents) {
            error!("Failed to read tar entry {}: {}", path_str, e);
            continue;
        }

        entries.push((path_str, contents));
    }

    Ok(entries)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_object_path() {
        // Valid paths
        assert_eq!(
            parse_object_path("/container/object.txt").unwrap(),
            ("container".to_string(), "object.txt".to_string())
        );

        assert_eq!(
            parse_object_path("container/folder/object.txt").unwrap(),
            ("container".to_string(), "folder/object.txt".to_string())
        );

        assert_eq!(
            parse_object_path("/my-container/path/to/file.txt").unwrap(),
            ("my-container".to_string(), "path/to/file.txt".to_string())
        );

        // With whitespace
        assert_eq!(
            parse_object_path("  /container/object.txt  ").unwrap(),
            ("container".to_string(), "object.txt".to_string())
        );
    }

    #[test]
    fn test_parse_object_path_invalid() {
        // Empty path
        assert!(parse_object_path("").is_err());
        assert!(parse_object_path("   ").is_err());

        // Missing object
        assert!(parse_object_path("/container").is_err());
        assert!(parse_object_path("/container/").is_err());

        // Missing container
        assert!(parse_object_path("/").is_err());
        assert!(parse_object_path("//object").is_err());
    }

    #[test]
    fn test_archive_format_from_query() {
        assert_eq!(ArchiveFormat::from_query("tar").unwrap(), ArchiveFormat::Tar);
        assert_eq!(ArchiveFormat::from_query("tar.gz").unwrap(), ArchiveFormat::TarGz);
        assert_eq!(ArchiveFormat::from_query("tgz").unwrap(), ArchiveFormat::TarGz);
        assert_eq!(ArchiveFormat::from_query("tar.bz2").unwrap(), ArchiveFormat::TarBz2);
        assert_eq!(ArchiveFormat::from_query("tbz2").unwrap(), ArchiveFormat::TarBz2);
        assert_eq!(ArchiveFormat::from_query("tbz").unwrap(), ArchiveFormat::TarBz2);

        // Invalid formats
        assert!(ArchiveFormat::from_query("zip").is_err());
        assert!(ArchiveFormat::from_query("rar").is_err());
        assert!(ArchiveFormat::from_query("").is_err());
    }

    #[test]
    fn test_bulk_delete_response_default() {
        let response = BulkDeleteResponse::default();
        assert_eq!(response.number_deleted, 0);
        assert_eq!(response.number_not_found, 0);
        assert!(response.errors.is_empty());
        assert_eq!(response.response_status, "200 OK");
        assert!(response.response_body.is_empty());
    }

    #[test]
    fn test_bulk_extract_response_default() {
        let response = BulkExtractResponse::default();
        assert_eq!(response.number_files_created, 0);
        assert!(response.errors.is_empty());
        assert_eq!(response.response_status, "201 Created");
        assert!(response.response_body.is_empty());
    }

    #[test]
    fn test_parse_multiple_paths() {
        let body = "/container1/file1.txt\n/container2/file2.txt\n/container1/folder/file3.txt";
        let paths: Vec<&str> = body.lines().collect();

        assert_eq!(paths.len(), 3);

        let (c1, o1) = parse_object_path(paths[0]).unwrap();
        assert_eq!(c1, "container1");
        assert_eq!(o1, "file1.txt");

        let (c2, o2) = parse_object_path(paths[1]).unwrap();
        assert_eq!(c2, "container2");
        assert_eq!(o2, "file2.txt");

        let (c3, o3) = parse_object_path(paths[2]).unwrap();
        assert_eq!(c3, "container1");
        assert_eq!(o3, "folder/file3.txt");
    }

    #[test]
    fn test_parse_paths_with_empty_lines() {
        let body = "/container1/file1.txt\n\n/container2/file2.txt\n   \n/container1/file3.txt";
        let paths: Vec<&str> = body.lines().filter(|line| !line.trim().is_empty()).collect();

        assert_eq!(paths.len(), 3);
    }
}
