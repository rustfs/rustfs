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

//! Request body size limits for admin API endpoints
//!
//! These limits prevent DoS attacks through unbounded memory allocation
//! while allowing legitimate use cases.

/// Maximum size for standard admin API request bodies (1 MB)
/// Used for: user creation/update, policies, tier config, KMS config, events, groups, service accounts
/// Rationale: Admin API payloads are typically JSON/XML configs under 100KB.
/// AWS IAM policy limit is 6KB-10KB. 1MB provides generous headroom.
pub const MAX_ADMIN_REQUEST_BODY_SIZE: usize = 1024 * 1024; // 1 MB

/// Maximum size for IAM import/export operations (10 MB)
/// Used for: IAM entity imports/exports containing multiple users, policies, groups
/// Rationale: ZIP archives with hundreds of IAM entities. 10MB allows ~10,000 small configs.
pub const MAX_IAM_IMPORT_SIZE: usize = 10 * 1024 * 1024; // 10 MB

/// Maximum size for bucket metadata import operations (100 MB)
/// Used for: Bucket metadata import containing configurations for many buckets
/// Rationale: Large deployments may have thousands of buckets with various configs.
/// 100MB allows importing metadata for ~10,000 buckets with reasonable configs.
pub const MAX_BUCKET_METADATA_IMPORT_SIZE: usize = 100 * 1024 * 1024; // 100 MB

/// Maximum size for healing operation requests (1 MB)
/// Used for: Healing parameters and configuration
/// Rationale: Healing requests contain bucket/object paths and options. Should be small.
pub const MAX_HEAL_REQUEST_SIZE: usize = 1024 * 1024; // 1 MB

/// Maximum size for S3 client response bodies (10 MB)
/// Used for: Reading responses from remote S3-compatible services (ACL, attributes, lists)
/// Rationale: Responses from external services should be bounded.
/// Large responses (>10MB) indicate misconfiguration or potential attack.
/// Typical responses: ACL XML < 10KB, List responses < 1MB
///
/// Rationale: Responses from external S3-compatible services should be bounded.
/// - ACL XML responses: typically < 10KB
/// - Object attributes: typically < 100KB
/// - List responses: typically < 1MB (1000 objects with metadata)
/// - Location/error responses: typically < 10KB
///
/// 10MB provides generous headroom for legitimate responses while preventing
/// memory exhaustion from malicious or misconfigured remote services.
pub const MAX_S3_CLIENT_RESPONSE_SIZE: usize = 10 * 1024 * 1024; // 10 MB
