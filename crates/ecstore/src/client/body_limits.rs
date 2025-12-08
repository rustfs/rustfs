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

//! Response body size limits for S3 client operations
//!
//! These limits prevent DoS attacks when reading responses from remote
//! S3-compatible services while allowing legitimate use cases.

/// Maximum size for S3 API response bodies from external services (10 MB)
/// Used for: ACL responses, object attributes, list responses, location queries
/// 
/// Rationale: Responses from external S3-compatible services should be bounded.
/// - ACL XML responses: typically < 10KB
/// - Object attributes: typically < 100KB  
/// - List responses: typically < 1MB (1000 objects with metadata)
/// - Location/error responses: typically < 10KB
///
/// 10MB provides generous headroom for legitimate responses while preventing
/// memory exhaustion from malicious or misconfigured remote services.
pub const MAX_S3_RESPONSE_SIZE: usize = 10 * 1024 * 1024; // 10 MB
