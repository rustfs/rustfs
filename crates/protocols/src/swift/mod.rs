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

//! OpenStack Swift API implementation
//!
//! This module provides support for the OpenStack Swift object storage API,
//! enabling RustFS to serve as a Swift-compatible storage backend while
//! reusing the existing S3 storage layer.
//!
//! # Architecture
//!
//! Swift requests follow the pattern: `/v1/{account}/{container}/{object}`
//! where:
//! - `account`: Tenant identifier (e.g., `AUTH_{project_id}`)
//! - `container`: Swift container (maps to S3 bucket)
//! - `object`: Object key (maps to S3 object key)
//!
//! # Authentication
//!
//! Swift API uses Keystone token-based authentication via the existing
//! `KeystoneAuthMiddleware`. The middleware validates X-Auth-Token headers
//! and stores credentials in task-local storage, which Swift handlers access
//! to enforce tenant isolation.

pub mod account;
pub mod acl;
pub mod bulk;
pub mod container;
pub mod cors;
pub mod dlo;
pub mod errors;
pub mod expiration;
pub mod expiration_worker;
pub mod formpost;
pub mod handler;
pub mod metadata_update;
pub mod object;
pub mod quota;
pub mod router;
pub mod slo;
pub mod staticweb;
mod storage_api;
pub mod symlink;
pub mod sync;
pub mod tempurl;
pub mod types;
pub mod versioning;

pub use errors::{SwiftError, SwiftResult};
pub use metadata_update::MetadataUpdate;
pub use router::{SwiftRoute, SwiftRouter};

/// Maximum number of metadata headers allowed per resource (Swift standard)
pub(crate) const MAX_METADATA_COUNT: usize = 90;

/// Maximum size in bytes for a single metadata value (Swift standard)
pub(crate) const MAX_METADATA_VALUE_SIZE: usize = 256;

/// Validate metadata against Swift limits
///
/// Checks that:
/// - Total number of metadata entries doesn't exceed MAX_METADATA_COUNT
/// - Individual metadata values don't exceed MAX_METADATA_VALUE_SIZE
///
/// This is the object-metadata form, where a POST replaces the whole set and
/// the request is therefore the complete result. Account and container POSTs
/// are additive, so their count has to be measured against the merged state
/// instead — see [`MetadataUpdate::apply_to_tags`].
///
/// Returns error if limits are exceeded.
pub(crate) fn validate_metadata(metadata: &std::collections::HashMap<String, String>) -> SwiftResult<()> {
    // Check total metadata count
    if metadata.len() > MAX_METADATA_COUNT {
        return Err(SwiftError::BadRequest(format!(
            "Too many metadata headers: {} (max: {})",
            metadata.len(),
            MAX_METADATA_COUNT
        )));
    }

    // Check individual value sizes
    for (key, value) in metadata.iter() {
        if value.len() > MAX_METADATA_VALUE_SIZE {
            return Err(SwiftError::BadRequest(format!(
                "Metadata value for '{}' too large: {} bytes (max: {} bytes)",
                key,
                value.len(),
                MAX_METADATA_VALUE_SIZE
            )));
        }
    }

    Ok(())
}

// Note: Container, Object, and SwiftMetadata types used by Swift implementation
pub use storage_api::public_api::{SwiftGetObjectReader, SwiftObjectInfo, SwiftObjectOptions, SwiftPutObjReader};
pub(crate) use storage_api::public_api::{
    get_swift_bucket_metadata, get_swift_bucket_usage, resolve_swift_object_store_handle, update_swift_bucket_tagging,
};
#[allow(unused_imports)]
pub use types::{Container, Object, SwiftMetadata};
