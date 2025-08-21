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

#![allow(dead_code)]

use crate::entry::ObjectVersion;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Args - defines the arguments for API operations
/// Args is used to define the arguments for API operations.
///
/// # Example
/// ```
/// use rustfs_audit_logger::Args;
/// use std::collections::HashMap;
///
/// let args = Args::new()
///     .set_bucket(Some("my-bucket".to_string()))
///     .set_object(Some("my-object".to_string()))
///     .set_version_id(Some("123".to_string()))
///     .set_metadata(Some(HashMap::new()));
/// ```
#[derive(Debug, Clone, Serialize, Deserialize, Default, Eq, PartialEq)]
pub struct Args {
    #[serde(rename = "bucket", skip_serializing_if = "Option::is_none")]
    pub bucket: Option<String>,
    #[serde(rename = "object", skip_serializing_if = "Option::is_none")]
    pub object: Option<String>,
    #[serde(rename = "versionId", skip_serializing_if = "Option::is_none")]
    pub version_id: Option<String>,
    #[serde(rename = "objects", skip_serializing_if = "Option::is_none")]
    pub objects: Option<Vec<ObjectVersion>>,
    #[serde(rename = "metadata", skip_serializing_if = "Option::is_none")]
    pub metadata: Option<HashMap<String, String>>,
}

impl Args {
    /// Create a new Args object
    pub fn new() -> Self {
        Args {
            bucket: None,
            object: None,
            version_id: None,
            objects: None,
            metadata: None,
        }
    }

    /// Set the bucket
    pub fn set_bucket(mut self, bucket: Option<String>) -> Self {
        self.bucket = bucket;
        self
    }

    /// Set the object
    pub fn set_object(mut self, object: Option<String>) -> Self {
        self.object = object;
        self
    }

    /// Set the version ID
    pub fn set_version_id(mut self, version_id: Option<String>) -> Self {
        self.version_id = version_id;
        self
    }

    /// Set the objects
    pub fn set_objects(mut self, objects: Option<Vec<ObjectVersion>>) -> Self {
        self.objects = objects;
        self
    }

    /// Set the metadata
    pub fn set_metadata(mut self, metadata: Option<HashMap<String, String>>) -> Self {
        self.metadata = metadata;
        self
    }
}
