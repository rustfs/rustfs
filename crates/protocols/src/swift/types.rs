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

//! Swift data types

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Swift container metadata
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Container {
    /// Container name
    pub name: String,
    /// Number of objects in container
    pub count: u64,
    /// Total bytes used by objects
    pub bytes: u64,
    /// Last modified timestamp (UNIX epoch)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_modified: Option<String>,
}

/// Swift object metadata
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Object {
    /// Object name (key)
    pub name: String,
    /// MD5 hash (ETag)
    pub hash: String,
    /// Size in bytes
    pub bytes: u64,
    /// Content type
    pub content_type: String,
    /// Last modified timestamp
    pub last_modified: String,
}

/// Swift metadata extracted from headers
#[derive(Debug, Clone, Default)]
pub struct SwiftMetadata {
    /// Custom metadata key-value pairs (from X-Container-Meta-* or X-Object-Meta-*)
    pub metadata: HashMap<String, String>,
    /// Container read ACL (from X-Container-Read)
    pub read_acl: Option<String>,
    /// Container write ACL (from X-Container-Write)
    pub write_acl: Option<String>,
}
