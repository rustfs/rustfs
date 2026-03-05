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
pub mod container;
pub mod errors;
pub mod handler;
pub mod object;
pub mod router;
pub mod types;

pub use errors::{SwiftError, SwiftResult};
pub use router::{SwiftRoute, SwiftRouter};
// Note: Container, Object, and SwiftMetadata types used by Swift implementation
#[allow(unused_imports)]
pub use types::{Container, Object, SwiftMetadata};
