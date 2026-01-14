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

//! CORS (Cross-Origin Resource Sharing) header name constants.
//!
//! This module provides centralized constants for CORS-related HTTP header names.
//! The http crate doesn't provide pre-defined constants for CORS headers,
//! so we define them here for type safety and maintainability.

/// CORS response header names
pub mod response {
    pub const ACCESS_CONTROL_ALLOW_ORIGIN: &str = "access-control-allow-origin";
    pub const ACCESS_CONTROL_ALLOW_METHODS: &str = "access-control-allow-methods";
    pub const ACCESS_CONTROL_ALLOW_HEADERS: &str = "access-control-allow-headers";
    pub const ACCESS_CONTROL_EXPOSE_HEADERS: &str = "access-control-expose-headers";
    pub const ACCESS_CONTROL_ALLOW_CREDENTIALS: &str = "access-control-allow-credentials";
    pub const ACCESS_CONTROL_MAX_AGE: &str = "access-control-max-age";
}

/// CORS request header names
pub mod request {
    pub const ACCESS_CONTROL_REQUEST_METHOD: &str = "access-control-request-method";
    pub const ACCESS_CONTROL_REQUEST_HEADERS: &str = "access-control-request-headers";
}

/// Standard HTTP header names used in CORS processing
pub mod standard {
    pub use http::header::{ORIGIN, VARY};
}
