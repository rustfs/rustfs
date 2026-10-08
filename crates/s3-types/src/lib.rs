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

//! RustFS-owned S3 contract types: event names, the S3 error carrier, and the
//! plain HTTP body.
//!
//! Responsible for: `EventName`, the error surface (`S3Error`, `S3ErrorCode`,
//! `S3Result`, the `s3_error` macro) that every RustFS crate raises S3 errors with,
//! and `Body`, the type-erased body the outbound client crates send.
//! Not responsible for: I/O, global state, wire rendering, S3 body semantics
//! (trailers, checksums, framing), or any s3s type; the legacy conversions live
//! behind the `compat-s3s` feature in `compat_s3s` only.
//! Upstream: `http`, `bytes`, `http-body`, `http-body-util`. Downstream: every
//! crate that raises or inspects S3 errors, and the crates that build HTTP bodies.

mod body;
#[cfg(any(test, feature = "compat-s3s"))]
mod compat_s3s;
mod error;
mod event_name;

pub use body::Body;
pub use error::{S3Error, S3ErrorCode, S3ErrorParts, S3Result, StdError};
pub use event_name::{EventName, ParseEventNameError, event_schema_version};
