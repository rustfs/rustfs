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

//! Cloud provider metadata fetching
//!
//! This module contains implementations for fetching metadata
//! from various cloud providers.

mod aws;
mod azure;
mod gcp;

pub use aws::*;
pub use azure::*;
pub use gcp::*;

/// Build the metadata HTTP client without panicking when the host has no
/// system CA bundle (issue #6734).
///
/// `reqwest::Client::new()` panics when the TLS backend cannot load any
/// system trust root, which is exactly the state of a minimal container
/// image. Cloud metadata endpoints are plain HTTP link-local addresses, so a
/// client with an explicit empty trust store is fully functional here; TLS
/// requests through it fail closed at the handshake.
pub(crate) fn metadata_http_client(timeout: std::time::Duration) -> reqwest::Client {
    reqwest::Client::builder().timeout(timeout).build().unwrap_or_else(|error| {
        tracing::warn!(
            "cloud metadata HTTP client could not load system TLS roots ({error}); continuing with an empty trust store"
        );
        reqwest::Client::builder()
            .timeout(timeout)
            .tls_certs_only(std::iter::empty::<reqwest::Certificate>())
            .build()
            .expect("HTTP client construction must succeed with an explicit empty trust store")
    })
}

#[cfg(test)]
mod tests {
    // The startup panic fix for hosts without a CA bundle (issue #6734) rests
    // on the constructor never panicking and its degraded fallback — an
    // explicit empty trust store — always building.
    #[test]
    fn metadata_http_client_construction_never_panics() {
        let _ = super::metadata_http_client(std::time::Duration::from_secs(1));
        reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(1))
            .tls_certs_only(std::iter::empty::<reqwest::Certificate>())
            .build()
            .expect("empty-trust-store client build must succeed without touching system roots");
    }
}
