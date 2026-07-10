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

//! Stub implementation used when the `dial9` feature is off.
//!
//! Mirrors the surface of [`super::enabled`] so that callers never need a
//! `#[cfg]`. Building a traced runtime always fails here, which drives callers
//! down their standard-runtime fallback path.

use crate::TelemetryError;

/// Placeholder for `dial9_tokio_telemetry::telemetry::TelemetryGuard`.
///
/// Never constructed: [`build_traced_runtime`] always fails without the
/// `dial9` feature.
#[derive(Debug)]
pub struct TelemetryGuard(());

/// Placeholder session guard. Never constructed without the `dial9` feature.
#[derive(Debug)]
pub struct Dial9SessionGuard(());

impl Dial9SessionGuard {
    /// Always false: telemetry support is not compiled in.
    pub fn is_active(&self) -> bool {
        false
    }
}

/// Mirrors the `Drop` the enabled guard uses to flush and seal the trace
/// segment, so a caller can drop the guard before `process::exit` under either
/// feature without `clippy::drop_non_drop` firing on this variant.
impl Drop for Dial9SessionGuard {
    fn drop(&mut self) {}
}

/// Always fails: telemetry support is not compiled in.
///
/// # Errors
///
/// Always returns [`TelemetryError::Io`] describing the missing feature.
pub fn build_traced_runtime(
    _builder: tokio::runtime::Builder,
) -> Result<(tokio::runtime::Runtime, Dial9SessionGuard), TelemetryError> {
    Err(TelemetryError::Io(
        "dial9 telemetry support is not compiled in; rebuild with --features dial9".to_string(),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn build_traced_runtime_always_fails_without_the_feature() {
        let result = build_traced_runtime(tokio::runtime::Builder::new_current_thread());
        assert!(result.is_err());
    }
}
