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

//! TLS fingerprint types for per-target certificate hot-reload detection.

use crate::error::TargetError;

/// SHA256 digest per TLS file component used to detect certificate changes.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TargetTlsFingerprint {
    pub ca_sha256: Option<[u8; 32]>,
    pub client_cert_sha256: Option<[u8; 32]>,
    pub client_key_sha256: Option<[u8; 32]>,
}

/// Monotonically increasing generation counter bumped on each successful reload.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct TargetTlsGeneration(pub u64);

/// Combined TLS state held per-target for tracking reload progress.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TargetTlsState {
    pub generation: TargetTlsGeneration,
    pub fingerprint: Option<TargetTlsFingerprint>,
}

impl TargetTlsState {
    /// Compares `next_fingerprint` with the current one. If different, bumps
    /// generation and stores the new fingerprint. Returns `true` when changed.
    pub fn refresh(&mut self, next_fingerprint: TargetTlsFingerprint) -> bool {
        if self.fingerprint.as_ref() == Some(&next_fingerprint) {
            return false;
        }

        self.generation = TargetTlsGeneration(self.generation.0.saturating_add(1));
        self.fingerprint = Some(next_fingerprint);
        true
    }

    /// Resets state to default (generation 0, no fingerprint).
    pub fn reset(&mut self) {
        *self = Self::default();
    }
}

/// Reads the three TLS material files from disk and returns a fingerprint
/// computed from their SHA256 digests. Empty paths produce `None` digests.
pub async fn build_target_tls_fingerprint(
    ca_path: &str,
    client_cert_path: &str,
    client_key_path: &str,
) -> Result<TargetTlsFingerprint, TargetError> {
    async fn load_optional_digest(path: &str) -> Result<Option<[u8; 32]>, TargetError> {
        if path.is_empty() {
            return Ok(None);
        }

        let bytes = tokio::fs::read(path)
            .await
            .map_err(|e| TargetError::Configuration(format!("Failed to read TLS material '{path}': {e}")))?;
        let digest = rustfs_tls_runtime::TlsFingerprint::from_optional_bytes(Some(&bytes), None, None, None, None).server_sha256;
        Ok(digest)
    }

    Ok(TargetTlsFingerprint {
        ca_sha256: load_optional_digest(ca_path).await?,
        client_cert_sha256: load_optional_digest(client_cert_path).await?,
        client_key_sha256: load_optional_digest(client_key_path).await?,
    })
}

/// Convenience helper: refreshes `state` and calls `invalidate_cached_connection`
/// only when the fingerprint actually changed.
pub fn refresh_tls_fingerprint_state(
    state: &mut TargetTlsState,
    next_fingerprint: TargetTlsFingerprint,
    invalidate_cached_connection: impl FnOnce(),
) {
    if state.refresh(next_fingerprint) {
        invalidate_cached_connection();
    }
}

#[cfg(test)]
mod tests {
    use super::{TargetTlsFingerprint, TargetTlsGeneration, TargetTlsState};

    #[test]
    fn refresh_increments_generation_only_when_fingerprint_changes() {
        let mut state = TargetTlsState::default();
        let first = TargetTlsFingerprint {
            ca_sha256: Some([1; 32]),
            client_cert_sha256: None,
            client_key_sha256: None,
        };
        let second = TargetTlsFingerprint {
            ca_sha256: Some([2; 32]),
            client_cert_sha256: None,
            client_key_sha256: None,
        };

        assert!(state.refresh(first.clone()));
        assert_eq!(state.generation, TargetTlsGeneration(1));
        assert!(!state.refresh(first));
        assert_eq!(state.generation, TargetTlsGeneration(1));
        assert!(state.refresh(second));
        assert_eq!(state.generation, TargetTlsGeneration(2));
    }

    #[test]
    fn reset_clears_generation_and_fingerprint() {
        let mut state = TargetTlsState {
            generation: TargetTlsGeneration(5),
            fingerprint: Some(TargetTlsFingerprint {
                ca_sha256: Some([9; 32]),
                client_cert_sha256: None,
                client_key_sha256: None,
            }),
        };

        state.reset();
        assert_eq!(state, TargetTlsState::default());
    }

    #[test]
    fn fingerprint_eq_when_all_fields_match() {
        let a = TargetTlsFingerprint {
            ca_sha256: Some([42; 32]),
            client_cert_sha256: Some([1; 32]),
            client_key_sha256: None,
        };
        let b = TargetTlsFingerprint {
            ca_sha256: Some([42; 32]),
            client_cert_sha256: Some([1; 32]),
            client_key_sha256: None,
        };
        assert_eq!(a, b);
    }

    #[test]
    fn fingerprint_ne_when_ca_differs() {
        let a = TargetTlsFingerprint {
            ca_sha256: Some([1; 32]),
            client_cert_sha256: None,
            client_key_sha256: None,
        };
        let b = TargetTlsFingerprint {
            ca_sha256: Some([2; 32]),
            client_cert_sha256: None,
            client_key_sha256: None,
        };
        assert_ne!(a, b);
    }
}
