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

//! TLS material validation helpers used by the reload coordinator before
//! attempting to build new client/pool objects.

use crate::error::TargetError;
use rustfs_tls_runtime::{load_certs, load_private_key};

/// Validates that a client certificate and private key file can be loaded
/// and paired together. Returns `Ok(())` if both files parse successfully,
/// or `Ok(())` if both paths are empty (no mTLS configured).
pub fn validate_cert_key_pairing(cert_path: &str, key_path: &str) -> Result<(), TargetError> {
    if cert_path.is_empty() && key_path.is_empty() {
        return Ok(());
    }

    if cert_path.is_empty() || key_path.is_empty() {
        return Err(TargetError::Configuration(
            "Client certificate and key must both be specified or both be empty".to_string(),
        ));
    }

    load_certs(cert_path).map_err(|e| TargetError::Configuration(format!("Invalid client certificate '{cert_path}': {e}")))?;

    load_private_key(key_path).map_err(|e| TargetError::Configuration(format!("Invalid client key '{key_path}': {e}")))?;

    Ok(())
}

/// Validates that a CA certificate file can be loaded. Returns `Ok(())`
/// if the path is empty (no custom CA) or if the file parses successfully.
pub fn validate_ca_file(ca_path: &str) -> Result<(), TargetError> {
    if ca_path.is_empty() {
        return Ok(());
    }

    load_certs(ca_path).map_err(|e| TargetError::Configuration(format!("Invalid CA certificate '{ca_path}': {e}")))?;

    Ok(())
}

/// Validates all three TLS material files in one call.
pub fn validate_tls_material(ca_path: &str, cert_path: &str, key_path: &str) -> Result<(), TargetError> {
    validate_ca_file(ca_path)?;
    validate_cert_key_pairing(cert_path, key_path)
}
