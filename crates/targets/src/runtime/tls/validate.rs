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
/// *and that the key actually corresponds to the certificate's public key*.
/// Returns `Ok(())` if both paths are empty (no mTLS configured).
pub fn validate_cert_key_pairing(cert_path: &str, key_path: &str) -> Result<(), TargetError> {
    if cert_path.is_empty() && key_path.is_empty() {
        return Ok(());
    }

    if cert_path.is_empty() || key_path.is_empty() {
        return Err(TargetError::Configuration(
            "Client certificate and key must both be specified or both be empty".to_string(),
        ));
    }

    let certs = load_certs(cert_path)
        .map_err(|e| TargetError::Configuration(format!("Invalid client certificate '{cert_path}': {e}")))?;
    let key =
        load_private_key(key_path).map_err(|e| TargetError::Configuration(format!("Invalid client key '{key_path}': {e}")))?;

    // Parsing both files is not enough: an operator can supply a cert and a key
    // that belong to different key pairs. Verify the private key's public key
    // matches the certificate's SubjectPublicKeyInfo before accepting them.
    let signing_key = rustls::crypto::aws_lc_rs::sign::any_supported_type(&key)
        .map_err(|e| TargetError::Configuration(format!("Unsupported client private key '{key_path}': {e:?}")))?;
    let certified = rustls::sign::CertifiedKey::new(certs, signing_key);
    match certified.keys_match() {
        Ok(()) => Ok(()),
        // `Unknown` means the signing key cannot expose its public key for
        // comparison (exotic key type). Both files parsed, so we do not reject a
        // possibly-valid pair on an inconclusive comparison; only a definite
        // mismatch is an error.
        Err(rustls::Error::InconsistentKeys(rustls::InconsistentKeys::Unknown)) => Ok(()),
        Err(e) => Err(TargetError::Configuration(format!(
            "Client certificate '{cert_path}' and key '{key_path}' do not match: {e}"
        ))),
    }
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

#[cfg(test)]
mod tests {
    use super::validate_cert_key_pairing;
    use std::path::Path;
    use tempfile::TempDir;

    struct Pair {
        cert_pem: String,
        key_pem: String,
    }

    fn generate_pair() -> Pair {
        let rcgen::CertifiedKey { cert, signing_key } =
            rcgen::generate_simple_self_signed(vec!["example.com".to_string()]).expect("cert should generate");
        Pair {
            cert_pem: cert.pem(),
            key_pem: signing_key.serialize_pem(),
        }
    }

    fn write(dir: &Path, name: &str, contents: &str) -> String {
        let path = dir.join(name);
        std::fs::write(&path, contents).expect("write pem");
        path.to_string_lossy().into_owned()
    }

    #[test]
    fn empty_paths_are_ok() {
        assert!(validate_cert_key_pairing("", "").is_ok());
    }

    #[test]
    fn one_empty_path_is_rejected() {
        assert!(validate_cert_key_pairing("/some/cert.pem", "").is_err());
        assert!(validate_cert_key_pairing("", "/some/key.pem").is_err());
    }

    #[test]
    fn matching_cert_and_key_pass() {
        let dir = TempDir::new().expect("tempdir");
        let pair = generate_pair();
        let cert_path = write(dir.path(), "cert.pem", &pair.cert_pem);
        let key_path = write(dir.path(), "key.pem", &pair.key_pem);

        validate_cert_key_pairing(&cert_path, &key_path).expect("matching cert/key must validate");
    }

    #[test]
    fn mismatched_cert_and_key_are_rejected() {
        let dir = TempDir::new().expect("tempdir");
        let a = generate_pair();
        let b = generate_pair();
        // Cert from pair A, key from pair B — a genuine mismatch.
        let cert_path = write(dir.path(), "cert.pem", &a.cert_pem);
        let key_path = write(dir.path(), "key.pem", &b.key_pem);

        let err = validate_cert_key_pairing(&cert_path, &key_path).expect_err("mismatched cert/key must be rejected");
        assert!(err.to_string().contains("do not match"), "unexpected error: {err}");
    }
}
