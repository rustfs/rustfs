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

use rustls::crypto::CryptoProvider;

/// The outcome of ensuring the process-wide rustls provider.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CryptoProviderStatus {
    Installed,
    AlreadyInstalled,
}

/// An incompatible process-wide rustls provider prevents FIPS startup.
#[derive(Debug, thiserror::Error)]
#[error(
    "FIPS TLS requested, but the process-wide rustls crypto provider is not FIPS compatible; configure the host to install a FIPS provider before starting RustFS"
)]
pub struct CryptoProviderError {
    _private: (),
}

/// Construct the provider selected by this crate's feature configuration.
pub fn default_crypto_provider() -> CryptoProvider {
    #[cfg(feature = "fips")]
    {
        rustls::crypto::default_fips_provider()
    }
    #[cfg(not(feature = "fips"))]
    {
        rustls::crypto::aws_lc_rs::default_provider()
    }
}

/// Install or reuse the process-wide rustls provider.
///
/// # Errors
/// Returns an error in a FIPS build if the provider actually installed in the
/// process is absent or does not satisfy rustls's FIPS checks.
pub fn ensure_crypto_provider() -> Result<CryptoProviderStatus, CryptoProviderError> {
    // Rustls's process-wide provider is write-once; check again after a race.
    let status = if CryptoProvider::get_default().is_some() || default_crypto_provider().install_default().is_err() {
        CryptoProviderStatus::AlreadyInstalled
    } else {
        CryptoProviderStatus::Installed
    };
    #[cfg(feature = "fips")]
    if !CryptoProvider::get_default().is_some_and(|provider| provider.fips()) {
        return Err(CryptoProviderError { _private: () });
    }
    Ok(status)
}

#[cfg(test)]
mod tests {
    use super::{CryptoProviderStatus, default_crypto_provider, ensure_crypto_provider};

    #[test]
    fn provider_policy_isolated() {
        const SCENARIO: &str = "RUSTFS_TLS_TEST_PROVIDER_SCENARIO";
        if let Ok(scenario) = std::env::var(SCENARIO) {
            match scenario.as_str() {
                "fresh" => {
                    assert_eq!(ensure_crypto_provider().expect("initial install"), CryptoProviderStatus::Installed);
                    assert_eq!(ensure_crypto_provider().expect("provider reuse"), CryptoProviderStatus::AlreadyInstalled);
                }
                "incompatible" => {
                    let mut provider = default_crypto_provider();
                    provider.kx_groups = vec![rustls::crypto::aws_lc_rs::kx_group::X25519];
                    provider.install_default().expect("install incompatible provider");
                    let result = ensure_crypto_provider();
                    if cfg!(feature = "fips") {
                        assert!(result.is_err(), "FIPS mode must reject the installed provider");
                    } else {
                        assert_eq!(result.expect("reuse existing provider"), CryptoProviderStatus::AlreadyInstalled);
                    }
                }
                _ => panic!("unknown provider scenario"),
            }
            return;
        }

        for scenario in ["fresh", "incompatible"] {
            let output = std::process::Command::new(std::env::current_exe().expect("test executable"))
                .arg("provider_policy_isolated")
                .arg("--nocapture")
                .env(SCENARIO, scenario)
                .output()
                .expect("start isolated provider test");
            assert!(
                output.status.success(),
                "{scenario}: stdout={} stderr={}",
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr)
            );
        }
    }
}
