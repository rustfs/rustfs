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

use crate::config::{TlsCommands, TlsInspectOpts, TlsOpts};
use rustfs_config::{RUSTFS_TLS_CERT, RUSTFS_TLS_KEY};
use rustfs_tls_runtime::{CertDirectoryLoadOptions, TlsCertPairInspection, TlsCertPairStatus, TlsSource, inspect_cert_directory};
use std::io::{Error, Result};

pub fn execute_tls(opts: &TlsOpts) -> Result<()> {
    match &opts.command {
        TlsCommands::Inspect(inspect) => execute_tls_inspect(inspect),
    }
}

fn execute_tls_inspect(opts: &TlsInspectOpts) -> Result<()> {
    let tls_source = TlsSource::from_directory(opts.path.trim());
    let tls_dir = tls_source.validate_directory().map_err(Error::other)?;
    let inspection = inspect_cert_directory(CertDirectoryLoadOptions::builder(tls_dir, RUSTFS_TLS_CERT, RUSTFS_TLS_KEY).build())?;

    println!("TLS directory inspection");
    println!("path: {}", inspection.directory.display());
    if let Some(canonical_directory) = &inspection.canonical_directory {
        println!("canonical path: {}", canonical_directory.display());
    }
    println!("root pair: {}", format_pair_summary(&inspection.root_pair));

    let valid_domains = inspection.valid_domain_names();
    let runtime_mode = if !valid_domains.is_empty() {
        "multi-cert/SNI"
    } else if inspection.has_valid_root_pair() {
        "single-cert/default"
    } else {
        "no-usable-server-cert"
    };
    println!("runtime mode: {}", runtime_mode);
    println!("valid domain pairs: {}", valid_domains.len());
    if !valid_domains.is_empty() {
        println!("valid domains: {}", valid_domains.join(", "));
    }

    if !inspection.skipped_directory_names.is_empty() {
        println!("skipped internal directories: {}", inspection.skipped_directory_names.join(", "));
    }

    if inspection.domain_pairs.is_empty() {
        println!("domain pairs: none");
    } else {
        println!("domain pairs:");
        for domain in &inspection.domain_pairs {
            println!("  - {}: {}", domain.domain_name, format_pair_summary(&domain.pair));
        }
    }

    Ok(())
}

fn format_pair_summary(pair: &TlsCertPairInspection) -> String {
    match &pair.status {
        TlsCertPairStatus::MissingBoth => format!(
            "missing cert/key (expected cert={}, key={})",
            pair.cert_path.display(),
            pair.key_path.display()
        ),
        TlsCertPairStatus::MissingCert => format!(
            "missing cert (expected cert={}, key={})",
            pair.cert_path.display(),
            pair.key_path.display()
        ),
        TlsCertPairStatus::MissingKey => format!(
            "missing key (expected cert={}, key={})",
            pair.cert_path.display(),
            pair.key_path.display()
        ),
        TlsCertPairStatus::Valid => {
            format!("valid cert/key pair (cert={}, key={})", pair.cert_path.display(), pair.key_path.display())
        }
        TlsCertPairStatus::Invalid { error } => format!(
            "invalid cert/key pair (cert={}, key={}): {}",
            pair.cert_path.display(),
            pair.key_path.display(),
            error
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::TempDir;

    fn write_test_cert_pair(dir: &std::path::Path) {
        let rcgen::CertifiedKey { cert, signing_key } =
            rcgen::generate_simple_self_signed(vec!["example.com".to_string()]).expect("cert should generate");
        fs::write(dir.join(RUSTFS_TLS_CERT), cert.pem()).expect("cert should write");
        fs::write(dir.join(RUSTFS_TLS_KEY), signing_key.serialize_pem()).expect("key should write");
    }

    #[test]
    fn execute_tls_inspect_accepts_valid_directory() {
        let temp_dir = TempDir::new().expect("tempdir should create");
        write_test_cert_pair(temp_dir.path());

        let opts = TlsInspectOpts {
            path: temp_dir.path().display().to_string(),
        };

        execute_tls_inspect(&opts).expect("tls inspect should succeed");
    }

    #[test]
    fn format_pair_summary_reports_missing_key() {
        let pair = TlsCertPairInspection {
            cert_path: "/tmp/rustfs_cert.pem".into(),
            key_path: "/tmp/rustfs_key.pem".into(),
            status: TlsCertPairStatus::MissingKey,
        };

        let summary = format_pair_summary(&pair);
        assert!(summary.contains("missing key"));
        assert!(summary.contains("/tmp/rustfs_cert.pem"));
        assert!(summary.contains("/tmp/rustfs_key.pem"));
    }
}
