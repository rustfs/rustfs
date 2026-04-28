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

use anyhow::{Context, Result, bail};
use clap::Parser;
use rcgen::{
    BasicConstraints, CertificateParams, CertifiedIssuer, DnType, ExtendedKeyUsagePurpose, IsCa, KeyPair, KeyUsagePurpose,
    SanType,
};
use std::fs;
use std::net::IpAddr;
use std::path::{Path, PathBuf};
use time::{Duration, OffsetDateTime};

pub const DEFAULT_OUT_DIR: &str = "target/tls";
pub const OUTPUT_FILES: [&str; 7] = [
    "rustfs_cert.pem",
    "rustfs_key.pem",
    "ca.crt",
    "public.crt",
    "client_ca.crt",
    "client_cert.pem",
    "client_key.pem",
];

#[derive(Debug, Parser)]
#[command(name = "tls_gen", about = "Generate a full RustFS TLS bundle for local TLS and mTLS tests.")]
pub struct Args {
    #[arg(long, default_value = DEFAULT_OUT_DIR)]
    pub out_dir: PathBuf,
    #[arg(long, default_value_t = 365)]
    pub days: i64,
    #[arg(long)]
    pub force: bool,
}

pub fn run(args: Args) -> Result<PathBuf> {
    if args.days <= 0 {
        bail!("--days must be a positive integer");
    }

    write_bundle(&args.out_dir, args.force, args.days)?;
    Ok(args.out_dir)
}

pub fn ensure_writable(out_dir: &Path, force: bool) -> Result<()> {
    if force {
        return Ok(());
    }

    let existing: Vec<_> = OUTPUT_FILES
        .iter()
        .map(|name| out_dir.join(name))
        .filter(|path| path.exists())
        .collect();

    if existing.is_empty() {
        return Ok(());
    }

    let existing_list = existing
        .iter()
        .map(|path| path.file_name().and_then(|name| name.to_str()).unwrap_or("<unknown>"))
        .collect::<Vec<_>>()
        .join(", ");

    bail!(
        "Refusing to overwrite existing files in {}: {}. Re-run with --force to replace them.",
        out_dir.display(),
        existing_list
    )
}

fn write_bundle(out_dir: &Path, force: bool, days: i64) -> Result<()> {
    fs::create_dir_all(out_dir).with_context(|| format!("failed to create output directory {}", out_dir.display()))?;
    ensure_writable(out_dir, force)?;

    let ca_key = generate_private_key()?;
    let ca = build_ca_certificate(ca_key, days)?;

    let server_key = generate_private_key()?;
    let server_cert = build_leaf_certificate(
        &server_key,
        "localhost",
        &[SanType::DnsName("localhost".try_into()?)],
        &[
            SanType::IpAddress(IpAddr::V4("127.0.0.1".parse()?)),
            SanType::IpAddress(IpAddr::V6("::1".parse()?)),
        ],
        ExtendedKeyUsagePurpose::ServerAuth,
        &ca,
        days,
    )?;

    let client_key = generate_private_key()?;
    let client_cert = build_leaf_certificate(
        &client_key,
        "rustfs-test-client",
        &[SanType::DnsName("rustfs-test-client".try_into()?)],
        &[],
        ExtendedKeyUsagePurpose::ClientAuth,
        &ca,
        days,
    )?;

    let ca_pem = ca.pem();
    let bundle = [
        ("rustfs_cert.pem", server_cert.pem()),
        ("rustfs_key.pem", server_key.serialize_pem()),
        ("ca.crt", ca_pem.clone()),
        ("public.crt", ca_pem.clone()),
        ("client_ca.crt", ca_pem),
        ("client_cert.pem", client_cert.pem()),
        ("client_key.pem", client_key.serialize_pem()),
    ];

    for (name, content) in bundle {
        fs::write(out_dir.join(name), content).with_context(|| format!("failed to write {}", out_dir.join(name).display()))?;
    }

    Ok(())
}

fn build_ca_certificate(signing_key: KeyPair, days: i64) -> Result<CertifiedIssuer<'static, KeyPair>> {
    let mut params = base_params("RustFS Test CA", days)?;
    params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    params.key_usages = vec![KeyUsagePurpose::KeyCertSign, KeyUsagePurpose::CrlSign];

    CertifiedIssuer::self_signed(params, signing_key).context("failed to create CA certificate")
}

fn build_leaf_certificate(
    signing_key: &KeyPair,
    common_name: &str,
    dns_names: &[SanType],
    ip_addresses: &[SanType],
    usage: ExtendedKeyUsagePurpose,
    issuer: &CertifiedIssuer<'_, KeyPair>,
    days: i64,
) -> Result<rcgen::Certificate> {
    let mut params = base_params(common_name, days)?;
    params.is_ca = IsCa::ExplicitNoCa;
    params.key_usages = vec![KeyUsagePurpose::DigitalSignature, KeyUsagePurpose::KeyEncipherment];
    params.extended_key_usages = vec![usage];
    params.use_authority_key_identifier_extension = true;
    params.subject_alt_names.extend_from_slice(dns_names);
    params.subject_alt_names.extend_from_slice(ip_addresses);

    params
        .signed_by(signing_key, issuer)
        .with_context(|| format!("failed to create leaf certificate for {common_name}"))
}

fn base_params(common_name: &str, days: i64) -> Result<CertificateParams> {
    let mut params = CertificateParams::default();
    let issued_at = OffsetDateTime::now_utc() - Duration::minutes(5);
    params.not_before = issued_at;
    params.not_after = issued_at + Duration::days(days);
    params.distinguished_name.push(DnType::CountryName, "US");
    params.distinguished_name.push(DnType::OrganizationName, "RustFS");
    params.distinguished_name.push(DnType::CommonName, common_name);
    Ok(params)
}

fn generate_private_key() -> Result<KeyPair> {
    KeyPair::generate().context("failed to generate private key")
}

#[cfg(test)]
mod tests {
    use super::{Args, OUTPUT_FILES, ensure_writable, run};
    use std::fs;
    use std::path::{Path, PathBuf};
    use std::time::{SystemTime, UNIX_EPOCH};

    fn unique_temp_dir() -> PathBuf {
        let suffix = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time must be after unix epoch")
            .as_nanos();
        std::env::temp_dir().join(format!("rustfs-tls-gen-{suffix}"))
    }

    struct TempDir {
        path: PathBuf,
    }

    impl TempDir {
        fn new() -> Self {
            let path = unique_temp_dir();
            fs::create_dir_all(&path).expect("temporary directory should be created");
            Self { path }
        }

        fn path(&self) -> &Path {
            &self.path
        }
    }

    impl Drop for TempDir {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.path);
        }
    }

    #[test]
    fn run_writes_full_bundle() {
        let temp_dir = TempDir::new();

        let out_dir = run(Args {
            out_dir: temp_dir.path().join("tls"),
            days: 365,
            force: false,
        })
        .expect("bundle generation should succeed");

        for name in OUTPUT_FILES {
            let content = fs::read(out_dir.join(name)).unwrap_or_else(|error| panic!("{name} should exist: {error}"));
            assert!(!content.is_empty(), "{name} should not be empty");
        }
    }

    #[test]
    fn ensure_writable_rejects_existing_files_without_force() {
        let temp_dir = TempDir::new();
        let existing = temp_dir.path().join(OUTPUT_FILES[0]);
        fs::write(&existing, "existing").expect("existing file should be created");

        let error = ensure_writable(temp_dir.path(), false).expect_err("existing files must be rejected");
        let message = format!("{error:#}");

        assert!(message.contains("Refusing to overwrite existing files"));
        assert!(message.contains(OUTPUT_FILES[0]));
    }

    #[test]
    fn run_rejects_non_positive_days() {
        let temp_dir = TempDir::new();
        let error = run(Args {
            out_dir: temp_dir.path().join("tls"),
            days: 0,
            force: false,
        })
        .expect_err("non-positive days must fail");

        assert_eq!(format!("{error:#}"), "--days must be a positive integer");
    }
}
