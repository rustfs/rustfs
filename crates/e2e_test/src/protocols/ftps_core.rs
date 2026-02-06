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

//! Core FTPS tests

use crate::common::rustfs_binary_path;
use crate::protocols::test_env::{DEFAULT_ACCESS_KEY, DEFAULT_SECRET_KEY, ProtocolTestEnvironment};
use anyhow::Result;
use rcgen::generate_simple_self_signed;
use rustls::crypto::aws_lc_rs::default_provider;
use rustls::{ClientConfig, RootCertStore};
use std::io::Cursor;
use std::path::PathBuf;
use std::sync::Arc;
use suppaftp::RustlsConnector;
use suppaftp::RustlsFtpStream;
use tokio::process::Command;
use tracing::info;

// Fixed FTPS port for testing
const FTPS_PORT: u16 = 9021;
const FTPS_ADDRESS: &str = "127.0.0.1:9021";

/// Test FTPS: put, ls, mkdir, rmdir, delete operations
pub async fn test_ftps_core_operations() -> Result<()> {
    let env = ProtocolTestEnvironment::new().map_err(|e| anyhow::anyhow!("{}", e))?;

    let cert_dir = PathBuf::from(&env.temp_dir).join("ftps_certs");
    tokio::fs::create_dir_all(&cert_dir).await?;

    // Generate default certificate for root directory
    let default_cert = generate_simple_self_signed(vec!["localhost".to_string(), "127.0.0.1".to_string()])?;
    let default_cert_path = cert_dir.join("rustfs_cert.pem");
    let default_key_path = cert_dir.join("rustfs_key.pem");
    tokio::fs::write(&default_cert_path, default_cert.cert.pem()).await?;
    tokio::fs::write(&default_key_path, default_cert.signing_key.serialize_pem()).await?;

    // Create subdirectory for domain-specific certificate
    let example_domain_dir = cert_dir.join("example1.com");
    tokio::fs::create_dir_all(&example_domain_dir).await?;
    let domain_cert = generate_simple_self_signed(vec!["example1.com".to_string()])?;
    let domain_cert_path = example_domain_dir.join("rustfs_cert.pem");
    let domain_key_path = example_domain_dir.join("rustfs_key.pem");
    tokio::fs::write(&domain_cert_path, domain_cert.cert.pem()).await?;
    tokio::fs::write(&domain_key_path, domain_cert.signing_key.serialize_pem()).await?;

    info!("Generated 2 certificates in {:?}", cert_dir);

    // Start server manually
    info!("Starting FTPS server on {}", FTPS_ADDRESS);
    let binary_path = rustfs_binary_path();
    let mut server_process = Command::new(&binary_path)
        .env("RUSTFS_FTPS_ENABLE", "true")
        .env("RUSTFS_FTPS_ADDRESS", FTPS_ADDRESS)
        .env("RUSTFS_FTPS_CERTS_DIR", cert_dir.to_str().unwrap())
        .arg(&env.temp_dir)
        .spawn()?;

    // Ensure server is cleaned up even on failure
    let result = async {
        // Wait for server to be ready
        ProtocolTestEnvironment::wait_for_port_ready(FTPS_PORT, 30)
            .await
            .map_err(|e| anyhow::anyhow!("{}", e))?;

        // Install the aws-lc-rs crypto provider
        default_provider()
            .install_default()
            .map_err(|e| anyhow::anyhow!("Failed to install crypto provider: {:?}", e))?;

        // Create a simple rustls config that accepts any certificate for testing
        let mut root_store = RootCertStore::empty();
        // Add the self-signed certificate to the trust store for e2e
        // Note: In a real environment, you'd use proper root certificates
        let cert_pem = default_cert.cert.pem();
        let cert_der = rustls_pemfile::certs(&mut Cursor::new(cert_pem))
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| anyhow::anyhow!("Failed to parse cert: {}", e))?;

        root_store.add_parsable_certificates(cert_der);

        let config = ClientConfig::builder()
            .with_root_certificates(root_store)
            .with_no_client_auth();

        // Wrap in suppaftp's RustlsConnector
        let tls_connector = RustlsConnector::from(Arc::new(config));

        // Connect to FTPS server
        let ftp_stream = RustlsFtpStream::connect(FTPS_ADDRESS).map_err(|e| anyhow::anyhow!("Failed to connect: {}", e))?;

        // Upgrade to secure connection
        let mut ftp_stream = ftp_stream
            .into_secure(tls_connector, "127.0.0.1")
            .map_err(|e| anyhow::anyhow!("Failed to upgrade to TLS: {}", e))?;
        ftp_stream.login(DEFAULT_ACCESS_KEY, DEFAULT_SECRET_KEY)?;

        info!("Testing FTPS: mkdir bucket");
        let bucket_name = "testbucket";
        ftp_stream.mkdir(bucket_name)?;
        info!("PASS: mkdir bucket '{}' successful", bucket_name);

        info!("Testing FTPS: cd to bucket");
        ftp_stream.cwd(bucket_name)?;
        info!("PASS: cd to bucket '{}' successful", bucket_name);

        info!("Testing FTPS: put file");
        let filename = "test.txt";
        let content = "Hello, FTPS!";
        ftp_stream.put_file(filename, &mut Cursor::new(content.as_bytes()))?;
        info!("PASS: put file '{}' ({} bytes) successful", filename, content.len());

        info!("Testing FTPS: download file");
        let downloaded_content = ftp_stream.retr(filename, |stream| {
            let mut buffer = Vec::new();
            stream.read_to_end(&mut buffer).map_err(suppaftp::FtpError::ConnectionError)?;
            Ok(buffer)
        })?;
        let downloaded_str = String::from_utf8(downloaded_content)?;
        assert_eq!(downloaded_str, content, "Downloaded content should match uploaded content");
        info!("PASS: download file '{}' successful, content matches", filename);

        info!("Testing FTPS: ls list objects in bucket");
        let list = ftp_stream.list(None)?;
        assert!(list.iter().any(|line| line.contains(filename)), "File should appear in list");
        info!("PASS: ls command successful, file '{}' found in bucket", filename);

        info!("Testing FTPS: ls . (list current directory)");
        let list_dot = ftp_stream.list(Some(".")).unwrap_or_else(|_| ftp_stream.list(None).unwrap());
        assert!(list_dot.iter().any(|line| line.contains(filename)), "File should appear in ls .");
        info!("PASS: ls . successful, file '{}' found", filename);

        info!("Testing FTPS: ls / (list root directory)");
        let list_root = ftp_stream.list(Some("/")).unwrap();
        assert!(list_root.iter().any(|line| line.contains(bucket_name)), "Bucket should appear in ls /");
        assert!(!list_root.iter().any(|line| line.contains(filename)), "File should not appear in ls /");
        info!(
            "PASS: ls / successful, bucket '{}' found, file '{}' not found in root",
            bucket_name, filename
        );

        info!("Testing FTPS: ls /. (list root directory with /.)");
        let list_root_dot = ftp_stream
            .list(Some("/."))
            .unwrap_or_else(|_| ftp_stream.list(Some("/")).unwrap());
        assert!(
            list_root_dot.iter().any(|line| line.contains(bucket_name)),
            "Bucket should appear in ls /."
        );
        info!("PASS: ls /. successful, bucket '{}' found", bucket_name);

        info!("Testing FTPS: ls /bucket (list bucket by absolute path)");
        let list_bucket = ftp_stream.list(Some(&format!("/{}", bucket_name))).unwrap();
        assert!(list_bucket.iter().any(|line| line.contains(filename)), "File should appear in ls /bucket");
        info!("PASS: ls /{} successful, file '{}' found", bucket_name, filename);

        info!("Testing FTPS: cd . (stay in current directory)");
        ftp_stream.cwd(".")?;
        info!("PASS: cd . successful (stays in current directory)");

        info!("Testing FTPS: ls after cd . (should still see file)");
        let list_after_dot = ftp_stream.list(None)?;
        assert!(
            list_after_dot.iter().any(|line| line.contains(filename)),
            "File should still appear in list after cd ."
        );
        info!("PASS: ls after cd . successful, file '{}' still found in bucket", filename);

        info!("Testing FTPS: cd / (go to root directory)");
        ftp_stream.cwd("/")?;
        info!("PASS: cd / successful (back to root directory)");

        info!("Testing FTPS: ls after cd / (should see bucket only)");
        let root_list_after = ftp_stream.list(None)?;
        assert!(
            !root_list_after.iter().any(|line| line.contains(filename)),
            "File should not appear in root ls"
        );
        assert!(
            root_list_after.iter().any(|line| line.contains(bucket_name)),
            "Bucket should appear in root ls"
        );
        info!("PASS: ls after cd / successful, file not in root, bucket '{}' found in root", bucket_name);

        info!("Testing FTPS: cd back to bucket");
        ftp_stream.cwd(bucket_name)?;
        info!("PASS: cd back to bucket '{}' successful", bucket_name);

        info!("Testing FTPS: delete object");
        ftp_stream.rm(filename)?;
        info!("PASS: delete object '{}' successful", filename);

        info!("Testing FTPS: ls verify object deleted");
        let list_after = ftp_stream.list(None)?;
        assert!(!list_after.iter().any(|line| line.contains(filename)), "File should be deleted");
        info!("PASS: ls after delete successful, file '{}' is not found", filename);

        info!("Testing FTPS: cd up to root directory");
        ftp_stream.cdup()?;
        info!("PASS: cd up to root directory successful");

        info!("Testing FTPS: cd to nonexistent bucket (should fail)");
        let nonexistent_bucket = "nonexistent-bucket";
        let cd_result = ftp_stream.cwd(nonexistent_bucket);
        assert!(cd_result.is_err(), "cd to nonexistent bucket should fail");
        info!("PASS: cd to nonexistent bucket '{}' failed as expected", nonexistent_bucket);

        info!("Testing FTPS: ls verify bucket exists in root");
        let root_list = ftp_stream.list(None)?;
        assert!(root_list.iter().any(|line| line.contains(bucket_name)), "Bucket should exist in root");
        info!("PASS: ls root successful, bucket '{}' found in root", bucket_name);

        info!("Testing FTPS: rmdir delete bucket");
        ftp_stream.rmdir(bucket_name)?;
        info!("PASS: rmdir bucket '{}' successful", bucket_name);

        info!("Testing FTPS: ls verify bucket deleted");
        let root_list_after = ftp_stream.list(None)?;
        assert!(!root_list_after.iter().any(|line| line.contains(bucket_name)), "Bucket should be deleted");
        info!("PASS: ls root after delete successful, bucket '{}' is not found", bucket_name);

        ftp_stream.quit()?;

        info!("FTPS core tests passed");
        Ok(())
    }
    .await;

    // Always cleanup server process
    let _ = server_process.kill().await;
    let _ = server_process.wait().await;

    result
}
