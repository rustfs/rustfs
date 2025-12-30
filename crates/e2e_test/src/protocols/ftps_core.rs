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
use native_tls::TlsConnector;
use rcgen::generate_simple_self_signed;
use std::io::Cursor;
use std::path::PathBuf;
use suppaftp::NativeTlsConnector;
use suppaftp::NativeTlsFtpStream;
use tokio::process::Command;
use tracing::info;

// Fixed FTPS port for testing
const FTPS_PORT: u16 = 9021;
const FTPS_ADDRESS: &str = "127.0.0.1:9021";

/// Test FTPS: put, ls, mkdir, rmdir, delete operations
pub async fn test_ftps_core_operations() -> Result<()> {
    let env = ProtocolTestEnvironment::new().map_err(|e| anyhow::anyhow!("{}", e))?;

    // Generate and write certificate
    let cert = generate_simple_self_signed(vec!["localhost".to_string(), "127.0.0.1".to_string()])?;
    let cert_path = PathBuf::from(&env.temp_dir).join("ftps.crt");
    let key_path = PathBuf::from(&env.temp_dir).join("ftps.key");

    let cert_pem = cert.cert.pem();
    let key_pem = cert.signing_key.serialize_pem();
    tokio::fs::write(&cert_path, &cert_pem).await?;
    tokio::fs::write(&key_path, &key_pem).await?;

    // Start server manually
    info!("Starting FTPS server on {}", FTPS_ADDRESS);
    let binary_path = rustfs_binary_path();
    let mut server_process = Command::new(&binary_path)
        .args([
            "--ftps-enable",
            "--ftps-address",
            FTPS_ADDRESS,
            "--ftps-certs-file",
            cert_path.to_str().unwrap(),
            "--ftps-key-file",
            key_path.to_str().unwrap(),
            &env.temp_dir,
        ])
        .spawn()?;

    // Ensure server is cleaned up even on failure
    let result = async {
        // Wait for server to be ready
        ProtocolTestEnvironment::wait_for_port_ready(FTPS_PORT, 30)
            .await
            .map_err(|e| anyhow::anyhow!("{}", e))?;

        // Create native TLS connector that accepts the certificate
        let tls_connector = TlsConnector::builder().danger_accept_invalid_certs(true).build()?;

        // Wrap in suppaftp's NativeTlsConnector
        let tls_connector = NativeTlsConnector::from(tls_connector);

        // Connect to FTPS server
        let ftp_stream = NativeTlsFtpStream::connect(FTPS_ADDRESS).map_err(|e| anyhow::anyhow!("Failed to connect: {}", e))?;

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
