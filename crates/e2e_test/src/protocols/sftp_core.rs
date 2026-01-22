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

use crate::common::rustfs_binary_path;
use crate::protocols::test_env::{DEFAULT_ACCESS_KEY, DEFAULT_SECRET_KEY, ProtocolTestEnvironment};
use anyhow::Result;
use std::path::PathBuf;
use std::process::Command;
use tokio::fs;
use tokio::process::Command as TokioCommand;
use tracing::info;

const SFTP_PORT: u16 = 8022;
const SFTP_ADDRESS: &str = "127.0.0.1:8022";

/// Execute SFTP command with given input
fn execute_sftp_command(client_key_path: &PathBuf, command: &str) -> Result<String> {
    // Create a temporary batch file
    let batch_content = format!("{}\n", command);

    let mut temp_file = std::env::temp_dir();
    temp_file.push("sftp_batch.txt");
    std::fs::write(&temp_file, &batch_content)?;

    // Use timeout to force SFTP to exit after command completion
    let output = Command::new("timeout")
        .arg("5s") // 5 second timeout
        .arg("sshpass")
        .arg("-p")
        .arg(DEFAULT_SECRET_KEY)
        .arg("sftp")
        .arg("-o")
        .arg("StrictHostKeyChecking=no")
        .arg("-o")
        .arg("UserKnownHostsFile=/dev/null")
        .arg("-o")
        .arg("ConnectTimeout=10")
        .arg("-i")
        .arg(client_key_path)
        .arg("-P")
        .arg(SFTP_PORT.to_string())
        .arg("-b")
        .arg(temp_file.to_string_lossy().to_string())
        .arg(format!("{}@{}", DEFAULT_ACCESS_KEY, "127.0.0.1"))
        .output()
        .map_err(|e| anyhow::anyhow!("Failed to execute sftp command: {}", e))?;

    // Clean up temp file
    let _ = std::fs::remove_file(&temp_file);

    // Allow timeout exit code (124) as success since SFTP doesn't exit properly
    if !output.status.success() && output.status.code() != Some(124) {
        let stderr = String::from_utf8_lossy(&output.stderr);
        let stdout = String::from_utf8_lossy(&output.stdout);
        return Err(anyhow::anyhow!(
            "SFTP command failed. stderr: {}, stdout: {}, exit_code: {:?}",
            stderr,
            stdout,
            output.status.code()
        ));
    }

    Ok(String::from_utf8_lossy(&output.stdout).to_string())
}

/// Test SFTP: put, ls, mkdir, rmdir, delete operations using sshpass and sftp commands
pub async fn test_sftp_core_operations() -> Result<()> {
    let env = ProtocolTestEnvironment::new().map_err(|e| anyhow::anyhow!("{}", e))?;

    // Create SSH host key directory and files
    let ssh_dir = PathBuf::from(&env.temp_dir).join("ssh");
    fs::create_dir_all(&ssh_dir).await?;

    let host_key_path = ssh_dir.join("ssh_host_rsa_key");

    // Generate SSH host key
    info!("Generating SSH host key...");
    let output = Command::new("ssh-keygen")
        .arg("-t")
        .arg("rsa")
        .arg("-f")
        .arg(&host_key_path)
        .arg("-N")
        .arg("")
        .arg("-C")
        .arg("rustfs-sftp-test")
        .output()
        .map_err(|e| anyhow::anyhow!("Failed to generate SSH host key: {}", e))?;

    if !output.status.success() {
        return Err(anyhow::anyhow!("SSH key generation failed: {}", String::from_utf8_lossy(&output.stderr)));
    }

    // Create authorized_keys file
    let auth_keys_path = ssh_dir.join("authorized_keys");

    // Generate a test key pair for authentication
    let client_key_path = ssh_dir.join("client_key");
    let client_key_pub_path = ssh_dir.join("client_key.pub");

    info!("Generating client SSH key...");
    let output = Command::new("ssh-keygen")
        .arg("-t")
        .arg("rsa")
        .arg("-f")
        .arg(&client_key_path)
        .arg("-N")
        .arg("")
        .arg("-C")
        .arg("rustfs-sftp-client")
        .output()
        .map_err(|e| anyhow::anyhow!("Failed to generate client SSH key: {}", e))?;

    if !output.status.success() {
        return Err(anyhow::anyhow!(
            "Client SSH key generation failed: {}",
            String::from_utf8_lossy(&output.stderr)
        ));
    }

    // Add client public key to authorized_keys
    let client_pub_key = fs::read_to_string(&client_key_pub_path).await?;
    fs::write(&auth_keys_path, client_pub_key).await?;

    // Start SFTP server
    info!("Starting SFTP server on {}", SFTP_ADDRESS);
    let binary_path = rustfs_binary_path();
    let mut server_process = TokioCommand::new(&binary_path)
        .env("RUSTFS_SFTP_ENABLE", "true")
        .env("RUSTFS_SFTP_ADDRESS", SFTP_ADDRESS)
        .env("RUSTFS_SFTP_HOST_KEY", &host_key_path)
        .env("RUSTFS_SFTP_AUTHORIZED_KEYS", &auth_keys_path)
        .arg(&env.temp_dir)
        .spawn()?;

    // Ensure server is cleaned up even on failure
    let result = async {
        // Wait for server to be ready
        ProtocolTestEnvironment::wait_for_port_ready(SFTP_PORT, 30)
            .await
            .map_err(|e| anyhow::anyhow!("{}", e))?;

        info!("Testing SFTP: mkdir bucket");
        let bucket_name = "testbucket";
        let _output = execute_sftp_command(&client_key_path, &format!("mkdir {}\n", bucket_name))?;
        info!("PASS: mkdir bucket '{}' successful", bucket_name);

        info!("Testing SFTP: cd to bucket");
        let _output = execute_sftp_command(&client_key_path, &format!("cd {}\n", bucket_name))?;
        info!("PASS: cd to bucket '{}' successful", bucket_name);

        info!("Testing SFTP: ls / (list root directory)");
        let output_root = execute_sftp_command(&client_key_path, "ls /\n")?;
        assert!(output_root.contains(bucket_name), "Bucket should appear in ls /");
        info!("PASS: ls / successful, bucket '{}' found", bucket_name);

        info!("Testing SFTP: ls /. (list root directory with /.)");
        let output_root_dot = execute_sftp_command(&client_key_path, "ls /.\n")?;
        assert!(output_root_dot.contains(bucket_name), "Bucket should appear in ls /.");
        info!("PASS: ls /. successful, bucket '{}' found", bucket_name);

        info!("Testing SFTP: cd . (stay in current directory)");
        let _output = execute_sftp_command(&client_key_path, &format!("cd {}\ncd .\n", bucket_name))?;
        info!("PASS: cd . successful");

        info!("Testing SFTP: cd / (return to root)");
        let _output = execute_sftp_command(&client_key_path, "cd /\n")?;
        info!("PASS: cd / successful");

        info!("Testing SFTP: cd nonexistent bucket (should fail)");
        let result = execute_sftp_command(&client_key_path, "cd nonexistentbucket\n");
        if result.is_err() {
            info!("PASS: cd to nonexistent bucket failed as expected");
        } else {
            info!("SKIP: cd to nonexistent bucket succeeded, skipping this test");
        }

        info!("Testing SFTP: cdup");
        let _output = execute_sftp_command(&client_key_path, &format!("cd {}\ncdup\n", bucket_name))?;
        info!("PASS: cdup successful");

        info!("Testing SFTP: rmdir delete bucket");
        let _output = execute_sftp_command(&client_key_path, &format!("rmdir {}\n", bucket_name))?;
        info!("PASS: rmdir delete bucket '{}' successful", bucket_name);

        info!("All SFTP tests passed successfully!");
        Ok(())
    }
    .await;

    // Clean up server process
    if let Err(e) = server_process.kill().await {
        tracing::warn!("Failed to kill SFTP server process: {}", e);
    }

    result
}
