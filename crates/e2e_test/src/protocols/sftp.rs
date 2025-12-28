use crate::common::{DEFAULT_ACCESS_KEY, DEFAULT_SECRET_KEY, RustFSTestEnvironment};
use crate::protocols::test_suite::{ProtocolTestSuite, TestDataFactory, wait_for_port_ready};
use anyhow::Result;
use ssh2::Session;
use std::io::{Read, Write};
use std::net::TcpStream;
use std::path::Path;
use tracing::info;

#[tokio::test]
async fn test_sftp_full_lifecycle() -> Result<()> {
    let mut test_suite = create_sftp_test_suite().await?;

    let sftp_port = RustFSTestEnvironment::find_available_port()
        .await
        .map_err(|e| anyhow::anyhow!("{}", e))?;
    let sftp_address = format!("127.0.0.1:{}", sftp_port);

    let extra_args = vec!["--sftp-enable", "--sftp-address", &sftp_address];
    test_suite
        .env_mut()
        .start_rustfs_server(extra_args)
        .await
        .map_err(|e| anyhow::anyhow!("{}", e))?;

    wait_for_port_ready(sftp_port, 30).await?;

    let (_session, sftp) = create_sftp_connection(&sftp_address).await?;

    test_sftp_file_operations(&sftp).await?;

    Ok(())
}

#[tokio::test]
async fn test_sftp_authentication_errors() -> Result<()> {
    let mut test_suite = create_sftp_test_suite().await?;

    let sftp_port = RustFSTestEnvironment::find_available_port()
        .await
        .map_err(|e| anyhow::anyhow!("{}", e))?;
    let sftp_address = format!("127.0.0.1:{}", sftp_port);

    let extra_args = vec!["--sftp-enable", "--sftp-address", &sftp_address];
    test_suite
        .env_mut()
        .start_rustfs_server(extra_args)
        .await
        .map_err(|e| anyhow::anyhow!("{}", e))?;

    wait_for_port_ready(sftp_port, 30).await?;

    // Test 1: Invalid username
    test_suite
        .test_authentication_errors::<String, _, _>(|_env| {
            let sftp_addr = sftp_address.clone();
            async move {
                let tcp = TcpStream::connect(&sftp_addr)?;
                let mut sess = Session::new()?;
                sess.set_tcp_stream(tcp);
                sess.handshake()?;

                let auth_result = sess.userauth_password("invalid_user", DEFAULT_SECRET_KEY);
                if auth_result.is_ok() {
                    anyhow::bail!("Authentication should have failed");
                }
                Ok("auth_failed".to_string())
            }
        })
        .await?;

    // Test 2: Invalid password
    test_suite
        .test_authentication_errors::<String, _, _>(|_env| {
            let sftp_addr = sftp_address.clone();
            async move {
                let tcp = TcpStream::connect(&sftp_addr)?;
                let mut sess = Session::new()?;
                sess.set_tcp_stream(tcp);
                sess.handshake()?;

                let auth_result = sess.userauth_password(DEFAULT_ACCESS_KEY, "invalid_password");
                if auth_result.is_ok() {
                    anyhow::bail!("Authentication should have failed");
                }
                Ok("auth_failed".to_string())
            }
        })
        .await?;

    // Test 3: Both invalid
    test_suite
        .test_authentication_errors::<String, _, _>(|_env| {
            let sftp_addr = sftp_address.clone();
            async move {
                let tcp = TcpStream::connect(&sftp_addr)?;
                let mut sess = Session::new()?;
                sess.set_tcp_stream(tcp);
                sess.handshake()?;

                let auth_result = sess.userauth_password("invalid_user", "invalid_password");
                if auth_result.is_ok() {
                    anyhow::bail!("Authentication should have failed");
                }
                Ok("auth_failed".to_string())
            }
        })
        .await?;

    Ok(())
}

#[tokio::test]
async fn test_sftp_file_not_found_errors() -> Result<()> {
    let mut test_suite = create_sftp_test_suite().await?;

    let sftp_port = RustFSTestEnvironment::find_available_port()
        .await
        .map_err(|e| anyhow::anyhow!("{}", e))?;
    let sftp_address = format!("127.0.0.1:{}", sftp_port);

    let extra_args = vec!["--sftp-enable", "--sftp-address", &sftp_address];
    test_suite
        .env_mut()
        .start_rustfs_server(extra_args)
        .await
        .map_err(|e| anyhow::anyhow!("{}", e))?;

    wait_for_port_ready(sftp_port, 30).await?;

    let (_session, sftp) = create_sftp_connection(&sftp_address).await?;

    // Test reading non-existent file
    test_suite
        .test_not_found_errors::<String, _, _>(|_env| {
            let sftp_addr = sftp_address.clone();
            async move {
                let (_session, sftp) = create_sftp_connection(&sftp_addr).await?;
                let result = sftp.open(Path::new("non_existent_file.txt"));
                if result.is_ok() {
                    anyhow::bail!("Read should have failed");
                }
                Ok("read_failed".to_string())
            }
        })
        .await?;

    // Test removing non-existent file
    test_suite
        .test_not_found_errors::<String, _, _>(|_env| {
            let sftp_addr = sftp_address.clone();
            async move {
                let (_session, sftp) = create_sftp_connection(&sftp_addr).await?;
                let result = sftp.unlink(Path::new("non_existent_file.txt"));
                if result.is_ok() {
                    anyhow::bail!("Unlink should have failed");
                }
                Ok("unlink_failed".to_string())
            }
        })
        .await?;

    // Test removing non-existent directory
    test_suite
        .test_not_found_errors::<String, _, _>(|_env| {
            let sftp_addr = sftp_address.clone();
            async move {
                let (_session, sftp) = create_sftp_connection(&sftp_addr).await?;
                let result = sftp.rmdir(Path::new("non_existent_dir"));
                if result.is_ok() {
                    anyhow::bail!("Rmdir should have failed");
                }
                Ok("rmdir_failed".to_string())
            }
        })
        .await?;

    Ok(())
}

#[tokio::test]
async fn test_sftp_special_characters() -> Result<()> {
    let mut test_suite = create_sftp_test_suite().await?;

    let sftp_port = RustFSTestEnvironment::find_available_port()
        .await
        .map_err(|e| anyhow::anyhow!("{}", e))?;
    let sftp_address = format!("127.0.0.1:{}", sftp_port);

    let extra_args = vec!["--sftp-enable", "--sftp-address", &sftp_address];
    test_suite
        .env_mut()
        .start_rustfs_server(extra_args)
        .await
        .map_err(|e| anyhow::anyhow!("{}", e))?;

    wait_for_port_ready(sftp_port, 30).await?;

    let (_session, sftp) = create_sftp_connection(&sftp_address).await?;

    let special_names = TestDataFactory::create_special_char_names();
    let test_content = "Test content with special characters: 中文测试";

    for filename in special_names.iter().take(5) {
        // Test first 5 names
        info!("Testing special filename: {}", filename);

        // Upload
        {
            let mut file = sftp.create(Path::new(filename))?;
            file.write_all(test_content.as_bytes())?;
            file.flush()?;
            drop(file); // Trigger S3 PutObject
        }

        // Download and verify
        {
            let mut file = sftp.open(Path::new(filename))?;
            let mut downloaded_content = String::new();
            file.read_to_string(&mut downloaded_content)?;
            assert_eq!(downloaded_content, test_content, "Content mismatch for {}", filename);
        }

        // Cleanup
        sftp.unlink(Path::new(filename))?;
    }

    Ok(())
}

#[tokio::test]
async fn test_sftp_large_file() -> Result<()> {
    let mut test_suite = create_sftp_test_suite().await?;

    let sftp_port = RustFSTestEnvironment::find_available_port()
        .await
        .map_err(|e| anyhow::anyhow!("{}", e))?;
    let sftp_address = format!("127.0.0.1:{}", sftp_port);

    let extra_args = vec!["--sftp-enable", "--sftp-address", &sftp_address];
    test_suite
        .env_mut()
        .start_rustfs_server(extra_args)
        .await
        .map_err(|e| anyhow::anyhow!("{}", e))?;

    wait_for_port_ready(sftp_port, 30).await?;

    let (_session, sftp) = create_sftp_connection(&sftp_address).await?;

    let large_data = TestDataFactory::create_large_file(5); // 5MB file
    let filename = "large_test_file.bin";
    let checksum_before = crate::protocols::test_suite::calculate_checksum(&large_data);

    info!("Uploading large file ({} bytes)", large_data.len());
    {
        let mut file = sftp.create(Path::new(filename))?;
        file.write_all(&large_data)?;
        file.flush()?;
        drop(file); // Trigger S3 PutObject
    }

    info!("Downloading large file");
    let downloaded_data = {
        let mut file = sftp.open(Path::new(filename))?;
        let mut buffer = Vec::new();
        file.read_to_end(&mut buffer)?;
        buffer
    };

    let checksum_after = crate::protocols::test_suite::calculate_checksum(&downloaded_data);
    assert_eq!(checksum_before, checksum_after, "Large file checksum mismatch");
    assert_eq!(large_data, downloaded_data, "Large file content mismatch");

    sftp.unlink(Path::new(filename))?;
    Ok(())
}

#[tokio::test]
async fn test_sftp_directory_operations() -> Result<()> {
    let mut test_suite = create_sftp_test_suite().await?;

    let sftp_port = RustFSTestEnvironment::find_available_port()
        .await
        .map_err(|e| anyhow::anyhow!("{}", e))?;
    let sftp_address = format!("127.0.0.1:{}", sftp_port);

    let extra_args = vec!["--sftp-enable", "--sftp-address", &sftp_address];
    test_suite
        .env_mut()
        .start_rustfs_server(extra_args)
        .await
        .map_err(|e| anyhow::anyhow!("{}", e))?;

    wait_for_port_ready(sftp_port, 30).await?;

    let (_session, sftp) = create_sftp_connection(&sftp_address).await?;

    // Create deep directory structure
    let deep_paths = TestDataFactory::create_deep_structure("deep_test", 5);

    // Create directories and files
    for path in &deep_paths {
        let _parts: Vec<&str> = path.split('/').collect();
        let dir_path = &path[..path.rfind('/').unwrap()];

        sftp.mkdir(Path::new(dir_path), 0o755)?;

        let content = format!("Content of {}", path);
        let mut file = sftp.create(Path::new(path))?;
        file.write_all(content.as_bytes())?;
        file.flush()?;
        drop(file); // Trigger S3 PutObject
    }

    // Verify files exist and have correct content
    for path in &deep_paths {
        let mut file = sftp.open(Path::new(path))?;
        let mut downloaded_content = String::new();
        file.read_to_string(&mut downloaded_content)?;
        let expected_content = format!("Content of {}", path);
        assert_eq!(downloaded_content, expected_content, "Content mismatch for {}", path);
    }

    // Cleanup in reverse order
    for path in deep_paths.iter().rev() {
        let _parts: Vec<&str> = path.split('/').collect();
        let dir_path = &path[..path.rfind('/').unwrap()];

        sftp.unlink(Path::new(path))?;
        sftp.rmdir(Path::new(dir_path))?;
    }

    Ok(())
}

#[tokio::test]
async fn test_sftp_rename_operations() -> Result<()> {
    let mut test_suite = create_sftp_test_suite().await?;

    let sftp_port = RustFSTestEnvironment::find_available_port()
        .await
        .map_err(|e| anyhow::anyhow!("{}", e))?;
    let sftp_address = format!("127.0.0.1:{}", sftp_port);

    let extra_args = vec!["--sftp-enable", "--sftp-address", &sftp_address];
    test_suite
        .env_mut()
        .start_rustfs_server(extra_args)
        .await
        .map_err(|e| anyhow::anyhow!("{}", e))?;

    wait_for_port_ready(sftp_port, 30).await?;

    let (_session, sftp) = create_sftp_connection(&sftp_address).await?;

    // Test file rename
    let original_file = "original_file.txt";
    let renamed_file = "renamed_file.txt";
    let content = "Content for rename test";

    // Create original file
    {
        let mut file = sftp.create(Path::new(original_file))?;
        file.write_all(content.as_bytes())?;
        file.flush()?;
        drop(file);
    }

    // Rename file
    sftp.rename(Path::new(original_file), Path::new(renamed_file), None)?;

    // Verify renamed file exists and has correct content
    {
        let mut file = sftp.open(Path::new(renamed_file))?;
        let mut downloaded_content = String::new();
        file.read_to_string(&mut downloaded_content)?;
        assert_eq!(downloaded_content, content, "Renamed file content mismatch");
    }

    // Verify original file no longer exists
    test_suite
        .test_not_found_errors::<String, _, _>(|_env| {
            let sftp_addr = sftp_address.clone();
            async move {
                let (_session, sftp) = create_sftp_connection(&sftp_addr).await?;
                let result = sftp.open(Path::new(original_file));
                if result.is_ok() {
                    anyhow::bail!("Read should have failed");
                }
                Ok("read_failed".to_string())
            }
        })
        .await?;

    // Cleanup
    sftp.unlink(Path::new(renamed_file))?;
    Ok(())
}

#[tokio::test]
async fn test_sftp_file_attributes() -> Result<()> {
    let mut test_suite = create_sftp_test_suite().await?;

    let sftp_port = RustFSTestEnvironment::find_available_port()
        .await
        .map_err(|e| anyhow::anyhow!("{}", e))?;
    let sftp_address = format!("127.0.0.1:{}", sftp_port);

    let extra_args = vec!["--sftp-enable", "--sftp-address", &sftp_address];
    test_suite
        .env_mut()
        .start_rustfs_server(extra_args)
        .await
        .map_err(|e| anyhow::anyhow!("{}", e))?;

    wait_for_port_ready(sftp_port, 30).await?;

    let (_session, sftp) = create_sftp_connection(&sftp_address).await?;

    let filename = "attributes_test.txt";
    let content = "Content for attributes test";

    // Create file
    {
        let mut file = sftp.create(Path::new(filename))?;
        file.write_all(content.as_bytes())?;
        file.flush()?;
        drop(file);
    }

    // Check file attributes
    let attrs = sftp.stat(Path::new(filename))?;
    assert!(attrs.size.unwrap_or(0) > 0, "File should have non-zero size");
    assert!(!attrs.is_dir(), "Should be a file, not a directory");

    // Test directory attributes
    let dir_name = "test_dir";
    sftp.mkdir(Path::new(dir_name), 0o755)?;

    let dir_attrs = sftp.stat(Path::new(dir_name))?;
    assert!(dir_attrs.is_dir(), "Should be a directory");

    // Cleanup
    sftp.unlink(Path::new(filename))?;
    sftp.rmdir(Path::new(dir_name))?;
    Ok(())
}

// Helper functions

async fn create_sftp_test_suite() -> Result<ProtocolTestSuite> {
    let env = RustFSTestEnvironment::new().await.map_err(|e| anyhow::anyhow!("{}", e))?;
    Ok(ProtocolTestSuite::new(env))
}

async fn create_sftp_connection(address: &str) -> Result<(Session, ssh2::Sftp)> {
    let tcp = TcpStream::connect(address)?;
    let mut sess = Session::new()?;
    sess.set_tcp_stream(tcp);
    sess.handshake()?;

    sess.userauth_password(DEFAULT_ACCESS_KEY, DEFAULT_SECRET_KEY)?;
    assert!(sess.authenticated(), "SSH authentication failed");

    let sftp = sess.sftp()?;
    info!("SFTP session established");

    Ok((sess, sftp))
}

async fn test_sftp_file_operations(sftp: &ssh2::Sftp) -> Result<()> {
    // --- 场景 1: 文件上传 (Write-on-Close) ---
    let filename = "sftp_test.txt";
    let content = b"Hello from RustFS SFTP E2E Test!";

    {
        let mut file = sftp.create(Path::new(filename))?;
        file.write_all(content)?;
        file.flush()?;
        drop(file); // 关键点：RustFS SFTP 实现依赖 Close (drop) 来触发 S3 PutObject
    }
    info!("File uploaded via SFTP");

    // --- 场景 2: 文件下载 ---
    let mut downloaded_content = Vec::new();
    {
        let mut file = sftp.open(Path::new(filename))?;
        file.read_to_end(&mut downloaded_content)?;
    }
    assert_eq!(downloaded_content, content, "Downloaded content should match uploaded content");

    // --- 场景 3: 目录操作 ---
    let dir_name = "sftp_test_dir";
    sftp.mkdir(Path::new(dir_name), 0o755)?;

    let sub_file = "sub_file.txt";
    let sub_content = b"Sub file content";
    let sub_path = format!("{}/{}", dir_name, sub_file);

    {
        let mut file = sftp.create(Path::new(&sub_path))?;
        file.write_all(sub_content)?;
        file.flush()?;
        drop(file);
    }

    let mut downloaded_sub = Vec::new();
    {
        let mut file = sftp.open(Path::new(&sub_path))?;
        file.read_to_end(&mut downloaded_sub)?;
    }
    assert_eq!(downloaded_sub, sub_content, "Sub file content should match");

    // --- 场景 4: 列表操作 ---
    let file_attrs = sftp.stat(Path::new(filename))?;
    assert!(file_attrs.size.unwrap_or(0) > 0, "File should have content");

    // --- 场景 5: 清理 ---
    sftp.unlink(Path::new(&sub_path))?;
    sftp.rmdir(Path::new(dir_name))?;
    sftp.unlink(Path::new(filename))?;

    Ok(())
}
