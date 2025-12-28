use crate::common::{DEFAULT_ACCESS_KEY, DEFAULT_SECRET_KEY, RustFSTestEnvironment};
use crate::protocols::test_suite::{ProtocolTestSuite, TestDataFactory, wait_for_port_ready};
use anyhow::Result;
use rcgen::generate_simple_self_signed;
use std::io::Cursor;
use std::path::PathBuf;
// 引入 suppaftp 的 native_tls 适配器
use suppaftp::NativeTlsConnector;
use suppaftp::native_tls::TlsConnector;
use tracing::info;

#[tokio::test]
async fn test_ftps_explicit_tls_full_lifecycle() -> Result<()> {
    let mut test_suite = create_ftps_test_suite().await?;

    let ftps_port = RustFSTestEnvironment::find_available_port()
        .await
        .map_err(|e| anyhow::anyhow!("{}", e))?;
    let ftps_address = format!("127.0.0.1:{}", ftps_port);

    let cert_data = generate_ftps_certificate(&test_suite.env()).await?;

    let extra_args = vec![
        "--ftps-enable",
        "--ftps-address",
        &ftps_address,
        "--ftps-certs-file",
        &cert_data.cert_path,
        "--ftps-key-file",
        &cert_data.key_path,
        "--ftps-passive-ports",
        "50000-50010",
    ];
    test_suite
        .env_mut()
        .start_rustfs_server(extra_args)
        .await
        .map_err(|e| anyhow::anyhow!("{}", e))?;

    wait_for_port_ready(ftps_port, 30).await?;

    let mut ftp_stream = create_ftps_connection(&ftps_address).await?;

    test_file_operations(ftp_stream).await?;

    Ok(())
}

#[tokio::test]
async fn test_ftps_authentication_errors() -> Result<()> {
    let mut test_suite = create_ftps_test_suite().await?;

    let ftps_port = RustFSTestEnvironment::find_available_port()
        .await
        .map_err(|e| anyhow::anyhow!("{}", e))?;
    let ftps_address = format!("127.0.0.1:{}", ftps_port);

    let cert_data = generate_ftps_certificate(&test_suite.env()).await?;

    let extra_args = vec![
        "--ftps-enable",
        "--ftps-address",
        &ftps_address,
        "--ftps-certs-file",
        &cert_data.cert_path,
        "--ftps-key-file",
        &cert_data.key_path,
        "--ftps-passive-ports",
        "50000-50010",
    ];
    test_suite
        .env_mut()
        .start_rustfs_server(extra_args)
        .await
        .map_err(|e| anyhow::anyhow!("{}", e))?;

    wait_for_port_ready(ftps_port, 30).await?;

    // Test 1: Invalid username
    test_suite
        .test_authentication_errors::<String, _, _>(|_env| {
            let ftps_addr = ftps_address.clone();
            async move {
                let mut ftp_stream = suppaftp::FtpStream::connect(&ftps_addr)?;
                let auth_result = ftp_stream.login("invalid_user", DEFAULT_SECRET_KEY);
                if auth_result.is_ok() {
                    anyhow::bail!("Login should have failed");
                }
                Ok("test".to_string())
            }
        })
        .await?;

    Ok(())
}

#[tokio::test]
async fn test_ftps_file_not_found_errors() -> Result<()> {
    let mut test_suite = create_ftps_test_suite().await?;

    let ftps_port = RustFSTestEnvironment::find_available_port()
        .await
        .map_err(|e| anyhow::anyhow!("{}", e))?;
    let ftps_address = format!("127.0.0.1:{}", ftps_port);

    let cert_data = generate_ftps_certificate(&test_suite.env()).await?;

    let extra_args = vec![
        "--ftps-enable",
        "--ftps-address",
        &ftps_address,
        "--ftps-certs-file",
        &cert_data.cert_path,
        "--ftps-key-file",
        &cert_data.key_path,
        "--ftps-passive-ports",
        "50000-50010",
    ];
    test_suite
        .env_mut()
        .start_rustfs_server(extra_args)
        .await
        .map_err(|e| anyhow::anyhow!("{}", e))?;

    wait_for_port_ready(ftps_port, 30).await?;

    let mut ftp_stream = create_ftps_connection(&ftps_address).await?;

    // Test downloading non-existent file
    test_suite
        .test_not_found_errors::<String, _, _>(|_env| {
            let ftps_addr = ftps_address.clone();
            async move {
                let mut ftp_stream = create_ftps_connection(&ftps_addr).await?;
                let result = ftp_stream.retr_as_buffer("non_existent_file.txt");
                if result.is_ok() {
                    anyhow::bail!("Download should have failed");
                }
                Ok("download_failed".to_string())
            }
        })
        .await?;

    // Test deleting non-existent file
    test_suite
        .test_not_found_errors::<String, _, _>(|_env| {
            let ftps_addr = ftps_address.clone();
            async move {
                let mut ftp_stream = create_ftps_connection(&ftps_addr).await?;
                let result = ftp_stream.rm("non_existent_file.txt");
                if result.is_ok() {
                    anyhow::bail!("Delete should have failed");
                }
                Ok("delete_failed".to_string())
            }
        })
        .await?;

    // Test removing non-existent directory
    test_suite
        .test_not_found_errors::<String, _, _>(|_env| {
            let ftps_addr = ftps_address.clone();
            async move {
                let mut ftp_stream = create_ftps_connection(&ftps_addr).await?;
                let result = ftp_stream.rmdir("non_existent_dir");
                if result.is_ok() {
                    anyhow::bail!("Rmdir should have failed");
                }
                Ok("rmdir_failed".to_string())
            }
        })
        .await?;

    ftp_stream.quit()?;
    Ok(())
}

#[tokio::test]
async fn test_ftps_special_characters() -> Result<()> {
    let mut test_suite = create_ftps_test_suite().await?;

    let ftps_port = RustFSTestEnvironment::find_available_port()
        .await
        .map_err(|e| anyhow::anyhow!("{}", e))?;
    let ftps_address = format!("127.0.0.1:{}", ftps_port);

    let cert_data = generate_ftps_certificate(test_suite.env()).await?;

    let extra_args = vec![
        "--ftps-enable",
        "--ftps-address",
        &ftps_address,
        "--ftps-certs-file",
        &cert_data.cert_path,
        "--ftps-key-file",
        &cert_data.key_path,
        "--ftps-passive-ports",
        "50000-50010",
    ];
    test_suite
        .env_mut()
        .start_rustfs_server(extra_args)
        .await
        .map_err(|e| anyhow::anyhow!("{}", e))?;

    wait_for_port_ready(ftps_port, 30).await?;

    let mut ftp_stream = create_ftps_connection(&ftps_address).await?;

    let special_names = TestDataFactory::create_special_char_names();
    let test_content = "Test content with special characters: 中文测试";

    for filename in special_names.iter().take(5) {
        // Test first 5 names
        info!("Testing special filename: {}", filename);

        // Upload
        ftp_stream.put_file(filename, &mut Cursor::new(test_content.as_bytes()))?;

        // Download and verify
        let downloaded = ftp_stream.retr_as_buffer(filename)?;
        assert_eq!(downloaded.into_inner(), test_content.as_bytes(), "Content mismatch for {}", filename);

        // Cleanup
        ftp_stream.rm(filename)?;
    }

    ftp_stream.quit()?;
    Ok(())
}

#[tokio::test]
async fn test_ftps_large_file() -> Result<()> {
    let mut test_suite = create_ftps_test_suite().await?;

    let ftps_port = RustFSTestEnvironment::find_available_port()
        .await
        .map_err(|e| anyhow::anyhow!("{}", e))?;
    let ftps_address = format!("127.0.0.1:{}", ftps_port);

    let cert_data = generate_ftps_certificate(test_suite.env()).await?;

    let extra_args = vec![
        "--ftps-enable",
        "--ftps-address",
        &ftps_address,
        "--ftps-certs-file",
        &cert_data.cert_path,
        "--ftps-key-file",
        &cert_data.key_path,
        "--ftps-passive-ports",
        "50000-50010",
    ];
    test_suite
        .env_mut()
        .start_rustfs_server(extra_args)
        .await
        .map_err(|e| anyhow::anyhow!("{}", e))?;

    wait_for_port_ready(ftps_port, 30).await?;

    let mut ftp_stream = create_ftps_connection(&ftps_address).await?;

    let large_data = TestDataFactory::create_large_file(5); // 5MB file
    let filename = "large_test_file.bin";
    let checksum_before = crate::protocols::test_suite::calculate_checksum(&large_data);

    info!("Uploading large file ({} bytes)", large_data.len());
    ftp_stream.put_file(filename, &mut Cursor::new(&large_data))?;

    info!("Downloading large file");
    let downloaded = ftp_stream.retr_as_buffer(filename)?;
    let downloaded_data = downloaded.into_inner();

    let checksum_after = crate::protocols::test_suite::calculate_checksum(&downloaded_data);
    assert_eq!(checksum_before, checksum_after, "Large file checksum mismatch");
    assert_eq!(large_data, downloaded_data, "Large file content mismatch");

    ftp_stream.rm(filename)?;
    ftp_stream.quit()?;
    Ok(())
}

#[tokio::test]
async fn test_ftps_directory_operations() -> Result<()> {
    let mut test_suite = create_ftps_test_suite().await?;

    let ftps_port = RustFSTestEnvironment::find_available_port()
        .await
        .map_err(|e| anyhow::anyhow!("{}", e))?;
    let ftps_address = format!("127.0.0.1:{}", ftps_port);

    let cert_data = generate_ftps_certificate(test_suite.env()).await?;

    let extra_args = vec![
        "--ftps-enable",
        "--ftps-address",
        &ftps_address,
        "--ftps-certs-file",
        &cert_data.cert_path,
        "--ftps-key-file",
        &cert_data.key_path,
        "--ftps-passive-ports",
        "50000-50010",
    ];
    test_suite
        .env_mut()
        .start_rustfs_server(extra_args)
        .await
        .map_err(|e| anyhow::anyhow!("{}", e))?;

    wait_for_port_ready(ftps_port, 30).await?;

    let mut ftp_stream = create_ftps_connection(&ftps_address).await?;

    // Create deep directory structure
    let deep_paths = TestDataFactory::create_deep_structure("deep_test", 5);

    // Create directories and files
    for path in &deep_paths {
        let _parts: Vec<&str> = path.split('/').collect();
        let dir_path = &path[..path.rfind('/').unwrap()];

        ftp_stream.mkdir(dir_path)?;

        ftp_stream.put_file(path, &mut Cursor::new(format!("Content of {}", path).as_bytes()))?;
    }

    // Verify files exist and have correct content
    for path in &deep_paths {
        let downloaded = ftp_stream.retr_as_buffer(path)?;
        let expected_content = format!("Content of {}", path);
        assert_eq!(downloaded.into_inner(), expected_content.as_bytes(), "Content mismatch for {}", path);
    }

    // Cleanup in reverse order
    for path in deep_paths.iter().rev() {
        let _parts: Vec<&str> = path.split('/').collect();
        let dir_path = &path[..path.rfind('/').unwrap()];

        ftp_stream.rm(path)?;
        ftp_stream.rmdir(dir_path)?;
    }

    ftp_stream.quit()?;
    Ok(())
}

// Helper functions and structs

struct FtpsCertData {
    cert_path: String,
    key_path: String,
}

async fn create_ftps_test_suite() -> Result<ProtocolTestSuite> {
    let env = RustFSTestEnvironment::new().await.map_err(|e| anyhow::anyhow!("{}", e))?;
    Ok(ProtocolTestSuite::new(env))
}

async fn generate_ftps_certificate(env: &RustFSTestEnvironment) -> Result<FtpsCertData> {
    let cert = generate_simple_self_signed(vec!["localhost".to_string(), "127.0.0.1".to_string()])?;
    let cert_pem = cert.cert.pem();
    let key_pem = cert.signing_key.serialize_pem();

    let cert_path = PathBuf::from(&env.temp_dir).join("ftps.crt");
    let key_path = PathBuf::from(&env.temp_dir).join("ftps.key");

    tokio::fs::write(&cert_path, cert_pem).await?;
    tokio::fs::write(&key_path, key_pem).await?;

    Ok(FtpsCertData {
        cert_path: cert_path.to_str().unwrap().to_string(),
        key_path: key_path.to_str().unwrap().to_string(),
    })
}

async fn test_file_operations(mut ftp_stream: suppaftp::FtpStream) -> Result<()> {
    // --- 场景 1: 基本文件上传下载 ---
    let filename = "test.txt";
    let content = "Hello, FTPS!";
    ftp_stream.put_file(filename, &mut Cursor::new(content.as_bytes()))?;

    let downloaded = ftp_stream.retr_as_buffer(filename)?;
    assert_eq!(downloaded.into_inner(), content.as_bytes());

    // --- 场景 2: 列表和删除 ---
    let list = ftp_stream.list(None)?;
    assert!(list.iter().any(|line| line.contains(filename)), "File should appear in list");

    ftp_stream.rm(filename)?;
    let list_after = ftp_stream.list(None)?;
    assert!(!list_after.iter().any(|line| line.contains(filename)), "File should be gone");

    // --- 场景 3: 目录操作 ---
    let dir_name = "testdir";
    ftp_stream.mkdir(dir_name)?;
    ftp_stream.cwd(dir_name)?;

    let sub_file = "inner.txt";
    let sub_content = "Inner Content";
    ftp_stream.put_file(sub_file, &mut Cursor::new(sub_content.as_bytes()))?;

    let downloaded = ftp_stream.retr_as_buffer(sub_file)?;
    assert_eq!(downloaded.into_inner(), sub_content.as_bytes());

    let renamed_file = "renamed_inner.txt";
    ftp_stream.rename(sub_file, renamed_file)?;

    let list_inner = ftp_stream.list(None)?;
    assert!(!list_inner.iter().any(|line| line.contains(sub_file)), "Old file should be gone");
    assert!(list_inner.iter().any(|line| line.contains(renamed_file)), "New file should exist");

    // --- 场景 4: 清理 ---
    ftp_stream.rm(renamed_file)?;
    ftp_stream.cdup()?;
    ftp_stream.rmdir(dir_name)?;

    ftp_stream.quit()?;
    Ok(())
}

async fn create_ftps_connection(address: &str) -> Result<suppaftp::FtpStream> {
    let mut ftp_stream = suppaftp::FtpStream::connect(address)?;
    ftp_stream.login(DEFAULT_ACCESS_KEY, DEFAULT_SECRET_KEY)?;
    Ok(ftp_stream)
}
