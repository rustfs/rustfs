use crate::common::{RustFSTestEnvironment, DEFAULT_ACCESS_KEY, DEFAULT_SECRET_KEY};
use anyhow::Result;
use rcgen::generate_simple_self_signed;
use std::io::Cursor;
use std::path::PathBuf;
use std::time::Duration;
use suppaftp::native_tls::{TlsConnector, TlsStream};
use suppaftp::FtpStream;
use tokio::time::sleep;
use tracing::info;

#[tokio::test]
async fn test_ftps_explicit_tls_workflow() -> Result<()> {
    // 1. 准备环境
    let mut env = RustFSTestEnvironment::new().await?;
    let ftps_port = RustFSTestEnvironment::find_available_port().await?;
    let ftps_address = format!("127.0.0.1:{}", ftps_port);

    // 2. 生成临时证书 (保存到 temp_dir)
    let cert = generate_simple_self_signed(vec!["localhost".to_string(), "127.0.0.1".to_string()])?;
    let cert_pem = cert.serialize_pem()?;
    let key_pem = cert.serialize_private_key_pem();

    let cert_path = PathBuf::from(&env.temp_dir).join("ftps.crt");
    let key_path = PathBuf::from(&env.temp_dir).join("ftps.key");

    tokio::fs::write(&cert_path, cert_pem).await?;
    tokio::fs::write(&key_path, key_pem).await?;

    // 3. 启动服务器 (启用 FTPS)
    let extra_args = vec![
        "--ftps-enable",
        "--ftps-address",
        &ftps_address,
        "--ftps-certs-file",
        cert_path.to_str().unwrap(),
        "--ftps-key-file",
        key_path.to_str().unwrap(),
    ];
    env.start_rustfs_server(extra_args).await?;

    // 4. 等待 FTPS 端口就绪
    wait_for_port(&ftps_address).await?;

    // 5. 建立 FTP 连接
    let mut ftp_stream = FtpStream::connect(&ftps_address)?;
    info!("Connected to FTPS server");

    // 6. 升级到 TLS (Explicit FTPS)
    let connector = TlsConnector::builder()
        .danger_accept_invalid_certs(true) // 测试用自签名证书
        .danger_accept_invalid_hostnames(true)
        .build()?;

    ftp_stream.auth_tls(&connector)?;
    info!("Upgraded to TLS");

    // 7. 登录
    ftp_stream.login(DEFAULT_ACCESS_KEY, DEFAULT_SECRET_KEY)?;

    // 8. 强制数据通道加密 (RustFS 要求 PROT P)
    ftp_stream.perform_command("PBSZ 0")?;
    ftp_stream.perform_command("PROT P")?;

    // --- 测试场景 1: 上传文件 ---
    let filename = "ftps_test.txt";
    let content = "Hello RustFS FTPS!";
    let mut reader = Cursor::new(content.as_bytes());

    ftp_stream.put_file(filename, &mut reader)?;
    info!("File uploaded via FTPS");

    // --- 测试场景 2: 列表 (List) ---
    // 注意：List 在 FTPS 中需要建立数据连接，如果 PROT P 未生效这里会失败
    let list = ftp_stream.list(None)?;
    assert!(list.iter().any(|line| line.contains(filename)), "File missing from LIST");

    // --- 测试场景 3: 下载文件 ---
    let downloaded = ftp_stream.retr_as_buffer(filename)?;
    assert_eq!(downloaded, content.as_bytes(), "Content mismatch");
    info!("File downloaded via FTPS");

    // --- 测试场景 4: 删除文件 ---
    ftp_stream.rm(filename)?;

    // 验证删除
    let list_after = ftp_stream.list(None)?;
    assert!(!list_after.iter().any(|line| line.contains(filename)), "File should be deleted");

    ftp_stream.quit()?;
    Ok(())
}

async fn wait_for_port(addr: &str) -> Result<()> {
    for _ in 0..30 {
        if std::net::TcpStream::connect(addr).is_ok() {
            return Ok(());
        }
        sleep(Duration::from_millis(500)).await;
    }
    Err(anyhow::anyhow!("Timeout waiting for port {}", addr))
}