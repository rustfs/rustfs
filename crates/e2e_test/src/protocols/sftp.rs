use crate::common::{RustFSTestEnvironment, DEFAULT_ACCESS_KEY, DEFAULT_SECRET_KEY};
use anyhow::Result;
use ssh2::Session;
use std::io::{Read, Write};
use std::net::TcpStream;
use std::path::Path;
use std::time::Duration;
use tokio::time::sleep;
use tracing::info;

#[tokio::test]
async fn test_sftp_basic_workflow() -> Result<()> {
    // 1. 准备环境
    let mut env = RustFSTestEnvironment::new().await?;
    let sftp_port = RustFSTestEnvironment::find_available_port().await?;
    let sftp_address = format!("127.0.0.1:{}", sftp_port);

    // 2. 启动服务器 (启用 SFTP)
    // 注意：根据代码逻辑，key_file 传 None 时服务器会自动生成 host key，所以这里不需要配置 key
    let extra_args = vec![
        "--sftp-enable",
        "--sftp-address",
        &sftp_address,
    ];
    env.start_rustfs_server(extra_args).await?;

    // 3. 等待 SFTP 端口就绪
    wait_for_port(&sftp_address).await?;

    // 4. 建立 SSH 连接
    let tcp = TcpStream::connect(&sftp_address)?;
    let mut sess = Session::new()?;
    sess.set_tcp_stream(tcp);
    sess.handshake()?;

    // 5. 认证 (使用默认的 minioadmin/minioadmin)
    sess.userauth_password("rustfsadmin", "rustfsadmin")?;
    assert!(sess.authenticated(), "SSH authentication failed");

    // 6. 初始化 SFTP
    let sftp = sess.sftp()?;
    info!("SFTP session established");

    // --- 测试场景 1: 上传文件 ---
    let filename = "sftp_test.txt";
    let content = b"Hello from RustFS SFTP E2E Test!";

    let mut file = sftp.create(Path::new(filename))?;
    file.write_all(content)?;
    // 必须 drop file 或者手动 flush/close 才能确保数据写入
    drop(file);
    info!("File uploaded via SFTP");

    // --- 测试场景 2: 验证文件属性 (Stat) ---
    let stat = sftp.stat(Path::new(filename))?;
    assert_eq!(stat.size.unwrap(), content.len() as u64, "File size mismatch");

    // --- 测试场景 3: 下载文件 ---
    let mut file = sftp.open(Path::new(filename))?;
    let mut download_buf = Vec::new();
    file.read_to_end(&mut download_buf)?;
    assert_eq!(download_buf, content, "Downloaded content mismatch");
    info!("File downloaded via SFTP");

    // --- 测试场景 4: 删除文件 ---
    sftp.unlink(Path::new(filename))?;

    // 验证删除
    assert!(sftp.open(Path::new(filename)).is_err(), "File should be deleted");
    info!("File deleted via SFTP");

    Ok(())
}

async fn wait_for_port(addr: &str) -> Result<()> {
    for _ in 0..30 {
        if TcpStream::connect(addr).is_ok() {
            return Ok(());
        }
        sleep(Duration::from_millis(500)).await;
    }
    Err(anyhow::anyhow!("Timeout waiting for port {}", addr))
}