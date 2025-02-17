use crate::utils::RustFSConfig;
use dioxus::logger::tracing::{debug, error, info};
use futures_util::TryStreamExt;
use std::error::Error;
use std::fs::File;
use std::path::{Path, PathBuf};
use std::process::Command as StdCommand;
use std::time::Duration;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;
use tokio::sync::mpsc;

/// Service command
/// This enum represents the commands that can be sent to the service manager
/// to start, stop, or restart the service
/// The `Start` variant contains the configuration for the service
/// The `Restart` variant contains the configuration for the service
///
/// # Example
/// ```
/// let config = RustFSConfig {
///    address: "127.0.0.1:9000".to_string(),
///    host: "127.0.0.1".to_string(),
///    port: "9000".to_string(),
///    access_key: "rustfsadmin".to_string(),
///    secret_key: "rustfsadmin".to_string(),
///    domain_name: "demo.rustfs.com".to_string(),
///    volume_name: "data".to_string(),
///    console_address: "127.0.0.1:9001".to_string(),
/// };
///
/// let command = ServiceCommand::Start(config);
/// println!("{:?}", command);
///
/// assert_eq!(command, ServiceCommand::Start(config));
/// ```
pub enum ServiceCommand {
    Start(RustFSConfig),
    Stop,
    Restart(RustFSConfig),
}

/// Service operation result
/// This struct represents the result of a service operation
/// It contains information about the success of the operation,
///
/// # Example
/// ```
/// use chrono::Local;
///
/// let result = ServiceOperationResult {
///     success: true,
///     start_time: chrono::Local::now(),
///     end_time: chrono::Local::now(),
///     message: "服务启动成功".to_string(),
/// };
///
/// println!("{:?}", result);
/// assert_eq!(result.success, true);
/// ```
#[derive(Debug)]
pub struct ServiceOperationResult {
    pub success: bool,
    pub start_time: chrono::DateTime<chrono::Local>,
    pub end_time: chrono::DateTime<chrono::Local>,
    pub message: String,
}

/// Service manager
/// This struct represents a service manager that can be used to start, stop, or restart a service
/// It contains a command sender that can be used to send commands to the service manager
///
/// # Example
/// ```
/// let service_manager = ServiceManager::new();
/// println!("{:?}", service_manager);
/// ```
#[derive(Debug, Clone)]
pub struct ServiceManager {
    command_tx: mpsc::Sender<ServiceCommand>,
    // process: Arc<Mutex<Option<Child>>>,
    // pid: Arc<Mutex<Option<u32>>>,                     // Add PID storage
    // current_config: Arc<Mutex<Option<RustFSConfig>>>, // Add configuration storage
}

impl ServiceManager {
    /// check if the service is running and return a pid
    /// This function is platform dependent
    /// On Unix systems, it uses the `ps` command to check for the service
    /// On Windows systems, it uses the `wmic` command to check for the service
    ///
    /// # Example
    /// ```
    /// let pid = check_service_status().await;
    /// println!("{:?}", pid);
    /// ```
    pub async fn check_service_status() -> Option<u32> {
        #[cfg(unix)]
        {
            // use the ps command on a unix system
            if let Ok(output) = StdCommand::new("ps").arg("-ef").output() {
                let output_str = String::from_utf8_lossy(&output.stdout);
                for line in output_str.lines() {
                    // match contains `rustfs/bin/rustfs` of the line
                    if line.contains("rustfs/bin/rustfs") && !line.contains("grep") {
                        if let Some(pid_str) = line.split_whitespace().nth(1) {
                            if let Ok(pid) = pid_str.parse::<u32>() {
                                return Some(pid);
                            }
                        }
                    }
                }
            }
        }

        #[cfg(windows)]
        {
            if let Ok(output) = StdCommand::new("wmic")
                .arg("process")
                .arg("where")
                .arg("caption='rustfs.exe'")
                .arg("get")
                .arg("processid")
                .output()
            {
                let output_str = String::from_utf8_lossy(&output.stdout);
                for line in output_str.lines() {
                    if let Ok(pid) = line.trim().parse::<u32>() {
                        return Some(pid);
                    }
                }
            }
        }

        None
    }

    /// Prepare the service
    /// This function downloads the service executable if it doesn't exist
    /// It also creates the necessary directories for the service
    ///
    /// # Example
    /// ```
    /// let executable_path = prepare_service().await;
    /// println!("{:?}", executable_path);
    /// ```
    async fn prepare_service() -> Result<PathBuf, Box<dyn Error>> {
        // get the user directory
        let home_dir = dirs::home_dir().ok_or("无法获取用户目录")?;
        let rustfs_dir = home_dir.join("rustfs");
        let bin_dir = rustfs_dir.join("bin");
        let data_dir = rustfs_dir.join("data");
        let logs_dir = rustfs_dir.join("logs");

        // create the necessary directories
        for dir in [&bin_dir, &data_dir, &logs_dir] {
            if !dir.exists() {
                tokio::fs::create_dir_all(dir).await?;
            }
        }

        let executable_path = if cfg!(windows) {
            bin_dir.join("rustfs.exe")
        } else {
            bin_dir.join("rustfs")
        };

        // If the executable file doesn't exist, download and unzip it
        if !executable_path.exists() {
            // download the file
            let tmp_zip = rustfs_dir.join("rustfs.zip");
            let file_download_url = if cfg!(windows) {
                "https://api.xmb.xyz/download/rustfs-win.zip"
            } else {
                "https://api.xmb.xyz/download/rustfs.zip"
            };

            let download_task = Self::download_file(file_download_url, &tmp_zip);
            let unzip_task = async {
                download_task.await?;
                Self::unzip_file(&tmp_zip, &bin_dir)?;
                tokio::fs::remove_file(&tmp_zip).await?;
                Ok::<(), Box<dyn Error>>(())
            };
            unzip_task.await?;

            // delete the temporary zip file
            tokio::fs::remove_file(&tmp_zip).await?;

            // set execution permissions on unix systems
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                let mut perms = std::fs::metadata(&executable_path)?.permissions();
                perms.set_mode(0o755);
                std::fs::set_permissions(&executable_path, perms)?;
            }
            Self::show_info("服务程序已成功下载并准备就绪");
        }

        Ok(executable_path)
    }

    /// Download the file
    /// This function is a modified version of the example from the `reqwest` crate documentation
    ///
    /// # Example
    /// ```
    /// let url = "https://api.xmb.xyz/download/rustfs.zip";
    /// let path = Path::new("rustfs.zip");
    /// download_file(url, path).await;
    /// ```
    async fn download_file(url: &str, path: &Path) -> Result<(), Box<dyn Error>> {
        let client = reqwest::Client::new();
        let res = client.get(url).send().await?;

        if !res.status().is_success() {
            return Err(format!("下载失败：{}", res.status()).into());
        }

        let mut file = tokio::fs::File::create(path).await?;
        let mut stream = res.bytes_stream();
        while let Ok(Some(chunk)) = stream.try_next().await {
            file.write_all(&chunk).await?;
        }

        Ok(())
    }

    /// unzip the file
    /// This function is a modified version of the example from the `zip` crate documentation
    ///
    /// # Example
    /// ```
    /// let zip_path = Path::new("rustfs.zip");
    /// let extract_path = Path::new("rustfs");
    /// unzip_file(zip_path, extract_path);
    /// ```
    fn unzip_file(zip_path: &Path, extract_path: &Path) -> Result<(), Box<dyn Error>> {
        let file = File::open(zip_path)?;
        let mut archive = zip::ZipArchive::new(file)?;

        for i in 0..archive.len() {
            let mut file = archive.by_index(i)?;
            let out_path = extract_path.join(file.name());

            if file.name().ends_with('/') {
                std::fs::create_dir_all(&out_path)?;
            } else {
                if let Some(p) = out_path.parent() {
                    if !p.exists() {
                        std::fs::create_dir_all(p)?;
                    }
                }
                let mut outfile = File::create(&out_path)?;
                std::io::copy(&mut file, &mut outfile)?;
            }
        }

        Ok(())
    }

    /// Helper function: Extracts the port from the address string
    ///
    /// # Example
    /// ```
    /// let address = "127.0.0.1:9000";
    /// let port = extract_port(address);
    /// println!("{:?}", port);
    /// ```
    fn extract_port(address: &str) -> Option<u16> {
        address.split(':').nth(1)?.parse().ok()
    }

    /// Create a new instance of the service manager
    ///
    /// # Example
    /// ```
    /// let service_manager = ServiceManager::new();
    /// println!("{:?}", service_manager);
    /// ```
    pub(crate) fn new() -> Self {
        let (command_tx, mut command_rx) = mpsc::channel(10);
        // Start the control loop
        tokio::spawn(async move {
            while let Some(cmd) = command_rx.recv().await {
                match cmd {
                    ServiceCommand::Start(config) => {
                        if let Err(e) = Self::start_service(&config).await {
                            Self::show_error(&format!("启动服务失败：{}", e));
                        }
                    }
                    ServiceCommand::Stop => {
                        if let Err(e) = Self::stop_service().await {
                            Self::show_error(&format!("停止服务失败：{}", e));
                        }
                    }
                    ServiceCommand::Restart(config) => {
                        if Self::check_service_status().await.is_some() {
                            if let Err(e) = Self::stop_service().await {
                                Self::show_error(&format!("重启服务失败：{}", e));
                                continue;
                            }
                        }
                        if let Err(e) = Self::start_service(&config).await {
                            Self::show_error(&format!("重启服务失败：{}", e));
                        }
                    }
                }
            }
        });

        ServiceManager { command_tx }
    }

    /// Start the service
    /// This function starts the service with the given configuration
    ///
    /// # Example
    /// ```
    /// let config = RustFSConfig {
    ///    address: "127.0.0.1:9000".to_string(),
    ///    host: "127.0.0.1".to_string(),
    ///    port: "9000".to_string(),
    ///    access_key: "rustfsadmin".to_string(),
    ///    secret_key: "rustfsadmin".to_string(),
    ///    domain_name: "demo.rustfs.com".to_string(),
    ///    volume_name: "data".to_string(),
    ///    console_address: "127.0.0.1:9001".to_string(),
    /// };
    ///
    /// let result = start_service(&config).await;
    /// println!("{:?}", result);
    /// ```
    async fn start_service(config: &RustFSConfig) -> Result<(), Box<dyn Error>> {
        // Check if the service is already running
        if let Some(existing_pid) = Self::check_service_status().await {
            return Err(format!("服务已经在运行，PID: {}", existing_pid).into());
        }

        // Prepare the service program
        let executable_path = Self::prepare_service().await?;
        // Check the data catalog
        let volume_name_path = Path::new(&config.volume_name);
        if !volume_name_path.exists() {
            tokio::fs::create_dir_all(&config.volume_name).await?;
        }

        // Extract the port from the configuration
        let main_port = Self::extract_port(&config.address).ok_or("无法解析主服务端口")?;
        let console_port = Self::extract_port(&config.console_address).ok_or("无法解析控制台端口")?;

        let host = config.address.split(':').next().ok_or("无法解析主机地址")?;

        // Check the port
        let ports = vec![main_port, console_port];
        for port in ports {
            if Self::is_port_in_use(host, port).await {
                return Err(format!("端口 {} 已被占用", port).into());
            }
        }

        // Start the service
        let mut child = tokio::process::Command::new(executable_path)
            .arg("--address")
            .arg(&config.address)
            .arg("--access-key")
            .arg(&config.access_key)
            .arg("--secret-key")
            .arg(&config.secret_key)
            .arg("--console-address")
            .arg(&config.console_address)
            .arg(config.volume_name.clone())
            .spawn()?;

        let process_pid = child.id().unwrap();
        // Wait for the service to start
        tokio::time::sleep(Duration::from_secs(2)).await;

        // Check if the service started successfully
        if Self::is_port_in_use(host, main_port).await {
            Self::show_info(&format!("服务启动成功！进程 ID: {}", process_pid));

            Ok(())
        } else {
            child.kill().await?;
            Err("服务启动失败".into())
        }
    }

    /// Stop the service
    /// This function stops the service
    ///
    /// # Example
    /// ```
    /// let result = stop_service().await;
    /// println!("{:?}", result);
    /// ```
    async fn stop_service() -> Result<(), Box<dyn Error>> {
        let existing_pid = Self::check_service_status().await;
        debug!("existing_pid: {:?}", existing_pid);
        if let Some(service_pid) = existing_pid {
            // An attempt was made to terminate the process
            #[cfg(unix)]
            {
                StdCommand::new("kill").arg("-9").arg(service_pid.to_string()).output()?;
            }

            #[cfg(windows)]
            {
                StdCommand::new("taskkill")
                    .arg("/F")
                    .arg("/PID")
                    .arg(&service_pid.to_string())
                    .output()?;
            }

            // Verify that the service is indeed stopped
            tokio::time::sleep(Duration::from_secs(1)).await;
            if Self::check_service_status().await.is_some() {
                return Err("服务停止失败".into());
            }
            Self::show_info("服务已成功停止");

            Ok(())
        } else {
            Err("服务未运行".into())
        }
    }

    /// Check if the port is in use
    /// This function checks if the given port is in use on the given host
    ///
    /// # Example
    /// ```
    /// let host = "127.0.0.1";
    /// let port = 9000;
    /// let result = is_port_in_use(host, port).await;
    /// println!("{:?}", result);
    /// ```
    async fn is_port_in_use(host: &str, port: u16) -> bool {
        TcpStream::connect(format!("{}:{}", host, port)).await.is_ok()
    }

    /// Show an error message
    /// This function shows an error message dialog
    ///
    /// # Example
    /// ```
    /// show_error("This is an error message");
    /// ```
    pub(crate) fn show_error(message: &str) {
        rfd::MessageDialog::new()
            .set_title("错误")
            .set_description(message)
            .set_level(rfd::MessageLevel::Error)
            .show();
    }

    /// Show an information message
    /// This function shows an information message dialog
    ///
    /// # Example
    /// ```
    /// show_info("This is an information message");
    /// ```
    pub(crate) fn show_info(message: &str) {
        rfd::MessageDialog::new()
            .set_title("成功")
            .set_description(message)
            .set_level(rfd::MessageLevel::Info)
            .show();
    }

    /// Start the service
    /// This function sends a `Start` command to the service manager
    ///
    /// # Example
    /// ```
    /// let config = RustFSConfig {
    ///    address: "127.0.0.1:9000".to_string(),
    ///    host: "127.0.0.1".to_string(),
    ///    port: "9000".to_string(),
    ///    access_key: "rustfsadmin".to_string(),
    ///    secret_key: "rustfsadmin".to_string(),
    ///    domain_name: "demo.rustfs.com".to_string(),
    ///    volume_name: "data".to_string(),
    ///    console_address: "127.0.0.1:9001".to_string(),
    /// };
    ///
    /// let service_manager = ServiceManager::new();
    /// let result = service_manager.start(config).await;
    /// println!("{:?}", result);
    /// ```
    ///
    /// # Errors
    /// This function returns an error if the service fails to start
    ///
    /// # Panics
    /// This function panics if the port number is invalid
    ///
    /// # Safety
    /// This function is not marked as unsafe
    ///
    /// # Performance
    /// This function is not optimized for performance
    ///
    /// # Design
    /// This function is designed to be simple and easy to use
    ///
    /// # Security
    /// This function does not have any security implications
    pub async fn start(&self, config: RustFSConfig) -> Result<ServiceOperationResult, Box<dyn Error>> {
        let start_time = chrono::Local::now();
        self.command_tx.send(ServiceCommand::Start(config.clone())).await?;

        let host = &config.host;
        let port = config.port.parse::<u16>().expect("无效的端口号");
        // wait for the service to actually start
        let mut retries = 0;
        while retries < 30 {
            // wait up to 30 seconds
            if Self::check_service_status().await.is_some() && Self::is_port_in_use(host, port).await {
                let end_time = chrono::Local::now();
                return Ok(ServiceOperationResult {
                    success: true,
                    start_time,
                    end_time,
                    message: "服务启动成功".to_string(),
                });
            }
            tokio::time::sleep(Duration::from_secs(1)).await;
            retries += 1;
        }

        Err("服务启动超时".into())
    }

    /// Stop the service
    /// This function sends a `Stop` command to the service manager
    ///
    /// # Example
    /// ```
    /// let service_manager = ServiceManager::new();
    /// let result = service_manager.stop().await;
    /// println!("{:?}", result);
    /// ```
    ///
    /// # Errors
    /// This function returns an error if the service fails to stop
    ///
    /// # Panics
    /// This function panics if the port number is invalid
    ///
    /// # Safety
    /// This function is not marked as unsafe
    ///
    /// # Performance
    /// This function is not optimized for performance
    ///
    /// # Design
    /// This function is designed to be simple and easy to use
    ///
    /// # Security
    /// This function does not have any security implications
    pub async fn stop(&self) -> Result<ServiceOperationResult, Box<dyn Error>> {
        let start_time = chrono::Local::now();
        self.command_tx.send(ServiceCommand::Stop).await?;

        // Wait for the service to actually stop
        let mut retries = 0;
        while retries < 15 {
            // Wait up to 15 seconds
            if Self::check_service_status().await.is_none() {
                let end_time = chrono::Local::now();
                return Ok(ServiceOperationResult {
                    success: true,
                    start_time,
                    end_time,
                    message: "服务停止成功".to_string(),
                });
            }
            tokio::time::sleep(Duration::from_secs(1)).await;
            retries += 1;
        }

        Err("服务停止超时".into())
    }

    /// Restart the service
    /// This function sends a `Restart` command to the service manager
    ///
    /// # Example
    /// ```
    /// let config = RustFSConfig {
    ///    address: "127.0.0.1:9000".to_string(),
    ///    host: "127.0.0.1".to_string(),
    ///    port: "9000".to_string(),
    ///    access_key: "rustfsadmin".to_string(),
    ///    secret_key: "rustfsadmin".to_string(),
    ///    domain_name: "demo.rustfs.com".to_string(),
    ///    volume_name: "data".to_string(),
    ///    console_address: "127.0.0.1:9001".to_string(),
    /// };
    ///
    /// let service_manager = ServiceManager::new();
    /// let result = service_manager.restart(config).await;
    /// println!("{:?}", result);
    /// ```
    ///
    /// # Errors
    /// This function returns an error if the service fails to restart
    ///
    /// # Panics
    /// This function panics if the port number is invalid
    ///
    /// # Safety
    /// This function is not marked as unsafe
    ///
    /// # Performance
    /// This function is not optimized for performance
    ///
    /// # Design
    /// This function is designed to be simple and easy to use
    ///
    /// # Security
    /// This function does not have any security implications
    pub async fn restart(&self, config: RustFSConfig) -> Result<ServiceOperationResult, Box<dyn Error>> {
        let start_time = chrono::Local::now();
        self.command_tx.send(ServiceCommand::Restart(config.clone())).await?;

        let host = &config.host;
        let port = config.port.parse::<u16>().expect("无效的端口号");

        // wait for the service to restart
        let mut retries = 0;
        while retries < 45 {
            // Longer waiting time is given as both the stop and start processes are involved
            if Self::check_service_status().await.is_some() && Self::is_port_in_use(host, port).await {
                match config.save() {
                    Ok(_) => info!("save config success"),
                    Err(e) => {
                        error!("save config error: {}", e);
                        self.command_tx.send(ServiceCommand::Stop).await?;
                        Self::show_error("保存配置失败");
                        return Err("保存配置失败".into());
                    }
                }
                let end_time = chrono::Local::now();
                return Ok(ServiceOperationResult {
                    success: true,
                    start_time,
                    end_time,
                    message: "服务重启成功".to_string(),
                });
            }
            tokio::time::sleep(Duration::from_secs(1)).await;
            retries += 1;
        }
        Err("服务重启超时".into())
    }
}
