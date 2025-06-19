use std::env;
use std::fmt;

#[cfg(unix)]
use libc::uname;
#[cfg(unix)]
use std::ffi::CStr;
#[cfg(windows)]
use std::process::Command;

// 定义 Rustfs 版本
const RUSTFS_VERSION: &str = "1.0.0";

// 业务类型枚举
#[derive(Debug, Clone, PartialEq)]
pub enum ServiceType {
    Basis,
    Core,
    Event,
    Logger,
    Custom(String),
}

impl ServiceType {
    fn as_str(&self) -> &str {
        match self {
            ServiceType::Basis => "basis",
            ServiceType::Core => "core",
            ServiceType::Event => "event",
            ServiceType::Logger => "logger",
            ServiceType::Custom(s) => s.as_str(),
        }
    }
}

// UserAgent 结构体
struct UserAgent {
    os_platform: String,
    arch: String,
    version: String,
    service: ServiceType,
}

impl UserAgent {
    // 创建新的 UserAgent 实例，接受业务类型参数
    fn new(service: ServiceType) -> Self {
        let os_platform = Self::get_os_platform();
        let arch = env::consts::ARCH.to_string();
        let version = RUSTFS_VERSION.to_string();

        UserAgent {
            os_platform,
            arch,
            version,
            service,
        }
    }

    // 获取操作系统平台信息
    fn get_os_platform() -> String {
        if cfg!(target_os = "windows") {
            Self::get_windows_platform()
        } else if cfg!(target_os = "macos") {
            Self::get_macos_platform()
        } else if cfg!(target_os = "linux") {
            Self::get_linux_platform()
        } else {
            "Unknown".to_string()
        }
    }

    // 获取 Windows 平台信息
    #[cfg(windows)]
    fn get_windows_platform() -> String {
        // 使用 cmd /c ver 获取版本
        let output = Command::new("cmd")
            .args(&["/C", "ver"])
            .output()
            .unwrap_or_default();
        let version = String::from_utf8_lossy(&output.stdout);
        let version = version
            .lines()
            .next()
            .unwrap_or("Windows NT 10.0")
            .replace("Microsoft Windows [Version ", "")
            .replace("]", "");
        format!("Windows NT {}", version.trim())
    }

    #[cfg(not(windows))]
    fn get_windows_platform() -> String {
        "N/A".to_string()
    }

    // 获取 macOS 平台信息
    #[cfg(target_os = "macos")]
    fn get_macos_platform() -> String {
        unsafe {
            let mut name = std::mem::zeroed();
            if uname(&mut name) == 0 {
                let release = CStr::from_ptr(name.release.as_ptr()).to_string_lossy();
                // 映射内核版本（如 23.5.0）到 User-Agent 格式（如 14_5_0）
                let major = release
                    .split('.')
                    .next()
                    .unwrap_or("14")
                    .parse::<i32>()
                    .unwrap_or(14);
                let minor = if major >= 20 { major - 9 } else { 14 };
                let patch = release.split('.').nth(1).unwrap_or("0");
                format!("Macintosh; Intel Mac OS X {}_{}_{}", minor, patch, 0)
            } else {
                "Macintosh; Intel Mac OS X 14_5_0".to_string()
            }
        }
    }

    #[cfg(not(target_os = "macos"))]
    fn get_macos_platform() -> String {
        "N/A".to_string()
    }

    // 获取 Linux 平台信息
    #[cfg(target_os = "linux")]
    fn get_linux_platform() -> String {
        unsafe {
            let mut name = std::mem::zeroed();
            if uname(&mut name) == 0 {
                let release = CStr::from_ptr(name.release.as_ptr()).to_string_lossy();
                format!("X11; Linux {}", release)
            } else {
                "X11; Linux Unknown".to_string()
            }
        }
    }

    #[cfg(not(target_os = "linux"))]
    fn get_linux_platform() -> String {
        "N/A".to_string()
    }
}

// 实现 Display trait 以格式化 User-Agent
impl fmt::Display for UserAgent {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        if self.service == ServiceType::Basis {
            return write!(
                f,
                "Mozilla/5.0 ({}; {}) Rustfs/{}",
                self.os_platform, self.arch, self.version
            );
        }
        write!(
            f,
            "Mozilla/5.0 ({}; {}) Rustfs/{} ({})",
            self.os_platform,
            self.arch,
            self.version,
            self.service.as_str()
        )
    }
}

// 获取 User-Agent 字符串，接受业务类型参数
pub fn get_user_agent(service: ServiceType) -> String {
    UserAgent::new(service).to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_user_agent_format_basis() {
        let ua = get_user_agent(ServiceType::Basis);
        assert!(ua.starts_with("Mozilla/5.0"));
        assert!(ua.contains("Rustfs/1.0.0"));
        println!("User-Agent: {}", ua);
    }

    #[test]
    fn test_user_agent_format_core() {
        let ua = get_user_agent(ServiceType::Core);
        assert!(ua.starts_with("Mozilla/5.0"));
        assert!(ua.contains("Rustfs/1.0.0 (core)"));
        println!("User-Agent: {}", ua);
    }

    #[test]
    fn test_user_agent_format_event() {
        let ua = get_user_agent(ServiceType::Event);
        assert!(ua.starts_with("Mozilla/5.0"));
        assert!(ua.contains("Rustfs/1.0.0 (event)"));

        println!("User-Agent: {}", ua);
    }

    #[test]
    fn test_user_agent_format_logger() {
        let ua = get_user_agent(ServiceType::Logger);
        assert!(ua.starts_with("Mozilla/5.0"));
        assert!(ua.contains("Rustfs/1.0.0 (logger)"));
        println!("User-Agent: {}", ua);
    }

    #[test]
    fn test_user_agent_format_custom() {
        let ua = get_user_agent(ServiceType::Custom("monitor".to_string()));
        assert!(ua.starts_with("Mozilla/5.0"));
        assert!(ua.contains("Rustfs/1.0.0 (monitor)"));
        println!("User-Agent: {}", ua);
    }
}
