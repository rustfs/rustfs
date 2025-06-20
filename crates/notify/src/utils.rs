use std::fmt;
#[cfg(unix)]
use std::os::unix::process::ExitStatusExt;
#[cfg(windows)]
use std::os::windows::process::ExitStatusExt;
use std::{env, process};

// Define Rustfs version
const RUSTFS_VERSION: &str = "1.0.0";

// Business Type Enumeration
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

// UserAgent structure
struct UserAgent {
    os_platform: String,
    arch: String,
    version: String,
    service: ServiceType,
}

impl UserAgent {
    // Create a new UserAgent instance and accept business type parameters
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

    // Obtain operating system platform information
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

    // Get Windows platform information
    #[cfg(windows)]
    fn get_windows_platform() -> String {
        // Use cmd /c ver to get the version
        let output = process::Command::new("cmd")
            .args(&["/C", "ver"])
            .output()
            .unwrap_or_else(|_| process::Output {
                status: process::ExitStatus::from_raw(0),
                stdout: Vec::new(),
                stderr: Vec::new(),
            });
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

    // Get macOS platform information
    #[cfg(target_os = "macos")]
    fn get_macos_platform() -> String {
        let output = process::Command::new("sw_vers")
            .args(&["-productVersion"])
            .output()
            .unwrap_or_else(|_| process::Output {
                status: process::ExitStatus::from_raw(0),
                stdout: Vec::new(),
                stderr: Vec::new(),
            });
        let version = String::from_utf8_lossy(&output.stdout).trim().to_string();
        let parts: Vec<&str> = version.split('.').collect();
        let major = parts.get(0).unwrap_or(&"10").parse::<i32>().unwrap_or(10);
        let minor = parts.get(1).map_or("15", |&m| m);
        let patch = parts.get(2).map_or("0", |&p| p);

        // Detect whether it is an Apple Silicon chip
        let arch = env::consts::ARCH;
        let cpu_info = if arch == "aarch64" { "Apple" } else { "Intel" };

        // Convert to User-Agent format
        format!("Macintosh; {} Mac OS X {}_{}_{}", cpu_info, major, minor, patch)
    }

    #[cfg(not(target_os = "macos"))]
    fn get_macos_platform() -> String {
        "N/A".to_string()
    }

    // Get Linux platform information
    #[cfg(target_os = "linux")]
    fn get_linux_platform() -> String {
        let output = process::Command::new("uname")
            .arg("-r")
            .output()
            .unwrap_or_else(|_| process::Output {
                status: process::ExitStatus::from_raw(0),
                stdout: Vec::new(),
                stderr: Vec::new(),
            });
        if output.status.success() {
            let release = String::from_utf8_lossy(&output.stdout).trim().to_string();
            format!("X11; Linux {}", release)
        } else {
            "X11; Linux Unknown".to_string()
        }
    }

    #[cfg(not(target_os = "linux"))]
    fn get_linux_platform() -> String {
        "N/A".to_string()
    }
}

// Implement Display trait to format User-Agent
impl fmt::Display for UserAgent {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        if self.service == ServiceType::Basis {
            return write!(f, "Mozilla/5.0 ({}; {}) Rustfs/{}", self.os_platform, self.arch, self.version);
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

// Get the User-Agent string and accept business type parameters
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
