use rustfs_config::VERSION;
use std::env;
use std::fmt;
use sysinfo::System;

/// Business Type Enumeration
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
    /// Create a new UserAgent instance and accept business type parameters
    ///
    /// # Arguments
    /// * `service` - The type of service for which the User-Agent is being created.
    /// # Returns
    /// A new instance of `UserAgent` with the current OS platform, architecture, version, and service type.
    fn new(service: ServiceType) -> Self {
        let os_platform = Self::get_os_platform();
        let arch = env::consts::ARCH.to_string();
        let version = VERSION.to_string();

        UserAgent {
            os_platform,
            arch,
            version,
            service,
        }
    }

    /// Obtain operating system platform information
    fn get_os_platform() -> String {
        let sys = System::new_all();
        if cfg!(target_os = "windows") {
            Self::get_windows_platform(&sys)
        } else if cfg!(target_os = "macos") {
            Self::get_macos_platform(&sys)
        } else if cfg!(target_os = "linux") {
            Self::get_linux_platform(&sys)
        } else {
            "Unknown".to_string()
        }
    }

    /// Get Windows platform information
    #[cfg(windows)]
    fn get_windows_platform(sys: &System) -> String {
        // Priority to using sysinfo to get versions
        if let Some(version) = sys.os_version() {
            format!("Windows NT {}", version)
        } else {
            // Fallback to cmd /c ver
            let output = std::process::Command::new("cmd")
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
    }

    #[cfg(not(windows))]
    fn get_windows_platform(_sys: &System) -> String {
        "N/A".to_string()
    }

    /// Get macOS platform information
    #[cfg(target_os = "macos")]
    fn get_macos_platform(_sys: &System) -> String {
        let binding = System::os_version().unwrap_or("14.5.0".to_string());
        let version = binding.split('.').collect::<Vec<&str>>();
        let major = version.get(0).unwrap_or(&"14").to_string();
        let minor = version.get(1).unwrap_or(&"5").to_string();
        let patch = version.get(2).unwrap_or(&"0").to_string();

        let arch = env::consts::ARCH;
        let cpu_info = if arch == "aarch64" { "Apple" } else { "Intel" };

        // Convert to User-Agent format
        format!("Macintosh; {} Mac OS X {}_{}_{}", cpu_info, major, minor, patch)
    }

    #[cfg(not(target_os = "macos"))]
    fn get_macos_platform(_sys: &System) -> String {
        "N/A".to_string()
    }

    /// Get Linux platform information
    #[cfg(target_os = "linux")]
    fn get_linux_platform(sys: &System) -> String {
        let name = sys.name().unwrap_or("Linux".to_string());
        let version = sys.os_version().unwrap_or("Unknown".to_string());
        format!("X11; {} {}", name, version)
    }

    #[cfg(not(target_os = "linux"))]
    fn get_linux_platform(_sys: &System) -> String {
        "N/A".to_string()
    }
}

/// Implement Display trait to format User-Agent
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

    #[test]
    fn test_all_service_type() {
        // Example: Generate User-Agents of Different Business Types
        let ua_core = get_user_agent(ServiceType::Core);
        let ua_event = get_user_agent(ServiceType::Event);
        let ua_logger = get_user_agent(ServiceType::Logger);
        let ua_custom = get_user_agent(ServiceType::Custom("monitor".to_string()));

        println!("Core User-Agent: {}", ua_core);
        println!("Event User-Agent: {}", ua_event);
        println!("Logger User-Agent: {}", ua_logger);
        println!("Custom User-Agent: {}", ua_custom);
    }
}
