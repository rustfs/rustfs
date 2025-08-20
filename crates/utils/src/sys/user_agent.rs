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

    /// Get Windows platform information
    #[cfg(windows)]
    fn get_windows_platform() -> String {
        // Priority to using sysinfo to get versions
        let version = match System::os_version() {
            Some(version) => version,
            None => "Windows NT Unknown".to_string(),
        };
        format!("Windows NT {version}")
    }

    #[cfg(not(windows))]
    fn get_windows_platform() -> String {
        "N/A".to_string()
    }

    /// Get macOS platform information
    #[cfg(target_os = "macos")]
    fn get_macos_platform() -> String {
        let binding = System::os_version().unwrap_or("14.5.0".to_string());
        let version = binding.split('.').collect::<Vec<&str>>();
        let major = version.first().unwrap_or(&"14").to_string();
        let minor = version.get(1).unwrap_or(&"5").to_string();
        let patch = version.get(2).unwrap_or(&"0").to_string();

        let arch = env::consts::ARCH;
        let cpu_info = if arch == "aarch64" { "Apple" } else { "Intel" };

        // Convert to User-Agent format
        format!("Macintosh; {cpu_info} Mac OS X {major}_{minor}_{patch}")
    }

    #[cfg(not(target_os = "macos"))]
    fn get_macos_platform() -> String {
        "N/A".to_string()
    }

    /// Get Linux platform information
    #[cfg(target_os = "linux")]
    fn get_linux_platform() -> String {
        format!("X11; {}", System::long_os_version().unwrap_or("Linux Unknown".to_string()))
    }

    #[cfg(not(target_os = "linux"))]
    fn get_linux_platform() -> String {
        "N/A".to_string()
    }
}

/// Implement Display trait to format User-Agent
impl fmt::Display for UserAgent {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        if self.service == ServiceType::Basis {
            return write!(f, "Mozilla/5.0 ({}; {}) RustFS/{}", self.os_platform, self.arch, self.version);
        }
        write!(
            f,
            "Mozilla/5.0 ({}; {}) RustFS/{} ({})",
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
    use rustfs_config::VERSION;
    use tracing::debug;
    #[test]
    fn test_user_agent_format_basis() {
        let ua = get_user_agent(ServiceType::Basis);
        assert!(ua.starts_with("Mozilla/5.0"));
        assert!(ua.contains(&format!("RustFS/{VERSION}").to_string()));
        debug!("Basic User-Agent: {}", ua);
    }

    #[test]
    fn test_user_agent_format_core() {
        let ua = get_user_agent(ServiceType::Core);
        assert!(ua.starts_with("Mozilla/5.0"));
        assert!(ua.contains(&format!("RustFS/{VERSION} (core)").to_string()));
        debug!("Core User-Agent: {}", ua);
    }

    #[test]
    fn test_user_agent_format_event() {
        let ua = get_user_agent(ServiceType::Event);
        assert!(ua.starts_with("Mozilla/5.0"));
        assert!(ua.contains(&format!("RustFS/{VERSION} (event)").to_string()));
        debug!("Event User-Agent: {}", ua);
    }

    #[test]
    fn test_user_agent_format_logger() {
        let ua = get_user_agent(ServiceType::Logger);
        assert!(ua.starts_with("Mozilla/5.0"));
        assert!(ua.contains(&format!("RustFS/{VERSION} (logger)").to_string()));
        debug!("Logger User-Agent: {}", ua);
    }

    #[test]
    fn test_user_agent_format_custom() {
        let ua = get_user_agent(ServiceType::Custom("monitor".to_string()));
        assert!(ua.starts_with("Mozilla/5.0"));
        assert!(ua.contains(&format!("RustFS/{VERSION} (monitor)").to_string()));
        debug!("Monitor User-Agent: {}", ua);
    }

    #[test]
    fn test_all_service_type() {
        // Example: Generate User-Agents of Different Business Types
        let ua_core = get_user_agent(ServiceType::Core);
        let ua_event = get_user_agent(ServiceType::Event);
        let ua_logger = get_user_agent(ServiceType::Logger);
        let ua_custom = get_user_agent(ServiceType::Custom("monitor".to_string()));

        debug!("Core User-Agent: {}", ua_core);
        debug!("Event User-Agent: {}", ua_event);
        debug!("Logger User-Agent: {}", ua_logger);
        debug!("Custom User-Agent: {}", ua_custom);
    }
}
