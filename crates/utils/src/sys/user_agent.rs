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
use std::borrow::Cow;
use std::env;
use std::fmt;
use std::sync::OnceLock;
#[cfg(not(target_os = "openbsd"))]
use sysinfo::System;

/// Business Type Enumeration
#[derive(Debug, Clone, PartialEq)]
pub enum ServiceType {
    Basis,
    Core,
    Event,
    Logger,
    Custom(Cow<'static, str>),
}

impl ServiceType {
    fn as_str(&self) -> &str {
        match self {
            ServiceType::Basis => "basis",
            ServiceType::Core => "core",
            ServiceType::Event => "event",
            ServiceType::Logger => "logger",
            ServiceType::Custom(s) => s,
        }
    }
}

/// UserAgent structure to hold User-Agent information
/// including OS platform, architecture, version, and service type.
#[derive(Debug)]
struct UserAgent {
    os_platform: &'static str,
    arch: &'static str,
    version: &'static str,
    service: ServiceType,
}

static OS_PLATFORM: OnceLock<String> = OnceLock::new();

impl UserAgent {
    /// Create a new UserAgent instance and accept business type parameters
    fn new(service: ServiceType) -> Self {
        UserAgent {
            os_platform: Self::get_os_platform(),
            arch: env::consts::ARCH,
            version: VERSION,
            service,
        }
    }

    /// Obtain operating system platform information using a thread-safe cache.
    ///
    /// The value is computed once on first use via `OnceLock` and then reused
    /// for all subsequent calls for the lifetime of the program.
    fn get_os_platform() -> &'static str {
        OS_PLATFORM.get_or_init(|| {
            if cfg!(target_os = "windows") {
                Self::get_windows_platform()
            } else if cfg!(target_os = "macos") {
                Self::get_macos_platform()
            } else if cfg!(target_os = "linux") {
                Self::get_linux_platform()
            } else if cfg!(target_os = "freebsd") {
                Self::get_freebsd_platform()
            } else if cfg!(target_os = "netbsd") {
                Self::get_netbsd_platform()
            } else {
                "Unknown".to_string()
            }
        })
    }

    /// Get Windows platform information
    #[cfg(windows)]
    fn get_windows_platform() -> String {
        let version = System::os_version().unwrap_or_else(|| "NT Unknown".to_string());
        if version.starts_with("Windows") {
            version
        } else {
            format!("Windows NT {version}")
        }
    }

    #[cfg(not(windows))]
    fn get_windows_platform() -> String {
        "N/A".to_string()
    }

    /// Get macOS platform information
    #[cfg(target_os = "macos")]
    fn get_macos_platform() -> String {
        let version_str = System::os_version().unwrap_or_else(|| "14.0.0".to_string());
        let mut parts = version_str.split('.');
        let major = parts.next().unwrap_or("14");
        let minor = parts.next().unwrap_or("0");
        let patch = parts.next().unwrap_or("0");

        let cpu_info = if env::consts::ARCH == "aarch64" { "Apple" } else { "Intel" };

        format!("Macintosh; {cpu_info} Mac OS X {major}_{minor}_{patch}")
    }

    #[cfg(not(target_os = "macos"))]
    fn get_macos_platform() -> String {
        "N/A".to_string()
    }

    /// Get Linux platform information
    #[cfg(target_os = "linux")]
    fn get_linux_platform() -> String {
        let os_name = System::long_os_version().unwrap_or_else(|| "Linux Unknown".to_string());
        format!("X11; {os_name}")
    }

    #[cfg(not(target_os = "linux"))]
    fn get_linux_platform() -> String {
        "N/A".to_string()
    }

    #[cfg(target_os = "freebsd")]
    fn get_freebsd_platform() -> String {
        format!("FreeBSD; {}", env::consts::ARCH)
    }

    #[cfg(not(target_os = "freebsd"))]
    fn get_freebsd_platform() -> String {
        "N/A".to_string()
    }

    #[cfg(target_os = "netbsd")]
    fn get_netbsd_platform() -> String {
        format!("NetBSD; {}", env::consts::ARCH)
    }

    #[cfg(not(target_os = "netbsd"))]
    fn get_netbsd_platform() -> String {
        "N/A".to_string()
    }
}

impl fmt::Display for UserAgent {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Mozilla/5.0 ({}; {}) RustFS/{}", self.os_platform, self.arch, self.version)?;
        if self.service != ServiceType::Basis {
            write!(f, " ({})", self.service.as_str())?;
        }
        Ok(())
    }
}

/// Get the User-Agent string and accept business type parameters
pub fn get_user_agent(service: ServiceType) -> String {
    UserAgent::new(service).to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use rustfs_config::VERSION;

    #[test]
    fn test_user_agent_format_basis() {
        let ua = get_user_agent(ServiceType::Basis);
        assert!(ua.starts_with("Mozilla/5.0"));
        assert!(ua.contains(&format!("RustFS/{VERSION}")));
        assert!(!ua.contains("(basis)"));
    }

    #[test]
    fn test_user_agent_format_core() {
        let ua = get_user_agent(ServiceType::Core);
        assert!(ua.contains(&format!("RustFS/{VERSION} (core)")));
    }

    #[test]
    fn test_user_agent_format_custom() {
        let ua = get_user_agent(ServiceType::Custom("monitor".into()));
        assert!(ua.contains(&format!("RustFS/{VERSION} (monitor)")));
    }

    #[test]
    fn test_os_platform_caching() {
        let ua1 = UserAgent::new(ServiceType::Basis);
        let ua2 = UserAgent::new(ServiceType::Basis);
        assert_eq!(ua1.os_platform, ua2.os_platform);
        // Ensure they point to the same static memory
        assert!(std::ptr::eq(ua1.os_platform.as_ptr(), ua2.os_platform.as_ptr()));
    }
}
