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

/// Path and file system constants
pub mod paths {
    /// Universal path constants
    pub const ROOT_PATH: &str = "/";
    pub const CURRENT_DIR: &str = ".";
    pub const PARENT_DIR: &str = "..";
    pub const PATH_SEPARATOR: &str = "/";

    /// File mode and permission constants
    pub const DIR_MODE: u32 = 0o040000;
    pub const FILE_MODE: u32 = 0o100000;
    pub const DIR_PERMISSIONS: u32 = 0o755;
    pub const FILE_PERMISSIONS: u32 = 0o644;
}

/// Network constants
pub mod network {
    /// Default network addresses
    pub const DEFAULT_SOURCE_IP: &str = "0.0.0.0";
    pub const DEFAULT_ADDR: &str = "0.0.0.0:0";

    /// Authentication constants
    pub const AUTH_SUFFIX_SVC: &str = "=svc";
    pub const AUTH_SUFFIX_LDAP: &str = "=ldap";
    pub const AUTH_FAILURE_DELAY_MS: u64 = 300;
}

/// FTPS constants
#[cfg(feature = "ftps")]
pub mod ftps {
    pub const PORT_RANGE_SEPARATOR: &str = "-";
    pub const PASSIVE_PORTS_PART_COUNT: usize = 2;
}

/// Default configuration values
pub mod defaults {
    /// Default protocol addresses
    #[cfg(feature = "ftps")]
    pub const DEFAULT_FTPS_ADDRESS: &str = "0.0.0.0:8021";

    /// Default FTPS passive port range
    #[cfg(feature = "ftps")]
    pub const DEFAULT_FTPS_PASSIVE_PORTS: &str = "40000-50000";
}
