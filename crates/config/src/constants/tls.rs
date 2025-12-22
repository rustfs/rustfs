//  Copyright 2024 RustFS Team
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

/// TLS related environment variable names and default values
/// Environment variable to enable TLS key logging
/// When set to "1", RustFS will log TLS keys to the specified file for debugging purposes.
/// By default, this is disabled.
/// To enable, set the environment variable RUSTFS_TLS_KEYLOG=1
pub const ENV_TLS_KEYLOG: &str = "RUSTFS_TLS_KEYLOG";

/// Default value for TLS key logging
/// By default, RustFS does not log TLS keys.
/// To change this behavior, set the environment variable RUSTFS_TLS_KEYLOG=1
pub const DEFAULT_TLS_KEYLOG: bool = false;

/// Environment variable to trust system CA certificates
/// When set to "1", RustFS will trust system CA certificates in addition to any
/// custom CA certificates provided in the configuration.
/// By default, this is disabled.
/// To enable, set the environment variable RUSTFS_TRUST_SYSTEM_CA=1
pub const ENV_TRUST_SYSTEM_CA: &str = "RUSTFS_TRUST_SYSTEM_CA";

/// Default value for trusting system CA certificates
/// By default, RustFS does not trust system CA certificates.
/// To change this behavior, set the environment variable RUSTFS_TRUST_SYSTEM_CA=1
pub const DEFAULT_TRUST_SYSTEM_CA: bool = false;
