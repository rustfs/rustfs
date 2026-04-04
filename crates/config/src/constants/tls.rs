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

/// Environment variable to trust leaf certificates as CA
/// When set to "1", RustFS will treat leaf certificates as CA certificates for trust validation.
/// By default, this is disabled.
/// To enable, set the environment variable RUSTFS_TRUST_LEAF_CERT_AS_CA=1
pub const ENV_TRUST_LEAF_CERT_AS_CA: &str = "RUSTFS_TRUST_LEAF_CERT_AS_CA";

/// Default value for trusting leaf certificates as CA
/// By default, RustFS does not trust leaf certificates as CA.
/// To change this behavior, set the environment variable RUSTFS_TRUST_LEAF_CERT_AS_CA=1
pub const DEFAULT_TRUST_LEAF_CERT_AS_CA: bool = false;

/// Default filename for client CA certificate
/// client_ca.crt (CA bundle for verifying client certificates in server mTLS)
pub const RUSTFS_CLIENT_CA_CERT_FILENAME: &str = "client_ca.crt";

/// Environment variable for client certificate file path
/// RUSTFS_MTLS_CLIENT_CERT
/// Specifies the file path to the client certificate used for mTLS authentication.
/// If not set, RustFS will look for the default filename "client_cert.pem" in the current directory.
/// To set, use the environment variable RUSTFS_MTLS_CLIENT_CERT=/path/to/client_cert.pem
pub const ENV_MTLS_CLIENT_CERT: &str = "RUSTFS_MTLS_CLIENT_CERT";

/// Default filename for client certificate
/// client_cert.pem
pub const RUSTFS_CLIENT_CERT_FILENAME: &str = "client_cert.pem";

/// Environment variable for client private key file path
/// RUSTFS_MTLS_CLIENT_KEY
/// Specifies the file path to the client private key used for mTLS authentication.
/// If not set, RustFS will look for the default filename "client_key.pem" in the current directory.
/// To set, use the environment variable RUSTFS_MTLS_CLIENT_KEY=/path/to/client_key.pem
pub const ENV_MTLS_CLIENT_KEY: &str = "RUSTFS_MTLS_CLIENT_KEY";

/// Default filename for client private key
/// client_key.pem
pub const RUSTFS_CLIENT_KEY_FILENAME: &str = "client_key.pem";

/// RUSTFS_SERVER_MTLS_ENABLE
/// Environment variable to enable server mTLS
/// When set to "1", RustFS server will require client certificates for authentication.
/// By default, this is disabled.
/// To enable, set the environment variable RUSTFS_SERVER_MTLS_ENABLE=1
pub const ENV_SERVER_MTLS_ENABLE: &str = "RUSTFS_SERVER_MTLS_ENABLE";

/// Default value for enabling server mTLS
/// By default, RustFS server mTLS is disabled.
/// To change this behavior, set the environment variable RUSTFS_SERVER_MTLS_ENABLE=1
pub const DEFAULT_SERVER_MTLS_ENABLE: bool = false;

// ── HTTP Transport Tuning Parameters ──

/// Environment variable for HTTP/2 initial stream window size (bytes)
/// Default: 4194304 (4 MB)
pub const ENV_H2_INITIAL_STREAM_WINDOW_SIZE: &str = "RUSTFS_H2_INITIAL_STREAM_WINDOW_SIZE";
pub const DEFAULT_H2_INITIAL_STREAM_WINDOW_SIZE: u32 = 4 * 1024 * 1024; // 4 MB

/// Environment variable for HTTP/2 initial connection window size (bytes)
/// Default: 8388608 (8 MB)
pub const ENV_H2_INITIAL_CONN_WINDOW_SIZE: &str = "RUSTFS_H2_INITIAL_CONN_WINDOW_SIZE";
pub const DEFAULT_H2_INITIAL_CONN_WINDOW_SIZE: u32 = 8 * 1024 * 1024; // 8 MB

/// Environment variable for HTTP/2 max frame size (bytes)
/// Range: 16384 (16 KB) to 16777216 (16 MB) per RFC 7540
/// Default: 524288 (512 KB)
pub const ENV_H2_MAX_FRAME_SIZE: &str = "RUSTFS_H2_MAX_FRAME_SIZE";
pub const DEFAULT_H2_MAX_FRAME_SIZE: u32 = 512 * 1024; // 512 KB

/// Environment variable for HTTP/2 max header list size (bytes)
/// Default: 65536 (64 KB)
pub const ENV_H2_MAX_HEADER_LIST_SIZE: &str = "RUSTFS_H2_MAX_HEADER_LIST_SIZE";
pub const DEFAULT_H2_MAX_HEADER_LIST_SIZE: u32 = 64 * 1024; // 64 KB

/// Environment variable for HTTP/2 max concurrent streams
/// Default: 2048
pub const ENV_H2_MAX_CONCURRENT_STREAMS: &str = "RUSTFS_H2_MAX_CONCURRENT_STREAMS";
pub const DEFAULT_H2_MAX_CONCURRENT_STREAMS: u32 = 2048;

/// Environment variable for HTTP/2 keep-alive interval (seconds)
/// Default: 20
pub const ENV_H2_KEEP_ALIVE_INTERVAL: &str = "RUSTFS_H2_KEEP_ALIVE_INTERVAL";
pub const DEFAULT_H2_KEEP_ALIVE_INTERVAL: u64 = 20;

/// Environment variable for HTTP/2 keep-alive timeout (seconds)
/// Default: 10
pub const ENV_H2_KEEP_ALIVE_TIMEOUT: &str = "RUSTFS_H2_KEEP_ALIVE_TIMEOUT";
pub const DEFAULT_H2_KEEP_ALIVE_TIMEOUT: u64 = 10;

/// Environment variable for HTTP/1.1 header read timeout (seconds)
/// Default: 5
pub const ENV_HTTP1_HEADER_READ_TIMEOUT: &str = "RUSTFS_HTTP1_HEADER_READ_TIMEOUT";
pub const DEFAULT_HTTP1_HEADER_READ_TIMEOUT: u64 = 5;

/// Environment variable for HTTP/1.1 max buffer size (bytes)
/// Default: 65536 (64 KB)
pub const ENV_HTTP1_MAX_BUF_SIZE: &str = "RUSTFS_HTTP1_MAX_BUF_SIZE";
pub const DEFAULT_HTTP1_MAX_BUF_SIZE: usize = 64 * 1024; // 64 KB

// ── TLS Hot Reload Parameters ──

/// Environment variable to enable TLS certificate hot reload
/// Default: false
/// To enable, set the environment variable RUSTFS_TLS_RELOAD_ENABLE=1
pub const ENV_TLS_RELOAD_ENABLE: &str = "RUSTFS_TLS_RELOAD_ENABLE";

/// Default value for TLS certificate hot reload
/// By default, RustFS does not reload TLS certificates automatically.
pub const DEFAULT_TLS_RELOAD_ENABLE: bool = false;

/// Environment variable for TLS certificate reload interval (seconds)
/// Default: 30 seconds. Minimum: 5 seconds.
pub const ENV_TLS_RELOAD_INTERVAL: &str = "RUSTFS_TLS_RELOAD_INTERVAL";

/// Default interval for TLS certificate reload check
pub const DEFAULT_TLS_RELOAD_INTERVAL: u64 = 30;
